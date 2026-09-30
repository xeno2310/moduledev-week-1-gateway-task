using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Npgsql;
using NpgsqlTypes;

var connectionString =
    Environment.GetEnvironmentVariable("COURSE_DB_CONNECTION")
    ?? throw new InvalidOperationException(
        "COURSE_DB_CONNECTION is not configured.");

var instanceId =
    Environment.GetEnvironmentVariable("WORKER_INSTANCE_ID")
    ?? throw new InvalidOperationException(
        "WORKER_INSTANCE_ID is not configured.");

var workerScopes =
    ParseWorkerScopes(
        Environment.GetEnvironmentVariable(
            "WORKFLOW_WORKER_SCOPES")
        ?? throw new InvalidOperationException(
            "WORKFLOW_WORKER_SCOPES is not configured."));

var testProfile =
    Environment.GetEnvironmentVariable("COURSE_TEST_PROFILE") == "1";

// Migration 024 uses a short lease, renewed independently until the task deadline.
var leaseMs = ReadBoundedIntegerEnvironment(
    "COURSE_JOB_LEASE_MS", testProfile ? 2000 : 10000, 100, 300000);
var pollMs = ReadBoundedIntegerEnvironment(
    "COURSE_WORKER_POLL_MS", testProfile ? 50 : 250, 1, 60000);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:8080");
builder.Services.AddHostedService(_ => new WorkerLoopService(token =>
    RunWorkerAsync(connectionString, instanceId, workerScopes, leaseMs, pollMs, token)));
var app = builder.Build();
app.MapGet("/health/live", () => WorkerObservability.Live().ToResult());
app.MapGet("/health/ready", async (CancellationToken token) =>
    (await WorkerObservability.ReadyAsync(
        ct => WorkerObservability.ProbePostgresAsync(connectionString, ct), token)).ToResult());
app.MapGet("/metrics", () => WorkerObservability.Metrics().ToResult());
// ConsoleLifetime owns SIGTERM/SIGINT; the hosted loop receives its stopping
// token. Bind/start failure propagates and stops the host and worker together.
await app.RunAsync();


static int ReadBoundedIntegerEnvironment(
    string name, int defaultValue, int minimum, int maximum)
{
    var raw = Environment.GetEnvironmentVariable(name);
    if (raw is null)
    {
        return defaultValue;
    }

    if (raw.Length is < 1 or > 9
        || raw.Any(character => character < '0' || character > '9')
        || !int.TryParse(raw, System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture, out var value)
        || value < minimum || value > maximum)
    {
        // Do not echo the untrusted value, even for startup errors.
        throw new InvalidOperationException(name + ": invalid integer configuration.");
    }

    return value;
}


static async Task RunWorkerAsync(
    string connectionString,
    string instanceId,
    string[] workerScopes,
    int leaseMs,
    int pollMs,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            await using var connection =
                new NpgsqlConnection(connectionString);

            await connection.OpenAsync(cancellationToken);

            while (!cancellationToken.IsCancellationRequested)
            {
                var claimStartedAt = System.Diagnostics.Stopwatch.GetTimestamp();
                var job = await ClaimAsync(
                    connection,
                    instanceId,
                    leaseMs,
                    cancellationToken);

                if (job is null)
                {
                    await Task.Delay(
                        pollMs,
                        cancellationToken);

                    continue;
                }

                await using var heartbeat = new JobLeaseHeartbeat(
                    leaseMs, job.TimeoutMs, cancellationToken,
                    token => RenewJobLeaseAsync(connectionString, job, instanceId, leaseMs, token),
                    claimStartedAt);
                await ProcessJobAsync(
                    connection, instanceId, workerScopes, job, heartbeat, cancellationToken);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            WriteWorkerLoopDiagnostic(
                instanceId,
                ex);

            try
            {
                await Task.Delay(
                    pollMs,
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}


static async Task<ClaimedJob?> ClaimAsync(
    NpgsqlConnection connection,
    string owner,
    int leaseMs,
    CancellationToken cancellationToken)
{
    await using var command = new NpgsqlCommand(
        """
        SELECT *
        FROM workflow.claim_jobs(
            @owner,
            @limit,
            @lease_ms
        );
        """,
        connection);

    command.Parameters.AddWithValue(
        "owner",
        owner);

    command.Parameters.AddWithValue(
        "limit",
        1);

    command.Parameters.AddWithValue(
        "lease_ms",
        leaseMs);

    await using var reader =
        await command.ExecuteReaderAsync(cancellationToken);

    if (!await reader.ReadAsync(cancellationToken))
    {
        return null;
    }

    return new ClaimedJob(
        reader.GetGuid(
            reader.GetOrdinal("job_id")),

        reader.GetGuid(
            reader.GetOrdinal("process_id")),

        reader.GetGuid(
            reader.GetOrdinal("step_instance_id")),

        reader.GetGuid(
            reader.GetOrdinal("execution_id")),

        reader.GetGuid(
            reader.GetOrdinal("attempt_id")),

        reader.GetInt64(
            reader.GetOrdinal("lease_version")),

        reader.GetInt32(
            reader.GetOrdinal("attempt_number")),

        reader.GetString(
            reader.GetOrdinal("process_data")),

        reader.GetString(
            reader.GetOrdinal("module")),

        reader.GetString(
            reader.GetOrdinal("action")),

        reader.GetInt32(
            reader.GetOrdinal("action_version")),

        reader.GetString(
            reader.GetOrdinal("request_schema")),

        reader.GetString(
            reader.GetOrdinal("response_schema")),

        reader.GetString(
            reader.GetOrdinal("outcomes")),

        reader.GetString(
            reader.GetOrdinal("required_policy")),

        reader.GetInt32(
            reader.GetOrdinal("timeout_ms")),

        reader.GetInt32(
            reader.GetOrdinal("max_attempts")),

        reader.GetString(
            reader.GetOrdinal("delays_ms")),

        reader.GetString(
            reader.GetOrdinal("input_mapping")),

        reader.GetString(
            reader.GetOrdinal("input_constants")));
}


static async Task<bool> RenewJobLeaseAsync(
    string connectionString, ClaimedJob job, string owner, int leaseMs, CancellationToken token)
{
    // Each renewal commits independently. Never borrow the action connection.
    var settings = new NpgsqlConnectionStringBuilder(connectionString)
    {
        Pooling = false, Timeout = 1, CommandTimeout = 1, CancellationTimeout = 100
    };
    await using var connection = new NpgsqlConnection(settings.ConnectionString);
    await connection.OpenAsync(token);
    await using var command = new NpgsqlCommand(
        "SELECT workflow.renew_job_lease(@job_id, @owner, @lease_version, @lease_ms)", connection);
    command.Parameters.AddWithValue("job_id", job.JobId);
    command.Parameters.AddWithValue("owner", owner);
    command.Parameters.AddWithValue("lease_version", job.LeaseVersion);
    command.Parameters.AddWithValue("lease_ms", leaseMs);
    return await command.ExecuteScalarAsync(token) is true;
}


static async Task ProcessJobAsync(
    NpgsqlConnection connection, string owner, string[] workerScopes, ClaimedJob job,
    JobLeaseHeartbeat heartbeat, CancellationToken shutdown)
{
    NpgsqlTransaction? transaction = null;
    string? failureCode = null;
    Exception? failure = null;
    var retryable = false;
    var committed = false;
    var completing = false;
    var completionUncertain = false;
    try
    {
        await heartbeat.Started;
        heartbeat.Token.ThrowIfCancellationRequested();
        await ReachFailpointAsync("after_job_claim", owner, heartbeat.Token);
        if (!HasRequiredPolicy(job.RequiredPolicy, workerScopes))
        {
            failureCode = "access.denied";
            return;
        }
        JsonNode? payload;
        try
        {
            payload = BuildPayload(job);
            if (!ValidateSchema(job.RequestSchema, payload))
            {
                failureCode = "workflow.request_invalid";
                return;
            }
        }
        catch (MappingMissingException)
        {
            failureCode = "workflow.mapping_missing";
            return;
        }
        catch (Exception error)
        {
            failureCode = "workflow.mapping_invalid";
            failure = error;
            return;
        }
        heartbeat.Token.ThrowIfCancellationRequested();
        transaction = await connection.BeginTransactionAsync(heartbeat.Token);
        var envelope = await InvokeActionAsync(
            connection, transaction, job, payload, workerScopes, heartbeat.Deadline, heartbeat.Token);
        var status = envelope["status"]?.GetValue<string>();
        if (status == "error")
        {
            failureCode = envelope["code"]?.GetValue<string>() ?? "workflow.action_error";
            retryable = envelope["retryable"]?.GetValue<bool>() ?? false;
            return;
        }
        if (status != "ok")
        {
            failureCode = "workflow.action_contract_invalid";
            return;
        }
        var outcome = envelope["outcome"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(outcome) || !IsDeclaredOutcome(job.Outcomes, outcome))
        {
            failureCode = "workflow.outcome_unknown";
            return;
        }
        var result = envelope["result"]?.DeepClone();
        if (!ValidateSchema(job.ResponseSchema, result))
        {
            failureCode = "workflow.response_invalid";
            return;
        }
        // The action effect is still uncommitted; the independent heartbeat
        // continues through this wait, and cancellation cannot continue to finish.
        await ReachFailpointAsync("after_action_before_finish", owner, heartbeat.Token);
        await heartbeat.StopAsync();
        if (heartbeat.Reason != JobLeaseStop.NormalStop)
            return;
        var budget = heartbeat.CompletionBudgetMs;
        if (budget == 0)
        {
            completionUncertain = true;
            return;
        }
        using var completion = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        completion.CancelAfter(budget);
        completing = true;
        var finished = await FinishAsync(
            connection, transaction, job, owner, outcome, result, completion.Token);
        if (finished["status"]?.GetValue<string>() != "ok")
        {
            failureCode = finished["code"]?.GetValue<string>() ?? "workflow.finish_failed";
            completionUncertain = failureCode == "workflow.lease_stale";
            return;
        }
        await transaction.CommitAsync(completion.Token);
        committed = true;
    }
    catch (OperationCanceledException)
    {
        // The heartbeat owns classification: shutdown, timeout, and lease loss
        // must not all consume the action's retry budget.
        completionUncertain = completing;
    }
    catch (Exception error)
    {
        failure = error;
        failureCode = "workflow.worker_error";
        retryable = true;
        completionUncertain = completing;
    }
    finally
    {
        // Join renewal before either finish/fail can acquire the job row lock.
        await heartbeat.StopAsync();
        if (transaction is not null)
        {
            if (!committed)
            {
                try { await transaction.RollbackAsync(CancellationToken.None); }
                catch { /* A broken connection also rolls back on the server. */ }
            }
            await transaction.DisposeAsync();
        }
        if (!committed && heartbeat.Reason != JobLeaseStop.Shutdown)
        {
            var budget = heartbeat.CompletionBudgetMs;
            if (heartbeat.Reason == JobLeaseStop.LeaseLost || completionUncertain || budget == 0)
            {
                // No fail_job: a stale/uncertain lease is recovered by expiry,
                // without charging failure_count or changing another owner's work.
                WriteJobDiagnostic(owner, job, heartbeat.Reason == JobLeaseStop.LeaseLost
                    ? heartbeat.DiagnosticCode : "workflow.lease_stale", false, null);
            }
            else
            {
                if (heartbeat.Reason == JobLeaseStop.Timeout)
                {
                    failureCode = "action.timeout";
                    retryable = true;
                    failure = null;
                }
                if (failureCode is not null)
                {
                    using var completion = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
                    completion.CancelAfter(budget);
                    await FailAsync(connection, job, owner, failureCode, retryable,
                        completion.Token, exception: failure);
                }
            }
        }
    }
}


static async Task<JsonObject> InvokeActionAsync(
    NpgsqlConnection connection,
    NpgsqlTransaction transaction,
    ClaimedJob job,
    JsonNode? payload,
    string[] workerScopes,
    DateTimeOffset deadline,
    CancellationToken cancellationToken)
{
    var scopes =
        new JsonArray();

    foreach (var scope in workerScopes)
    {
        scopes.Add(scope);
    }

    var correlationId =
        Guid.NewGuid();

    var context = new JsonObject
    {
        ["principal"] = "workflow-worker",
        ["consumer"] = "workflow-worker",

        ["scopes"] =
            scopes,

        ["correlationId"] =
            correlationId.ToString(),

        // Stable across retry/reclaim.
        ["requestId"] =
            job.ExecutionId.ToString(),

        ["processId"] =
            job.ProcessId.ToString(),

        ["jobId"] =
            job.JobId.ToString(),

        ["executionId"] =
            job.ExecutionId.ToString(),

        ["attemptId"] =
            job.AttemptId.ToString(),

        ["deadline"] =
            deadline.ToString("O")
    };

    await using var command = new NpgsqlCommand(
        """
        SELECT api.invoke(
            @module,
            @action,
            @version,
            @context,
            @payload
        )::text;
        """,
        connection,
        transaction);

    command.Parameters.AddWithValue(
        "module",
        job.Module);

    command.Parameters.AddWithValue(
        "action",
        job.Action);

    command.Parameters.AddWithValue(
        "version",
        job.ActionVersion);

    command.Parameters.Add(
        "context",
        NpgsqlDbType.Jsonb).Value =
            context.ToJsonString();

    command.Parameters.Add(
        "payload",
        NpgsqlDbType.Jsonb).Value =
            payload?.ToJsonString()
            ?? "null";

    var raw = await command.ExecuteScalarAsync(cancellationToken);

    if (raw is not string json)
    {
        throw new InvalidOperationException(
            "api.invoke returned no JSON.");
    }

    return JsonNode.Parse(json)
        as JsonObject
        ?? throw new InvalidOperationException(
            "api.invoke returned invalid envelope.");
}


static async Task<JsonObject> FinishAsync(
    NpgsqlConnection connection,
    NpgsqlTransaction transaction,
    ClaimedJob job,
    string owner,
    string outcome,
    JsonNode? result,
    CancellationToken cancellationToken)
{
    await using var command = new NpgsqlCommand(
        """
        SELECT workflow.finish_job(
            @job_id,
            @owner,
            @lease_version,
            @outcome,
            @result
        )::text;
        """,
        connection,
        transaction);

    command.Parameters.AddWithValue(
        "job_id",
        job.JobId);

    command.Parameters.AddWithValue(
        "owner",
        owner);

    command.Parameters.AddWithValue(
        "lease_version",
        job.LeaseVersion);

    command.Parameters.AddWithValue(
        "outcome",
        outcome);

    command.Parameters.Add(
        "result",
        NpgsqlDbType.Jsonb).Value =
            result?.ToJsonString()
            ?? "null";

    var raw =
        await command.ExecuteScalarAsync(
            cancellationToken);

    if (raw is not string json)
    {
        throw new InvalidOperationException(
            "finish_job returned no JSON.");
    }

    return JsonNode.Parse(json)
        as JsonObject
        ?? throw new InvalidOperationException(
            "finish_job returned invalid JSON.");
}


static async Task FailAsync(
    NpgsqlConnection connection,
    ClaimedJob job,
    string owner,
    string errorCode,
    bool retryable,
    CancellationToken cancellationToken,
    Exception? exception = null)
{
    WriteJobDiagnostic(
        owner,
        job,
        errorCode,
        retryable,
        exception);

    await using var command = new NpgsqlCommand(
        """
        SELECT workflow.fail_job(
            @job_id,
            @owner,
            @lease_version,
            @error_code,
            @retryable
        )::text;
        """,
        connection);

    command.Parameters.AddWithValue(
        "job_id",
        job.JobId);

    command.Parameters.AddWithValue(
        "owner",
        owner);

    command.Parameters.AddWithValue(
        "lease_version",
        job.LeaseVersion);

    command.Parameters.AddWithValue(
        "error_code",
        errorCode);

    command.Parameters.AddWithValue(
        "retryable",
        retryable);

    await command.ExecuteScalarAsync(
        cancellationToken);
}


static string[] ParseWorkerScopes(
    string value)
{
    var scopes =
        value
            .Split(
                ',',
                StringSplitOptions.RemoveEmptyEntries
                | StringSplitOptions.TrimEntries)
            .Distinct(
                StringComparer.Ordinal)
            .ToArray();

    if (scopes.Length == 0)
    {
        throw new InvalidOperationException(
            "WORKFLOW_WORKER_SCOPES must contain at least one scope.");
    }

    return scopes;
}


static bool HasRequiredPolicy(
    string requiredPolicyJson,
    string[] workerScopes)
{
    try
    {
        var requiredPolicy =
            JsonNode.Parse(requiredPolicyJson)
            as JsonArray;

        if (requiredPolicy is null)
        {
            return false;
        }

        foreach (var node in requiredPolicy)
        {
            if (node is null)
            {
                return false;
            }

            var requiredScope =
                node.GetValue<string>();

            if (string.IsNullOrWhiteSpace(requiredScope)
                || !workerScopes.Contains(
                    requiredScope,
                    StringComparer.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
    catch
    {
        // Malformed policy fails closed.
        return false;
    }
}


static void WriteWorkerLoopDiagnostic(
    string workerId,
    Exception exception)
{
    Console.Error.WriteLine(
        JsonSerializer.Serialize(
            new
            {
                @event =
                    "worker.loop_error",

                level =
                    "error",

                workerId,

                exceptionType =
                    exception
                        .GetType()
                        .Name
            }));
}


static void WriteJobDiagnostic(
    string workerId,
    ClaimedJob job,
    string errorCode,
    bool retryable,
    Exception? exception)
{
    Console.Error.WriteLine(
        JsonSerializer.Serialize(
            new
            {
                @event =
                    "worker.job_failure",

                level =
                    "error",

                workerId,

                processId =
                    job.ProcessId,

                jobId =
                    job.JobId,

                executionId =
                    job.ExecutionId,

                attemptId =
                    job.AttemptId,

                leaseVersion =
                    job.LeaseVersion,

                attemptNumber =
                    job.AttemptNumber,

                errorCode,

                retryable,

                exceptionType =
                    exception?
                        .GetType()
                        .Name
            }));
}


static JsonNode? BuildPayload(
    ClaimedJob job)
{
    var processData =
        JsonNode.Parse(job.ProcessData);

    JsonNode? payload =
        JsonNode.Parse(job.InputConstants);

    var mappings =
        JsonNode.Parse(job.InputMapping)
        as JsonObject
        ?? throw new InvalidOperationException(
            "input_mapping must be an object.");

    foreach (var mapping in mappings)
    {
        var targetPointer =
            mapping.Key;

        var sourcePointer =
            mapping.Value?.GetValue<string>()
            ?? throw new InvalidOperationException(
                "source pointer must be a string.");

        if (!TryResolvePointer(
                processData,
                sourcePointer,
                out var sourceValue))
        {
            throw new MappingMissingException();
        }

        payload = SetPointer(
            payload,
            targetPointer,
            sourceValue);
    }

    return payload;
}


static bool ValidateSchema(
    string schemaJson,
    JsonNode? instance)
{
    try
    {
        var buildOptions =
            new BuildOptions
            {
                SchemaRegistry =
                    new SchemaRegistry()
            };

        var schema =
            JsonSchema.FromText(
                schemaJson,
                buildOptions);

        using var document =
            JsonDocument.Parse(
                instance?.ToJsonString()
                ?? "null");

        var result =
            schema.Evaluate(
                document.RootElement,
                new EvaluationOptions
                {
                    OutputFormat =
                        OutputFormat.Flag
                });

        return result.IsValid;
    }
    catch
    {
        return false;
    }
}


static bool IsDeclaredOutcome(
    string outcomesJson,
    string outcome)
{
    var outcomes =
        JsonNode.Parse(outcomesJson)
        as JsonArray;

    if (outcomes is null)
    {
        return false;
    }

    return outcomes.Any(
        node =>
            node is not null
            && string.Equals(
                node.GetValue<string>(),
                outcome,
                StringComparison.Ordinal));
}


static bool TryResolvePointer(
    JsonNode? root,
    string pointer,
    out JsonNode? value)
{
    value = root;

    var segments =
        ParsePointer(pointer);

    if (segments.Length == 0)
    {
        value = root?.DeepClone();
        return true;
    }

    JsonNode? current =
        root;

    foreach (var segment in segments)
    {
        if (current is JsonObject obj)
        {
            if (!obj.TryGetPropertyValue(
                    segment,
                    out current))
            {
                value = null;
                return false;
            }

            continue;
        }

        if (current is JsonArray array)
        {
            if (!TryCanonicalArrayIndex(
                    segment,
                    out var index)
                || index >= array.Count)
            {
                value = null;
                return false;
            }

            current =
                array[index];

            continue;
        }

        value = null;
        return false;
    }

    value =
        current?.DeepClone();

    return true;
}


static JsonNode? SetPointer(
    JsonNode? root,
    string pointer,
    JsonNode? value)
{
    var segments =
        ParsePointer(pointer);

    if (segments.Length == 0)
    {
        return value?.DeepClone();
    }

    root ??= new JsonObject();

    JsonNode current =
        root;

    for (var i = 0;
         i < segments.Length - 1;
         i++)
    {
        var segment =
            segments[i];

        var next =
            segments[i + 1];

        if (current is JsonObject obj)
        {
            if (!obj.TryGetPropertyValue(
                    segment,
                    out var child)
                || child is null)
            {
                child =
                    LooksLikeArrayIndex(next)
                        ? new JsonArray()
                        : new JsonObject();

                obj[segment] =
                    child;
            }

            current =
                child;

            continue;
        }

        if (current is JsonArray array)
        {
            if (!TryArrayIndex(
                    segment,
                    array.Count,
                    allowAppend: true,
                    out var index))
            {
                throw new InvalidOperationException(
                    "invalid target JSON Pointer");
            }

            JsonNode child;

            if (index == array.Count)
            {
                child =
                    LooksLikeArrayIndex(next)
                        ? new JsonArray()
                        : new JsonObject();

                array.Add(child);
            }
            else
            {
                child =
                    array[index]
                    ?? (
                        LooksLikeArrayIndex(next)
                            ? new JsonArray()
                            : new JsonObject());

                if (array[index] is null)
                {
                    array[index] =
                        child;
                }
            }

            current =
                child;

            continue;
        }

        throw new InvalidOperationException(
            "target JSON Pointer crosses a scalar value");
    }

    var last =
        segments[^1];

    if (current is JsonObject finalObject)
    {
        finalObject[last] =
            value?.DeepClone();

        return root;
    }

    if (current is JsonArray finalArray)
    {
        if (!TryArrayIndex(
                last,
                finalArray.Count,
                allowAppend: true,
                out var index))
        {
            throw new InvalidOperationException(
                "invalid target JSON Pointer");
        }

        if (index == finalArray.Count)
        {
            finalArray.Add(
                value?.DeepClone());
        }
        else
        {
            finalArray[index] =
                value?.DeepClone();
        }

        return root;
    }

    throw new InvalidOperationException(
        "target JSON Pointer cannot address scalar");
}


static string[] ParsePointer(
    string pointer)
{
    if (pointer.Length == 0)
    {
        return [];
    }

    if (!pointer.StartsWith('/'))
    {
        throw new InvalidOperationException(
            "invalid JSON Pointer");
    }

    return pointer
        .Split('/')[1..]
        .Select(UnescapePointerSegment)
        .ToArray();
}


static string UnescapePointerSegment(
    string value)
{
    var result =
        new System.Text.StringBuilder();

    for (var i = 0;
         i < value.Length;
         i++)
    {
        if (value[i] != '~')
        {
            result.Append(
                value[i]);

            continue;
        }

        if (i + 1 >= value.Length)
        {
            throw new InvalidOperationException(
                "invalid JSON Pointer escape");
        }

        i++;

        result.Append(
            value[i] switch
            {
                '0' => '~',
                '1' => '/',
                _ => throw new InvalidOperationException(
                    "invalid JSON Pointer escape")
            });
    }

    return result.ToString();
}


static bool LooksLikeArrayIndex(
    string value)
{
    return value == "-"
        || TryCanonicalArrayIndex(
            value,
            out _);
}


static bool TryArrayIndex(
    string value,
    int count,
    bool allowAppend,
    out int index)
{
    if (value == "-"
        && allowAppend)
    {
        index = count;
        return true;
    }

    if (!TryCanonicalArrayIndex(
            value,
            out index))
    {
        return false;
    }

    return index < count
        || (
            allowAppend
            && index == count
        );
}


static bool TryCanonicalArrayIndex(
    string value,
    out int index)
{
    index = 0;

    if (string.IsNullOrEmpty(value))
    {
        return false;
    }

    if (value.Length > 1
        && value[0] == '0')
    {
        return false;
    }

    foreach (var character in value)
    {
        if (character < '0'
            || character > '9')
        {
            return false;
        }
    }

    return int.TryParse(
        value,
        System.Globalization.NumberStyles.None,
        System.Globalization.CultureInfo.InvariantCulture,
        out index);
}

static async Task ReachFailpointAsync(
    string name,
    string instanceId,
    CancellationToken cancellationToken)
{
    var enabled =
        Environment.GetEnvironmentVariable(
            "COURSE_TEST_PROFILE") == "1";

    var requested =
        Environment.GetEnvironmentVariable(
            "COURSE_FAILPOINT");

    if (!enabled
        || !string.Equals(
            requested,
            name,
            StringComparison.Ordinal))
    {
        return;
    }

    Console.WriteLine(
        JsonSerializer.Serialize(
            new
            {
                @event = "failpoint.reached",
                name,
                instanceId
            }));
    await Console.Out.FlushAsync(cancellationToken);

    // Checker deliberately kills this container.
    await Task.Delay(
        Timeout.InfiniteTimeSpan,
        cancellationToken);
}


public enum JobLeaseStop { Running, NormalStop, Timeout, LeaseLost, Shutdown }

public static class JobLeasePolicy
{
    public static int IntervalMs(int leaseMs)
    {
        if (leaseMs is < 100 or > 300000)
            throw new ArgumentOutOfRangeException(nameof(leaseMs));
        return Math.Clamp(leaseMs / 4, 25, 1000);
    }

    public static int RemainingTaskMs(int timeoutMs, double elapsedMs)
    {
        if (timeoutMs is < 1 or > 30000)
            throw new ArgumentOutOfRangeException(nameof(timeoutMs));
        return (int)Math.Max(0, Math.Floor(timeoutMs - Math.Max(0, elapsedMs)));
    }

    public static int CompletionBudgetMs(int leaseMs, double sinceRenewalMs) =>
        (int)Math.Max(0, Math.Min(1000,
            Math.Floor(leaseMs - Math.Max(0, sinceRenewalMs) - IntervalMs(leaseMs))));

    public static JobLeaseStop Classify(bool shutdown, bool lost, bool timeout, bool stopped) =>
        shutdown ? JobLeaseStop.Shutdown : lost ? JobLeaseStop.LeaseLost :
        timeout ? JobLeaseStop.Timeout : stopped ? JobLeaseStop.NormalStop : JobLeaseStop.Running;
}

public sealed class JobLeaseHeartbeat : IAsyncDisposable
{
    private readonly int leaseMs;
    private readonly int timeoutMs;
    private readonly long startedAt = System.Diagnostics.Stopwatch.GetTimestamp();
    private long lastRenewalStarted;
    private readonly CancellationToken shutdown;
    private readonly CancellationTokenSource deadline = new();
    private readonly CancellationTokenSource lost = new();
    private readonly CancellationTokenSource stop = new();
    private readonly CancellationTokenSource work;
    private readonly CancellationTokenSource delay;
    private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task loop;
    private int stoppedReason;

    public JobLeaseHeartbeat(int leaseMs, int timeoutMs, CancellationToken shutdown,
        Func<CancellationToken, Task<bool>> renew, long claimStartedAt)
    {
        JobLeasePolicy.IntervalMs(leaseMs);
        JobLeasePolicy.RemainingTaskMs(timeoutMs, 0);
        this.leaseMs = leaseMs;
        this.timeoutMs = timeoutMs;
        this.shutdown = shutdown;
        lastRenewalStarted = claimStartedAt;
        Deadline = DateTimeOffset.UtcNow.AddMilliseconds(
            JobLeasePolicy.RemainingTaskMs(timeoutMs, ElapsedMs));
        work = CancellationTokenSource.CreateLinkedTokenSource(shutdown, deadline.Token, lost.Token);
        delay = CancellationTokenSource.CreateLinkedTokenSource(work.Token, stop.Token);
        deadline.CancelAfter(JobLeasePolicy.RemainingTaskMs(timeoutMs, ElapsedMs));
        loop = RunAsync(renew); // Owned and joined by StopAsync/DisposeAsync.
    }

    public DateTimeOffset Deadline { get; }
    public CancellationToken Token => work.Token;
    public Task Started => started.Task;
    public string DiagnosticCode { get; private set; } = "workflow.lease_lost";
    private double ElapsedMs => System.Diagnostics.Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;
    public int CompletionBudgetMs => JobLeasePolicy.CompletionBudgetMs(leaseMs,
        System.Diagnostics.Stopwatch.GetElapsedTime(Volatile.Read(ref lastRenewalStarted)).TotalMilliseconds);
    public JobLeaseStop Reason => shutdown.IsCancellationRequested ? JobLeaseStop.Shutdown :
        Volatile.Read(ref stoppedReason) is var reason && reason != 0 ? (JobLeaseStop)reason :
        JobLeasePolicy.Classify(false, lost.IsCancellationRequested,
            deadline.IsCancellationRequested || ElapsedMs >= timeoutMs, stop.IsCancellationRequested);

    private async Task RunAsync(Func<CancellationToken, Task<bool>> renew)
    {
        try
        {
            while (!delay.IsCancellationRequested)
            {
                var remaining = JobLeasePolicy.RemainingTaskMs(timeoutMs, ElapsedMs);
                if (remaining == 0)
                {
                    deadline.Cancel();
                    return;
                }
                var renewalStarted = System.Diagnostics.Stopwatch.GetTimestamp();
                try
                {
                    using var request = CancellationTokenSource.CreateLinkedTokenSource(work.Token);
                    request.CancelAfter(Math.Min(JobLeasePolicy.IntervalMs(leaseMs), remaining));
                    if (!await renew(request.Token))
                    {
                        lost.Cancel();
                        return;
                    }
                    Volatile.Write(ref lastRenewalStarted, renewalStarted);
                    started.TrySetResult();
                }
                catch (Exception)
                {
                    if (!work.IsCancellationRequested)
                    {
                        DiagnosticCode = "workflow.lease_renewal_failed";
                        lost.Cancel();
                    }
                    return;
                }
                try { await Task.Delay(JobLeasePolicy.IntervalMs(leaseMs), delay.Token); }
                catch (OperationCanceledException) when (delay.IsCancellationRequested) { return; }
            }
        }
        finally { started.TrySetResult(); }
    }

    public async Task StopAsync()
    {
        // Cancel the interval, but join any bounded renewal already in flight.
        // Normal stop must neither cancel the action nor imitate lease loss.
        stop.Cancel();
        await loop;
        Interlocked.CompareExchange(ref stoppedReason, (int)Reason, 0);
        deadline.CancelAfter(Timeout.Infinite);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        delay.Dispose();
        work.Dispose();
        stop.Dispose();
        lost.Dispose();
        deadline.Dispose();
    }
}

sealed class WorkerLoopService(Func<CancellationToken, Task> run) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => run(stoppingToken);
}

public sealed record WorkerProbeResponse(int StatusCode, string Body, string ContentType)
{
    public IResult ToResult() => Results.Text(Body, ContentType, statusCode: StatusCode);
}

public static class WorkerObservability
{
    public const string MetricsContentType = "application/openmetrics-text; version=1.0.0; charset=utf-8";
    public static WorkerProbeResponse Live() => new(200, "{\"status\":\"live\"}", "application/json");
    public static WorkerProbeResponse Metrics() => new(200, "# TYPE process_up gauge\nprocess_up 1\n# EOF\n", MetricsContentType);

    public static async Task<WorkerProbeResponse> ReadyAsync(
        Func<CancellationToken, Task> probe, CancellationToken cancellationToken)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(2));
            await probe(timeout.Token).WaitAsync(timeout.Token);
            return new(200, "{\"status\":\"ready\"}", "application/json");
        }
        catch (Exception)
        {
            return new(503, "{\"status\":\"not_ready\",\"code\":\"dependency.unavailable\"}", "application/json");
        }
    }

    public static async Task ProbePostgresAsync(string connectionString, CancellationToken token)
    {
        // A separate unpooled probe never changes action connections/timeouts.
        var settings = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = false, Timeout = 2, CommandTimeout = 2, CancellationTimeout = 500
        };
        await using var connection = new NpgsqlConnection(settings.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand("SELECT 1", connection);
        await command.ExecuteScalarAsync(token);
    }
}

sealed record ClaimedJob(
    Guid JobId,
    Guid ProcessId,
    Guid StepInstanceId,
    Guid ExecutionId,
    Guid AttemptId,
    long LeaseVersion,
    int AttemptNumber,
    string ProcessData,
    string Module,
    string Action,
    int ActionVersion,
    string RequestSchema,
    string ResponseSchema,
    string Outcomes,
    string RequiredPolicy,
    int TimeoutMs,
    int MaxAttempts,
    string DelaysMs,
    string InputMapping,
    string InputConstants);


sealed class MappingMissingException : Exception;
