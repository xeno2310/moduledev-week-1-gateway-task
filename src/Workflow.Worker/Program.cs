using System.Runtime.InteropServices;
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

var testProfile =
    Environment.GetEnvironmentVariable("COURSE_TEST_PROFILE") == "1";

var leaseMs = testProfile ? 2000 : 10000;
var pollMs = testProfile ? 50 : 250;

using var shutdown = new CancellationTokenSource();

using var sigTerm = PosixSignalRegistration.Create(
    PosixSignal.SIGTERM,
    context =>
    {
        context.Cancel = true;
        shutdown.Cancel();
    });

using var sigInt = PosixSignalRegistration.Create(
    PosixSignal.SIGINT,
    context =>
    {
        context.Cancel = true;
        shutdown.Cancel();
    });

await RunWorkerAsync(
    connectionString,
    instanceId,
    leaseMs,
    pollMs,
    shutdown.Token);


static async Task RunWorkerAsync(
    string connectionString,
    string instanceId,
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

                await ReachFailpointAsync(
                    "after_job_claim",
                    instanceId);

                await ProcessJobAsync(
                    connection,
                    instanceId,
                    job,
                    cancellationToken);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(
                $"worker loop error: {ex.GetType().Name}: {ex.Message}");

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


static async Task ProcessJobAsync(
    NpgsqlConnection connection,
    string owner,
    ClaimedJob job,
    CancellationToken cancellationToken)
{
    JsonNode? payload;

    try
    {
        payload = BuildPayload(job);

        if (!ValidateSchema(
                job.RequestSchema,
                payload))
        {
            await FailAsync(
                connection,
                job,
                owner,
                "workflow.request_invalid",
                retryable: false,
                cancellationToken);

            return;
        }
    }
    catch (MappingMissingException)
    {
        await FailAsync(
            connection,
            job,
            owner,
            "workflow.mapping_missing",
            retryable: false,
            cancellationToken);

        return;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(
            $"mapping error: {ex.Message}");

        await FailAsync(
            connection,
            job,
            owner,
            "workflow.mapping_invalid",
            retryable: false,
            cancellationToken);

        return;
    }

    await using var transaction =
        await connection.BeginTransactionAsync(
            cancellationToken);

    try
    {
        JsonObject envelope;

        try
        {
            envelope = await InvokeActionAsync(
                connection,
                transaction,
                job,
                payload,
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            await transaction.RollbackAsync(
                CancellationToken.None);

            await FailAsync(
                connection,
                job,
                owner,
                "action.timeout",
                retryable: true,
                CancellationToken.None);

            return;
        }

        var status =
            envelope["status"]?.GetValue<string>();

        if (status == "error")
        {
            var code =
                envelope["code"]?.GetValue<string>()
                ?? "workflow.action_error";

            var retryable =
                envelope["retryable"]?.GetValue<bool>()
                ?? false;

            await transaction.RollbackAsync(
                CancellationToken.None);

            await FailAsync(
                connection,
                job,
                owner,
                code,
                retryable,
                CancellationToken.None);

            return;
        }

        if (status != "ok")
        {
            await transaction.RollbackAsync(
                CancellationToken.None);

            await FailAsync(
                connection,
                job,
                owner,
                "workflow.action_contract_invalid",
                retryable: false,
                CancellationToken.None);

            return;
        }

        var outcome =
            envelope["outcome"]?.GetValue<string>();

        if (string.IsNullOrWhiteSpace(outcome)
            || !IsDeclaredOutcome(
                job.Outcomes,
                outcome))
        {
            await transaction.RollbackAsync(
                CancellationToken.None);

            await FailAsync(
                connection,
                job,
                owner,
                "workflow.outcome_unknown",
                retryable: false,
                CancellationToken.None);

            return;
        }

        var result =
            envelope["result"]?.DeepClone();

        if (!ValidateSchema(
                job.ResponseSchema,
                result))
        {
            await transaction.RollbackAsync(
                CancellationToken.None);

            await FailAsync(
                connection,
                job,
                owner,
                "workflow.response_invalid",
                retryable: false,
                CancellationToken.None);

            return;
        }

        // Вторая обязательная crash-window:
        // action уже выполнен внутри transaction,
        // finish ещё не вызван и COMMIT ещё не было.
        await ReachFailpointAsync(
            "after_action_before_finish",
            owner);

        var finishEnvelope =
            await FinishAsync(
                connection,
                transaction,
                job,
                owner,
                outcome,
                result,
                cancellationToken);

        var finishStatus =
            finishEnvelope["status"]?.GetValue<string>();

        if (finishStatus != "ok")
        {
            var code =
                finishEnvelope["code"]?.GetValue<string>()
                ?? "workflow.finish_failed";

            await transaction.RollbackAsync(
                CancellationToken.None);

            // При fencing ничего дополнительно менять нельзя.
            if (code != "workflow.lease_stale")
            {
                await FailAsync(
                    connection,
                    job,
                    owner,
                    code,
                    retryable: false,
                    CancellationToken.None);
            }

            return;
        }

        await transaction.CommitAsync(
            cancellationToken);
    }
    catch (OperationCanceledException)
        when (cancellationToken.IsCancellationRequested)
    {
        try
        {
            await transaction.RollbackAsync(
                CancellationToken.None);
        }
        catch
        {
            // Container is shutting down.
        }

        throw;
    }
    catch (Exception ex)
    {
        try
        {
            await transaction.RollbackAsync(
                CancellationToken.None);
        }
        catch
        {
            // Connection may already be broken.
        }

        Console.Error.WriteLine(
            $"job execution error: {ex.GetType().Name}: {ex.Message}");

        try
        {
            await FailAsync(
                connection,
                job,
                owner,
                "workflow.worker_error",
                retryable: true,
                CancellationToken.None);
        }
        catch
        {
            // If the connection itself failed, lease expiry will
            // make the job recoverable by another worker.
        }
    }
}


static async Task<JsonObject> InvokeActionAsync(
    NpgsqlConnection connection,
    NpgsqlTransaction transaction,
    ClaimedJob job,
    JsonNode? payload,
    CancellationToken cancellationToken)
{
    var requiredPolicy =
        JsonNode.Parse(job.RequiredPolicy)
        as JsonArray
        ?? new JsonArray();

    var correlationId =
        Guid.NewGuid();

    var deadline =
        DateTimeOffset.UtcNow
            .AddMilliseconds(job.TimeoutMs);

    var context = new JsonObject
    {
        ["principal"] = "workflow-worker",
        ["consumer"] = "workflow-worker",

        ["scopes"] =
            requiredPolicy.DeepClone(),

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

    using var timeout =
        CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);

    timeout.CancelAfter(
        TimeSpan.FromMilliseconds(
            job.TimeoutMs));

    var raw =
        await command.ExecuteScalarAsync(
            timeout.Token);

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
    CancellationToken cancellationToken)
{
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
        var schema =
            JsonSchema.FromText(schemaJson);

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
    string instanceId)
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

    // Checker deliberately kills this container.
    await Task.Delay(
        Timeout.InfiniteTimeSpan);
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