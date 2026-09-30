using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json.Nodes;
using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;


var workerDll =
    Path.Combine(
        AppContext.BaseDirectory,
        "Workflow.Worker.dll");

if (!File.Exists(workerDll))
{
    Console.Error.WriteLine(
        $"Worker assembly was not found: {workerDll}");

    return 2;
}


var assembly =
    AssemblyLoadContext.Default
        .LoadFromAssemblyPath(
            workerDll);


var programType =
    assembly.GetType("Program")
    ?? throw new InvalidOperationException(
        "Production Program type was not found.");


const BindingFlags StaticFlags =
    BindingFlags.Static
    | BindingFlags.Public
    | BindingFlags.NonPublic;


MethodInfo FindProductionMethod(
    string fragment)
{
    var methods =
        programType
            .GetMethods(StaticFlags)
            .Where(
                method =>
                    method.Name.Contains(
                        fragment,
                        StringComparison.Ordinal))
            .ToArray();

    if (methods.Length != 1)
    {
        throw new InvalidOperationException(
            $"Expected exactly one production "
            + $"{fragment} method, found "
            + $"{methods.Length}.");
    }

    return methods[0];
}


var validateSchema =
    FindProductionMethod(
        "ValidateSchema");

var buildPayload =
    FindProductionMethod(
        "BuildPayload");


var claimedJobType =
    assembly
        .GetTypes()
        .Single(
            type =>
                type.Name == "ClaimedJob");


var claimedJobConstructor =
    claimedJobType
        .GetConstructors(
            BindingFlags.Instance
            | BindingFlags.Public
            | BindingFlags.NonPublic)
        .Single(
            constructor =>
                constructor
                    .GetParameters()
                    .Length == 20);


var passed = 0;
var failed = 0;


void Check(
    string name,
    bool condition,
    string? detail = null)
{
    if (condition)
    {
        passed++;

        Console.WriteLine(
            $"PASS | {name}"
            + (
                string.IsNullOrWhiteSpace(
                    detail)
                    ? ""
                    : $" | {detail}"
            ));

        return;
    }

    failed++;

    Console.WriteLine(
        $"FAIL | {name}"
        + (
            string.IsNullOrWhiteSpace(
                detail)
                ? ""
                : $" | {detail}"
        ));
}


bool Validate(
    string schema,
    JsonNode? instance)
{
    return validateSchema.Invoke(
        null,
        new object?[]
        {
            schema,
            instance
        }) is true;
}


object CreateJob(
    string processData,
    string inputMapping,
    string inputConstants)
{
    return claimedJobConstructor.Invoke(
        new object?[]
        {
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),

            1L,
            1,

            processData,

            "logic-test-module",
            "logic-test-action",
            1,

            """{"type":"object"}""",
            """{"type":"object"}""",

            """["ok"]""",
            """[]""",

            1000,
            1,
            """[]""",

            inputMapping,
            inputConstants
        });
}


JsonNode? Build(
    string processData,
    string inputMapping,
    string inputConstants)
{
    var job =
        CreateJob(
            processData,
            inputMapping,
            inputConstants);

    return (JsonNode?)
        buildPayload.Invoke(
            null,
            new[]
            {
                job
            });
}


Exception? BuildFailure(
    string processData,
    string inputMapping,
    string inputConstants)
{
    try
    {
        Build(
            processData,
            inputMapping,
            inputConstants);

        return null;
    }
    catch (TargetInvocationException ex)
    {
        return ex.InnerException
            ?? ex;
    }
}


Console.WriteLine(
    "===== PRODUCTION SCHEMA VALIDATOR =====");


var simpleSchema =
    """
    {
      "type": "object",
      "required": ["name"],
      "properties": {
        "name": {
          "type": "string"
        }
      },
      "additionalProperties": false
    }
    """;


Check(
    "valid instance accepted",
    Validate(
        simpleSchema,
        JsonNode.Parse(
            """{"name":"Alice"}""")));


Check(
    "missing required property rejected",
    !Validate(
        simpleSchema,
        JsonNode.Parse(
            """{}""")));


Check(
    "wrong property type rejected",
    !Validate(
        simpleSchema,
        JsonNode.Parse(
            """{"name":123}""")));


Check(
    "malformed schema fails closed",
    !Validate(
        """{"type":""",
        JsonNode.Parse(
            """{"name":"Alice"}""")));


var repeatedId =
    "urn:week2-regression:"
    + Guid.NewGuid().ToString("N");


var schemaWithId =
    new JsonObject
    {
        ["$id"] =
            repeatedId,

        ["type"] =
            "object",

        ["required"] =
            new JsonArray(
                "value"),

        ["properties"] =
            new JsonObject
            {
                ["value"] =
                    new JsonObject
                    {
                        ["type"] =
                            "string"
                    }
            }
    }.ToJsonString();


var repeatedInstance =
    JsonNode.Parse(
        """{"value":"ok"}""");


Check(
    "schema with $id validates first time",
    Validate(
        schemaWithId,
        repeatedInstance));


Check(
    "same schema $id validates repeatedly",
    Validate(
        schemaWithId,
        repeatedInstance));


Check(
    "same schema still rejects invalid instance",
    !Validate(
        schemaWithId,
        JsonNode.Parse(
            """{"value":123}""")));


async Task<(int Status, string? Type, string Body)> ExecuteResult(IResult result)
{
    using var services = new ServiceCollection().AddLogging().BuildServiceProvider();
    var context = new DefaultHttpContext { RequestServices = services };
    using var body = new MemoryStream();
    context.Response.Body = body;
    await result.ExecuteAsync(context);
    return (context.Response.StatusCode, context.Response.ContentType,
        new UTF8Encoding(false, true).GetString(body.ToArray()));
}

var live = await ExecuteResult(WorkerObservability.Live().ToResult());
Check("worker exact live HTTP response", live.Status == 200
    && live.Type!.StartsWith("application/json") && live.Body == "{\"status\":\"live\"}");
var ready = await WorkerObservability.ReadyAsync(_ => Task.CompletedTask, CancellationToken.None);
Check("worker exact ready response", ready.StatusCode == 200 && ready.Body == "{\"status\":\"ready\"}");
var unavailable = await WorkerObservability.ReadyAsync(
    _ => Task.FromException(new Exception("secret-canary")), CancellationToken.None);
var failedProbe = await ExecuteResult(unavailable.ToResult());
Check("worker safe readiness failure", failedProbe.Status == 503
    && failedProbe.Type!.StartsWith("application/json")
    && failedProbe.Body == "{\"status\":\"not_ready\",\"code\":\"dependency.unavailable\"}");
using (var cancelled = new CancellationTokenSource())
{
    cancelled.Cancel();
    var stopped = await WorkerObservability.ReadyAsync(ct => Task.Delay(Timeout.Infinite, ct), cancelled.Token);
    Check("worker readiness cancellation is safe", stopped == unavailable);
}
var metrics = await ExecuteResult(WorkerObservability.Metrics().ToResult());
Check("worker exact OpenMetrics media type", metrics.Status == 200 && metrics.Type == WorkerObservability.MetricsContentType);
Check("worker exact LF-only metrics body", metrics.Body == "# TYPE process_up gauge\nprocess_up 1\n# EOF\n"
    && !metrics.Body.Contains('\r'));
var cultureBefore = CultureInfo.CurrentCulture;
try
{
    CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("ru-RU");
    var rendered = ApiObservability.Render(new(2, 1.25, 3, 4, 2.5, 1));
    const string expectedMetrics = "# TYPE workflow_jobs_ready gauge\nworkflow_jobs_ready 2\n"
        + "# TYPE workflow_job_oldest_age_seconds gauge\nworkflow_job_oldest_age_seconds 1.25\n"
        + "# TYPE workflow_processes_waiting gauge\nworkflow_processes_waiting 3\n"
        + "# TYPE outbox_pending gauge\noutbox_pending 4\n"
        + "# TYPE outbox_oldest_age_seconds gauge\noutbox_oldest_age_seconds 2.5\n"
        + "# TYPE workflow_failures counter\nworkflow_failures_total 1\n# EOF\n";
    Check("API production renderer is invariant and exact", rendered == expectedMetrics);
    var wire = await ExecuteResult(Results.Text(rendered, ApiObservability.MetricsContentType));
    Check("API exact OpenMetrics serialization", wire.Type == ApiObservability.MetricsContentType && wire.Body == rendered);
    bool RejectMetric(ApiMetricSnapshot value)
    {
        try { ApiObservability.Render(value); return false; }
        catch (ArgumentOutOfRangeException) { return true; }
    }
    Check("API metrics reject non-finite ages", new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity }
        .All(v => RejectMetric(new(0, v, 0, 0, 0, 0)) && RejectMetric(new(0, 0, 0, 0, v, 0))));
    Check("API metrics reject negative counts and ages", new ApiMetricSnapshot[] {
        new(-1,0,0,0,0,0), new(0,-1,0,0,0,0), new(0,0,-1,0,0,0),
        new(0,0,0,-1,0,0), new(0,0,0,0,-1,0), new(0,0,0,0,0,-1) }.All(RejectMetric));
}
finally { CultureInfo.CurrentCulture = cultureBefore; }
// Malformed settings fail before opening a socket; these are offline tests.
var apiReadyFailure = await ExecuteResult(await ApiObservability.ReadyAsync("secret-canary", CancellationToken.None));
var apiMetricsFailure = await ExecuteResult(await ApiObservability.MetricsAsync("secret-canary", CancellationToken.None));
Check("API readiness safely rejects invalid dependency settings", apiReadyFailure == failedProbe);
Check("API metrics failure returns no stale business values", apiMetricsFailure == failedProbe);

Console.WriteLine();
Console.WriteLine(
    "===== PRODUCTION PAYLOAD MAPPER =====");


var processData =
    """
    {
      "user": {
        "name": "Alice"
      },
      "a/b": {
        "~key": "escaped-value"
      },
      "items": [
        {
          "value": 7
        },
        {
          "value": 8
        }
      ]
    }
    """;


var mapped =
    Build(
        processData,
        """
        {
          "/customer/name": "/user/name",
          "/escaped": "/a~1b/~0key",
          "/values/0": "/items/1/value"
        }
        """,
        """
        {
          "fixed": true
        }
        """);


var expectedMapped =
    JsonNode.Parse(
        """
        {
          "fixed": true,
          "customer": {
            "name": "Alice"
          },
          "escaped": "escaped-value",
          "values": [
            8
          ]
        }
        """);


Check(
    "constants and nested mappings",
    JsonNode.DeepEquals(
        mapped,
        expectedMapped),
    mapped?.ToJsonString());


var rootMapped =
    Build(
        processData,
        """
        {
          "": "/user/name"
        }
        """,
        """
        {
          "ignored": true
        }
        """);


Check(
    "empty target pointer replaces root",
    rootMapped?.GetValue<string>()
        == "Alice",
    rootMapped?.ToJsonString());


var appended =
    Build(
        processData,
        """
        {
          "/values/-": "/items/0/value"
        }
        """,
        "{}");


Check(
    "array append target",
    JsonNode.DeepEquals(
        appended,
        JsonNode.Parse(
            """
            {
              "values": [
                7
              ]
            }
            """)),
    appended?.ToJsonString());


Console.WriteLine();
Console.WriteLine(
    "===== MAPPER BOUNDARIES =====");


var missing =
    BuildFailure(
        processData,
        """
        {
          "/value": "/missing/path"
        }
        """,
        "{}");


Check(
    "missing source reports MappingMissingException",
    missing?.GetType().Name
        == "MappingMissingException",
    missing?.GetType().FullName);


var nonCanonical =
    BuildFailure(
        processData,
        """
        {
          "/value": "/items/01/value"
        }
        """,
        "{}");


Check(
    "non-canonical source array index rejected",
    nonCanonical?.GetType().Name
        == "MappingMissingException",
    nonCanonical?.GetType().FullName);


var invalidSourceEscape =
    BuildFailure(
        processData,
        """
        {
          "/value": "/a~2b"
        }
        """,
        "{}");


Check(
    "invalid source pointer escape rejected",
    invalidSourceEscape
        is InvalidOperationException,
    invalidSourceEscape?.Message);


var invalidTargetEscape =
    BuildFailure(
        processData,
        """
        {
          "/bad~2target": "/user/name"
        }
        """,
        "{}");


Check(
    "invalid target pointer escape rejected",
    invalidTargetEscape
        is InvalidOperationException,
    invalidTargetEscape?.Message);


var scalarCrossing =
    BuildFailure(
        processData,
        """
        {
          "/fixed/child/value": "/user/name"
        }
        """,
        """
        {
          "fixed": "scalar"
        }
        """);


Check(
    "mapping cannot cross scalar constant",
    scalarCrossing
        is InvalidOperationException,
    scalarCrossing?.Message);


var invalidMapping =
    BuildFailure(
        processData,
        "[]",
        "{}");


Check(
    "input_mapping must be object",
    invalidMapping
        is InvalidOperationException,
    invalidMapping?.Message);


var nonStringSource =
    BuildFailure(
        processData,
        """
        {
          "/value": 123
        }
        """,
        "{}");


Check(
    "source pointer must be string",
    nonStringSource
        is InvalidOperationException,
    nonStringSource?.Message);


var readInteger = FindProductionMethod("ReadBoundedIntegerEnvironment");
const string configName = "COURSE_WORKER_POLL_MS";
var originalConfig = Environment.GetEnvironmentVariable(configName);
int ReadConfig(string? value, int fallback = 50)
{
    Environment.SetEnvironmentVariable(configName, value);
    return (int)readInteger.Invoke(null, new object[] { configName, fallback, 1, 60000 })!;
}
bool RejectConfig(string value)
{
    try { ReadConfig(value); return false; }
    catch (TargetInvocationException error)
    {
        return error.InnerException is InvalidOperationException;
    }
}
try
{
    Check("configuration absent uses test default", ReadConfig(null) == 50);
    Check("configuration absent uses normal default", ReadConfig(null, 250) == 250);
    Check("configuration explicit values override defaults",
        ReadConfig("123") == 123 && ReadConfig("000100") == 100);
    Check("configuration inclusive boundaries", ReadConfig("1") == 1 && ReadConfig("60000") == 60000);
    // Empty process environment values are not portable across .NET platforms.
    // Exercise nonempty malformed values here; Python also tests explicit empty.
    Check("configuration malformed values fail closed",
        new[] { " ", "1.5", "-1", "+1", "12ms", "\uFF11\uFF12", "1\n", "999999999999" }.All(RejectConfig));
    Check("configuration out of range fails closed",
        new[] { "0", "60001", "999999999" }.All(RejectConfig));
    var safeConfigError = false;
    try { ReadConfig("secret-canary"); }
    catch (TargetInvocationException error)
    {
        safeConfigError = error.InnerException?.Message == configName + ": invalid integer configuration.";
    }
    Check("configuration error does not echo value", safeConfigError);
}
finally
{
    Environment.SetEnvironmentVariable(configName, originalConfig);
}

Console.WriteLine("===== JOB LEASE HEARTBEAT =====");
Check("heartbeat interval bounds and 2s cadence",
    JobLeasePolicy.IntervalMs(100) == 25 && JobLeasePolicy.IntervalMs(2000) == 500
    && JobLeasePolicy.IntervalMs(10000) == 1000 && JobLeasePolicy.IntervalMs(300000) == 1000);
bool InvalidLease(int value)
{
    try { JobLeasePolicy.IntervalMs(value); return false; }
    catch (ArgumentOutOfRangeException) { return true; }
}
Check("heartbeat invalid lease rejected", InvalidLease(99) && InvalidLease(300001));
Check("heartbeat absolute task budget bounds",
    JobLeasePolicy.RemainingTaskMs(1, 0) == 1 && JobLeasePolicy.RemainingTaskMs(30000, 100) == 29900
    && JobLeasePolicy.RemainingTaskMs(30000, 30001) == 0 && JobLeasePolicy.RemainingTaskMs(1, -1) == 1);
Check("heartbeat finish budget retains safety margin",
    JobLeasePolicy.CompletionBudgetMs(2000, 0) == 1000
    && JobLeasePolicy.CompletionBudgetMs(2000, 1000) == 500
    && JobLeasePolicy.CompletionBudgetMs(2000, 1500) == 0);
Check("heartbeat stop reasons and precedence",
    JobLeasePolicy.Classify(false, false, false, false) == JobLeaseStop.Running
    && JobLeasePolicy.Classify(false, false, false, true) == JobLeaseStop.NormalStop
    && JobLeasePolicy.Classify(false, false, true, true) == JobLeaseStop.Timeout
    && JobLeasePolicy.Classify(false, true, true, true) == JobLeaseStop.LeaseLost
    && JobLeasePolicy.Classify(true, true, true, true) == JobLeaseStop.Shutdown);
long Timestamp() => System.Diagnostics.Stopwatch.GetTimestamp();
await using (var heartbeat = new JobLeaseHeartbeat(2000, 30000, CancellationToken.None,
    _ => Task.FromResult(true), Timestamp()))
{
    var latestDeadline = DateTimeOffset.UtcNow.AddMilliseconds(30000);
    await heartbeat.Started.WaitAsync(TimeSpan.FromSeconds(2));
    await heartbeat.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
    Check("heartbeat normal stop joins without cancelling action",
        heartbeat.Reason == JobLeaseStop.NormalStop && !heartbeat.Token.IsCancellationRequested);
    Check("heartbeat deadline never exceeds task timeout", heartbeat.Deadline <= latestDeadline);
}
await using (var heartbeat = new JobLeaseHeartbeat(2000, 30000, CancellationToken.None,
    _ => Task.FromResult(false), Timestamp()))
{
    await heartbeat.Started.WaitAsync(TimeSpan.FromSeconds(2));
    await heartbeat.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
    Check("renew false cancels action and classifies lease loss", heartbeat.Reason == JobLeaseStop.LeaseLost
        && heartbeat.Token.IsCancellationRequested && heartbeat.DiagnosticCode == "workflow.lease_lost");
}
await using (var heartbeat = new JobLeaseHeartbeat(2000, 30000, CancellationToken.None,
    _ => Task.FromException<bool>(new Npgsql.NpgsqlException("secret-canary")), Timestamp()))
{
    await heartbeat.Started.WaitAsync(TimeSpan.FromSeconds(2));
    await heartbeat.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
    Check("renew DB failure cancels safely without raw exception", heartbeat.Reason == JobLeaseStop.LeaseLost
        && heartbeat.Token.IsCancellationRequested && heartbeat.DiagnosticCode == "workflow.lease_renewal_failed");
}
using (var shutdown = new CancellationTokenSource())
{
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var heartbeat = new JobLeaseHeartbeat(2000, 30000, shutdown.Token, async ct =>
    {
        entered.SetResult();
        await Task.Delay(Timeout.Infinite, ct);
        return true;
    }, Timestamp());
    await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
    shutdown.Cancel();
    await heartbeat.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
    Check("shutdown cancels in-flight renewal and action", heartbeat.Reason == JobLeaseStop.Shutdown
        && heartbeat.Token.IsCancellationRequested);
}
await using (var heartbeat = new JobLeaseHeartbeat(2000, 30, CancellationToken.None,
    _ => Task.FromResult(true), Timestamp()))
{
    var cancelled = false;
    try { await Task.Delay(Timeout.Infinite, heartbeat.Token).WaitAsync(TimeSpan.FromSeconds(2)); }
    catch (OperationCanceledException) { cancelled = true; }
    await heartbeat.StopAsync().WaitAsync(TimeSpan.FromSeconds(2));
    Check("absolute timeout cancels action and ends renewal", cancelled && heartbeat.Reason == JobLeaseStop.Timeout);
}
{
    var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    CancellationToken requestToken = default;
    await using var heartbeat = new JobLeaseHeartbeat(2000, 30000, CancellationToken.None, ct =>
    {
        requestToken = ct;
        entered.SetResult();
        return release.Task;
    }, Timestamp());
    await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
    var stopping = heartbeat.StopAsync();
    var waits = !stopping.IsCompleted && !requestToken.IsCancellationRequested;
    release.SetResult(true);
    await stopping.WaitAsync(TimeSpan.FromSeconds(2));
    Check("normal stop awaits in-flight renewal before finish", waits && heartbeat.Reason == JobLeaseStop.NormalStop);
}
var reachFailpoint = FindProductionMethod("ReachFailpointAsync");
var originalProfile = Environment.GetEnvironmentVariable("COURSE_TEST_PROFILE");
var originalFailpoint = Environment.GetEnvironmentVariable("COURSE_FAILPOINT");
try
{
    Environment.SetEnvironmentVariable("COURSE_TEST_PROFILE", "1");
    foreach (var point in new[] { "after_job_claim", "after_action_before_finish" })
    {
        Environment.SetEnvironmentVariable("COURSE_FAILPOINT", point);
        using var cancellation = new CancellationTokenSource();
        using var output = new StringWriter();
        var previousOutput = Console.Out;
        var cancelled = false;
        string acknowledgement;
        try
        {
            Console.SetOut(output);
            var wait = (Task)reachFailpoint.Invoke(null, new object[] { point, "logic-worker", cancellation.Token })!;
            acknowledgement = output.ToString().Trim();
            cancellation.Cancel();
            try { await wait.WaitAsync(TimeSpan.FromSeconds(2)); }
            catch (OperationCanceledException) { cancelled = true; }
        }
        finally { Console.SetOut(previousOutput); }
        Check(point + " exact acknowledgement and no continuation on cancellation",
            cancelled && JsonNode.DeepEquals(JsonNode.Parse(acknowledgement), new JsonObject {
                ["event"] = "failpoint.reached", ["name"] = point, ["instanceId"] = "logic-worker" }));
    }
}
finally
{
    Environment.SetEnvironmentVariable("COURSE_TEST_PROFILE", originalProfile);
    Environment.SetEnvironmentVariable("COURSE_FAILPOINT", originalFailpoint);
}
{
    using var output = new StringWriter();
    var previousError = Console.Error;
    try
    {
        Console.SetError(output);
        FindProductionMethod("WriteJobDiagnostic").Invoke(null, new object?[] {
            "logic-worker", CreateJob("{\"password\":\"payload-canary\"}", "{}", "{}"),
            "workflow.lease_lost", false, new Npgsql.NpgsqlException("secret-canary") });
    }
    finally { Console.SetError(previousError); }
    var text = output.ToString();
    var diagnostic = JsonNode.Parse(text)!;
    Check("lease diagnostic excludes payload and raw exception", !text.Contains("secret-canary")
        && !text.Contains("payload-canary") && diagnostic["errorCode"]?.GetValue<string>() == "workflow.lease_lost"
        && diagnostic["executionId"] is not null && diagnostic["leaseVersion"] is not null);
}
{
    var cleared = 0;
    ApiPoolRecovery.InvalidateOnFailure(new Npgsql.NpgsqlException("secret-canary"), () => cleared++);
    ApiPoolRecovery.InvalidateOnFailure(new TimeoutException("secret-canary"), () => cleared++);
    ApiPoolRecovery.InvalidateOnFailure(new OperationCanceledException(), () => cleared++);
    Check("API dependency failures invalidate pool once without action replay", cleared == 3);
    ApiPoolRecovery.InvalidateOnFailure(new ArgumentException("secret-canary"), () => cleared++);
    Check("API unrelated errors do not clear pools", cleared == 3);
}

Console.WriteLine();
Console.WriteLine(
    "===== RESULT =====");

Console.WriteLine(
    $"passed={passed}");

Console.WriteLine(
    $"failed={failed}");


if (failed != 0)
{
    Console.Error.WriteLine(
        "WORKER LOGIC REGRESSION FAILED");

    return 1;
}


Console.WriteLine(
    "WORKER LOGIC REGRESSION PASSED");

return 0;
