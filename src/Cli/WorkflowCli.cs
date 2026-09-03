using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Npgsql;
using NpgsqlTypes;
using YamlDotNet.Serialization;

static class WorkflowCli
{
    public static async Task<int> RunAsync(
        string[] args)
    {
        try
        {
            if (args.Length == 3 &&
                args[1] == "validate")
            {
                var map =
                    await LoadMapAsync(args[2]);

                await ValidateMapAsync(map);

                return WriteOk(new
                {
                    resource = "flow",
                    operation = "validated",
                    flowName =
                        RequiredString(
                            map,
                            "flow_name"),
                    flowVersion =
                        RequiredInt(
                            map,
                            "version")
                });
            }

            if (args.Length == 3 &&
                args[1] == "publish")
            {
                return await PublishAsync(
                    args[2]);
            }

            if (args.Length == 2 &&
                args[1] == "list")
            {
                return await CallNoArgAsync(
                    "SELECT workflow.list_flows()::text;");
            }

            if (args.Length == 5 &&
                args[1] == "activate" &&
                args[3] == "--version" &&
                int.TryParse(
                    args[4],
                    out var version))
            {
                return await ActivateAsync(
                    args[2],
                    version);
            }

            if (args.Length is 5 or 7 &&
                args[1] == "start" &&
                args[3] == "--business-key")
            {
                string? dataFile = null;

                if (args.Length == 7)
                {
                    if (args[5] != "--data")
                    {
                        return InvalidArguments();
                    }

                    dataFile = args[6];
                }

                return await StartAsync(
                    args[2],
                    args[4],
                    dataFile);
            }

            if (args.Length == 3 &&
                args[1] == "get")
            {
                return await GetAsync(
                    args[2]);
            }

            if (args.Length == 9 &&
                args[1] == "signal" &&
                args[3] == "--type" &&
                args[5] == "--message-id" &&
                args[7] == "--payload")
            {
                return await SignalAsync(
                    args[2],
                    args[4],
                    args[6],
                    args[8]);
            }

            if (args.Length == 11 &&
                args[1] == "test-finish" &&
                args[3] == "--owner" &&
                args[5] == "--lease-version" &&
                long.TryParse(
                    args[6],
                    out var leaseVersion) &&
                args[7] == "--outcome" &&
                args[9] == "--result")
            {
                return await TestFinishAsync(
                    args[2],
                    args[4],
                    leaseVersion,
                    args[8],
                    args[10]);
            }

            return InvalidArguments();
        }
        catch (WorkflowMapInvalidException ex)
        {
            Console.Error.WriteLine(ex.Message);

            return WriteError(
                "workflow.map_invalid",
                "workflow map is invalid");
        }
        catch (JsonException ex)
        {
            Console.Error.WriteLine(ex.Message);

            return WriteError(
                "workflow.input_invalid",
                "JSON input is invalid");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(
                $"{ex.GetType().Name}: {ex.Message}");

            return WriteError(
                "workflow.command_failed",
                "workflow command failed");
        }
    }


    static async Task<int> PublishAsync(
        string path)
    {
        var map =
            await LoadMapAsync(path);

        await ValidateMapAsync(map);

        var canonical =
            CanonicalJson(map);

        var hash =
            Sha256(canonical);

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.publish_flow(
                    @document,
                    @hash
                )::text;
                """,
                connection);

        command.Parameters.Add(
            "document",
            NpgsqlDbType.Jsonb).Value =
                canonical;

        command.Parameters.AddWithValue(
            "hash",
            hash);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> ActivateAsync(
        string flowName,
        int version)
    {
        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.activate_flow(
                    @flow_name,
                    @version
                )::text;
                """,
                connection);

        command.Parameters.AddWithValue(
            "flow_name",
            flowName);

        command.Parameters.AddWithValue(
            "version",
            version);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> StartAsync(
        string flowName,
        string businessKey,
        string? dataFile)
    {
        JsonNode data;

        if (dataFile is null)
        {
            data = new JsonObject();
        }
        else
        {
            data =
                await LoadJsonAsync(
                    dataFile);
        }

        var canonical =
            CanonicalJson(data);

        var hash =
            Sha256(canonical);

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.start_process(
                    @flow_name,
                    @business_key,
                    @data,
                    @hash
                )::text;
                """,
                connection);

        command.Parameters.AddWithValue(
            "flow_name",
            flowName);

        command.Parameters.AddWithValue(
            "business_key",
            businessKey);

        command.Parameters.Add(
            "data",
            NpgsqlDbType.Jsonb).Value =
                canonical;

        command.Parameters.AddWithValue(
            "hash",
            hash);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> GetAsync(
        string processIdText)
    {
        if (!Guid.TryParse(
                processIdText,
                out var processId))
        {
            return WriteError(
                "workflow.process_not_found",
                "process id is invalid");
        }

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.get_process(
                    @process_id
                )::text;
                """,
                connection);

        command.Parameters.AddWithValue(
            "process_id",
            processId);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> SignalAsync(
        string processIdText,
        string signalType,
        string messageId,
        string payloadFile)
    {
        if (!Guid.TryParse(
                processIdText,
                out var processId))
        {
            return WriteError(
                "workflow.process_not_found",
                "process id is invalid");
        }

        var payload =
            await LoadJsonAsync(
                payloadFile);

        var canonical =
            CanonicalJson(payload);

        var hash =
            Sha256(canonical);

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.signal_process(
                    @process_id,
                    @signal_type,
                    @message_id,
                    @body,
                    @hash
                )::text;
                """,
                connection);

        command.Parameters.AddWithValue(
            "process_id",
            processId);

        command.Parameters.AddWithValue(
            "signal_type",
            signalType);

        command.Parameters.AddWithValue(
            "message_id",
            messageId);

        command.Parameters.Add(
            "body",
            NpgsqlDbType.Jsonb).Value =
                canonical;

        command.Parameters.AddWithValue(
            "hash",
            hash);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> TestFinishAsync(
        string jobIdText,
        string owner,
        long leaseVersion,
        string outcome,
        string resultFile)
    {
        if (Environment.GetEnvironmentVariable(
                "COURSE_TEST_PROFILE") != "1")
        {
            return WriteError(
                "workflow.test_profile_required",
                "test command is disabled");
        }

        if (!Guid.TryParse(
                jobIdText,
                out var jobId))
        {
            return WriteError(
                "workflow.job_not_found",
                "job id is invalid");
        }

        var result =
            await LoadJsonAsync(
                resultFile);

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                """
                SELECT workflow.finish_job(
                    @job_id,
                    @owner,
                    @lease_version,
                    @outcome,
                    @result
                )::text;
                """,
                connection);

        command.Parameters.AddWithValue(
            "job_id",
            jobId);

        command.Parameters.AddWithValue(
            "owner",
            owner);

        command.Parameters.AddWithValue(
            "lease_version",
            leaseVersion);

        command.Parameters.AddWithValue(
            "outcome",
            outcome);

        command.Parameters.Add(
            "result",
            NpgsqlDbType.Jsonb).Value =
                CanonicalJson(result);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> CallNoArgAsync(
        string sql)
    {
        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                sql,
                connection);

        return await ExecuteEnvelopeAsync(
            command);
    }


    static async Task<int> ExecuteEnvelopeAsync(
        NpgsqlCommand command)
    {
        var raw =
            await command.ExecuteScalarAsync();

        if (raw is not string json)
        {
            return WriteError(
                "workflow.command_failed",
                "database returned no result");
        }

        JsonObject? body;

        try
        {
            body =
                JsonNode.Parse(json)
                as JsonObject;
        }
        catch
        {
            body = null;
        }

        Console.WriteLine(json);

        return body?["status"]?
            .GetValue<string>() == "ok"
                ? 0
                : 1;
    }


    // ========================================================
    // Schema + semantic validation
    // ========================================================

    static async Task ValidateMapAsync(
        JsonObject map)
    {
        ValidateAgainstSchema(map);

        var flowName =
            RequiredString(
                map,
                "flow_name");

        var startStep =
            RequiredString(
                map,
                "start_step");

        var steps =
            map["steps"] as JsonArray
            ?? throw Invalid("steps");

        var transitions =
            map["transitions"] as JsonArray
            ?? throw Invalid("transitions");

        var stepObjects =
            steps
                .Select(
                    item =>
                        item as JsonObject
                        ?? throw Invalid(
                            "step"))
                .ToList();

        var stepByKey =
            new Dictionary<
                string,
                JsonObject>(
                    StringComparer.Ordinal);

        foreach (var step in stepObjects)
        {
            var key =
                RequiredString(
                    step,
                    "key");

            if (!stepByKey.TryAdd(
                    key,
                    step))
            {
                throw Invalid(
                    "duplicate step key");
            }
        }

        if (!stepByKey.ContainsKey(
                startStep))
        {
            throw Invalid(
                "unknown start step");
        }

        var edges =
            new Dictionary<
                string,
                List<(string Outcome, string To)>>(
                    StringComparer.Ordinal);

        foreach (var key in stepByKey.Keys)
        {
            edges[key] = [];
        }

        var transitionKeys =
            new HashSet<string>(
                StringComparer.Ordinal);

        foreach (var item in transitions)
        {
            var transition =
                item as JsonObject
                ?? throw Invalid(
                    "transition");

            var from =
                RequiredString(
                    transition,
                    "from");

            var outcome =
                RequiredString(
                    transition,
                    "outcome");

            var to =
                RequiredString(
                    transition,
                    "to");

            if (!stepByKey.ContainsKey(from) ||
                !stepByKey.ContainsKey(to))
            {
                throw Invalid(
                    "transition references unknown step");
            }

            if (!transitionKeys.Add(
                    from + "\u001f" + outcome))
            {
                throw Invalid(
                    "duplicate transition outcome");
            }

            edges[from].Add(
                (outcome, to));
        }

        if (!stepObjects.Any(
                step =>
                    RequiredString(
                        step,
                        "type") == "end"))
        {
            throw Invalid(
                "flow has no end step");
        }

        EnsureAcyclic(
            startStep,
            edges);

        var reachable =
            Reachable(
                startStep,
                edges);

        if (reachable.Count !=
            stepByKey.Count)
        {
            throw Invalid(
                "unreachable step");
        }

        await using var connection =
            new NpgsqlConnection(
                ConnectionString());

        await connection.OpenAsync();

        foreach (var step in stepObjects)
        {
            var key =
                RequiredString(
                    step,
                    "key");

            var type =
                RequiredString(
                    step,
                    "type");

            HashSet<string> expectedOutcomes;

            switch (type)
            {
                case "automatic":
                {
                    var task =
                        step["task"]
                        as JsonObject
                        ?? throw Invalid(
                            "automatic task");

                    ValidateMapping(task);
                    ValidateRetry(task);

                    var module =
                        RequiredString(
                            task,
                            "module");

                    var action =
                        RequiredString(
                            task,
                            "action");

                    var actionVersion =
                        RequiredInt(
                            task,
                            "action_version");

                    await using var command =
                        new NpgsqlCommand(
                            """
                            SELECT
                                outcomes::text,
                                required_policy::text,
                                enabled
                            FROM catalog.action_definitions
                            WHERE module = @module
                              AND action = @action
                              AND version = @version;
                            """,
                            connection);

                    command.Parameters.AddWithValue(
                        "module",
                        module);

                    command.Parameters.AddWithValue(
                        "action",
                        action);

                    command.Parameters.AddWithValue(
                        "version",
                        actionVersion);

                    await using var reader =
                        await command.ExecuteReaderAsync();

                    if (!await reader.ReadAsync())
                    {
                        throw Invalid(
                            "unknown action version");
                    }

                    var actionOutcomes =
                        reader.GetString(0);

                    var actionPolicy =
                        reader.GetString(1);

                    var enabled =
                        reader.GetBoolean(2);

                    await reader.CloseAsync();

                    if (!enabled)
                    {
                        throw Invalid(
                            "disabled action version");
                    }

                    expectedOutcomes =
                        StringSet(
                            JsonNode.Parse(
                                actionOutcomes)
                            as JsonArray);

                    var expectedPolicy =
                        StringSet(
                            JsonNode.Parse(
                                actionPolicy)
                            as JsonArray);

                    var actualPolicy =
                        StringSet(
                            task["required_policy"]
                            as JsonArray);

                    if (!expectedPolicy.SetEquals(
                            actualPolicy))
                    {
                        throw Invalid(
                            "required policy mismatch");
                    }

                    break;
                }

                case "wait_signal":
                    expectedOutcomes =
                    [
                        RequiredString(
                            step,
                            "outcome")
                    ];
                    break;

                case "manual":
                    expectedOutcomes =
                        StringSet(
                            step["allowed_outcomes"]
                            as JsonArray);
                    break;

                case "end":
                    expectedOutcomes = [];
                    break;

                default:
                    throw Invalid(
                        "unsupported step type");
            }

            var actualOutcomes =
                edges[key]
                    .Select(
                        edge =>
                            edge.Outcome)
                    .ToHashSet(
                        StringComparer.Ordinal);

            if (!expectedOutcomes.SetEquals(
                    actualOutcomes))
            {
                throw Invalid(
                    "incomplete or unknown outcomes");
            }
        }
    }


    static void ValidateAgainstSchema(
        JsonObject map)
    {
        var schemaPath =
            ResolveSchemaPath();

        var schema =
            JsonSchema.FromText(
                File.ReadAllText(
                    schemaPath,
                    Encoding.UTF8));

        using var document =
            JsonDocument.Parse(
                map.ToJsonString());

        var result =
            schema.Evaluate(
                document.RootElement,
                new EvaluationOptions
                {
                    OutputFormat =
                        OutputFormat.Flag
                });

        if (!result.IsValid)
        {
            throw Invalid(
                "workflow map does not match schema");
        }
    }


    static void ValidateRetry(
        JsonObject task)
    {
        var retry =
            task["retry"]
            as JsonObject
            ?? throw Invalid(
                "retry");

        var maxAttempts =
            RequiredInt(
                retry,
                "max_attempts");

        var delays =
            retry["delays_ms"]
            as JsonArray
            ?? throw Invalid(
                "retry delays");

        if (maxAttempts < 1 ||
            maxAttempts > 10 ||
            delays.Count !=
                maxAttempts - 1)
        {
            throw Invalid(
                "invalid retry policy");
        }
    }


    static void ValidateMapping(
        JsonObject task)
    {
        var mapping =
            task["input_mapping"]
            as JsonObject
            ?? throw Invalid(
                "input_mapping");

        var constants =
            task["input_constants"]
            as JsonObject
            ?? throw Invalid(
                "input_constants");

        var targets =
            new List<string[]>();

        foreach (var item in mapping)
        {
            var target =
                ParsePointer(
                    item.Key);

            var sourceText =
                item.Value?
                    .GetValue<string>()
                ?? throw Invalid(
                    "mapping source");

            ParsePointer(
                sourceText);

            foreach (var previous in targets)
            {
                if (PointersOverlap(
                        previous,
                        target))
                {
                    throw Invalid(
                        "overlapping mapping targets");
                }
            }

            if (constants.Count > 0)
            {
                if (target.Length == 0 ||
                    constants.ContainsKey(
                        target[0]))
                {
                    throw Invalid(
                        "mapping overlaps constants");
                }
            }

            targets.Add(target);
        }
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
            throw Invalid(
                "invalid JSON Pointer");
        }

        return pointer
            .Split('/')[1..]
            .Select(
                segment =>
                    DecodePointerSegment(
                        segment))
            .ToArray();
    }


    static string DecodePointerSegment(
        string segment)
    {
        var output =
            new StringBuilder();

        for (var i = 0;
             i < segment.Length;
             i++)
        {
            if (segment[i] != '~')
            {
                output.Append(
                    segment[i]);

                continue;
            }

            if (i + 1 >=
                segment.Length)
            {
                throw Invalid(
                    "invalid JSON Pointer");
            }

            i++;

            output.Append(
                segment[i] switch
                {
                    '0' => '~',
                    '1' => '/',
                    _ => throw Invalid(
                        "invalid JSON Pointer")
                });
        }

        return output.ToString();
    }


    static bool PointersOverlap(
        string[] left,
        string[] right)
    {
        var common =
            Math.Min(
                left.Length,
                right.Length);

        for (var i = 0;
             i < common;
             i++)
        {
            if (!string.Equals(
                    left[i],
                    right[i],
                    StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }


    static void EnsureAcyclic(
        string start,
        Dictionary<
            string,
            List<(string Outcome, string To)>> edges)
    {
        var state =
            new Dictionary<string, int>(
                StringComparer.Ordinal);

        void Visit(string node)
        {
            state.TryGetValue(
                node,
                out var current);

            if (current == 1)
            {
                throw Invalid(
                    "workflow graph contains cycle");
            }

            if (current == 2)
            {
                return;
            }

            state[node] = 1;

            foreach (var edge in edges[node])
            {
                Visit(edge.To);
            }

            state[node] = 2;
        }

        Visit(start);
    }


    static HashSet<string> Reachable(
        string start,
        Dictionary<
            string,
            List<(string Outcome, string To)>> edges)
    {
        var result =
            new HashSet<string>(
                StringComparer.Ordinal);

        var stack =
            new Stack<string>();

        stack.Push(start);

        while (stack.Count > 0)
        {
            var current =
                stack.Pop();

            if (!result.Add(
                    current))
            {
                continue;
            }

            foreach (var edge in edges[current])
            {
                stack.Push(
                    edge.To);
            }
        }

        return result;
    }


    // ========================================================
    // JSON / YAML / canonicalization
    // ========================================================

    static async Task<JsonObject> LoadMapAsync(
        string path)
    {
        var text =
            await ReadTextAsync(path);

        try
        {
            return JsonNode.Parse(text)
                as JsonObject
                ?? throw Invalid(
                    "map root must be object");
        }
        catch (JsonException)
        {
            try
            {
                var deserializer =
                    new DeserializerBuilder()
                        .WithAttemptingUnquotedStringTypeDeserialization()
                        .Build();

                var yaml =
                    deserializer.Deserialize<object>(
                        text);

                return ToJsonNode(yaml)
                    as JsonObject
                    ?? throw Invalid(
                        "map root must be object");
            }
            catch (WorkflowMapInvalidException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new WorkflowMapInvalidException(
                    "invalid JSON/YAML workflow map",
                    ex);
            }
        }
    }


    static async Task<JsonNode> LoadJsonAsync(
        string path)
    {
        var text =
            await ReadTextAsync(path);

        return JsonNode.Parse(text)
            ?? throw new JsonException(
                "input is null");
    }


    static async Task<string> ReadTextAsync(
        string path)
    {
        if (path == "/dev/stdin")
        {
            return await Console.In
                .ReadToEndAsync();
        }

        return await File.ReadAllTextAsync(
            path,
            Encoding.UTF8);
    }


    static JsonNode? ToJsonNode(
        object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is IDictionary<object, object> map)
        {
            var result =
                new JsonObject();

            foreach (var pair in map)
            {
                if (pair.Key is not string key)
                {
                    throw Invalid(
                        "YAML object key must be string");
                }

                result[key] =
                    ToJsonNode(
                        pair.Value);
            }

            return result;
        }

        if (value is IEnumerable<object> sequence &&
            value is not string)
        {
            var result =
                new JsonArray();

            foreach (var item in sequence)
            {
                result.Add(
                    ToJsonNode(item));
            }

            return result;
        }

        return value switch
        {
            string text =>
                JsonValue.Create(text),

            bool boolean =>
                JsonValue.Create(boolean),

            sbyte integer =>
                JsonValue.Create(integer),

            byte integer =>
                JsonValue.Create(integer),

            short integer =>
                JsonValue.Create(integer),

            ushort integer =>
                JsonValue.Create(integer),

            int integer =>
                JsonValue.Create(integer),

            uint integer =>
                JsonValue.Create(integer),

            long integer =>
                JsonValue.Create(integer),

            ulong integer =>
                JsonValue.Create(integer),

            float number =>
                JsonValue.Create(number),

            double number =>
                JsonValue.Create(number),

            decimal number =>
                JsonValue.Create(number),

            DateTime dateTime =>
                JsonValue.Create(
                    dateTime.ToString("O")),

            _ =>
                JsonValue.Create(
                    value.ToString())
        };
    }


    static string CanonicalJson(
        JsonNode node)
    {
        var canonical =
            CanonicalNode(node);

        return canonical?
            .ToJsonString(
                new JsonSerializerOptions
                {
                    WriteIndented = false
                })
            ?? "null";
    }


    static JsonNode? CanonicalNode(
        JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node is JsonObject obj)
        {
            var result =
                new JsonObject();

            foreach (var pair in obj
                         .OrderBy(
                             pair =>
                                 pair.Key,
                             StringComparer.Ordinal))
            {
                result[pair.Key] =
                    CanonicalNode(
                        pair.Value);
            }

            return result;
        }

        if (node is JsonArray array)
        {
            var result =
                new JsonArray();

            foreach (var item in array)
            {
                result.Add(
                    CanonicalNode(item));
            }

            return result;
        }

        return node.DeepClone();
    }


    static string Sha256(
        string text)
    {
        var bytes =
            SHA256.HashData(
                Encoding.UTF8.GetBytes(
                    text));

        return Convert
            .ToHexString(bytes)
            .ToLowerInvariant();
    }


    // ========================================================
    // Small helpers
    // ========================================================

    static string RequiredString(
        JsonObject obj,
        string name)
    {
        try
        {
            var value =
                obj[name]?
                    .GetValue<string>();

            if (string.IsNullOrWhiteSpace(
                    value))
            {
                throw Invalid(name);
            }

            return value;
        }
        catch
        {
            throw Invalid(name);
        }
    }


    static int RequiredInt(
        JsonObject obj,
        string name)
    {
        var node =
            obj[name]
            ?? throw Invalid(name);

        try
        {
            using var document =
                JsonDocument.Parse(
                    node.ToJsonString());

            if (document.RootElement.ValueKind ==
                    JsonValueKind.Number
                && document.RootElement.TryGetInt32(
                    out var value))
            {
                return value;
            }
        }
        catch
        {
            // Converted below to the normal validation error.
        }

        throw Invalid(name);
    }


    static HashSet<string> StringSet(
        JsonArray? array)
    {
        if (array is null)
        {
            throw Invalid(
                "expected string array");
        }

        var result =
            new HashSet<string>(
                StringComparer.Ordinal);

        foreach (var item in array)
        {
            if (item is null)
            {
                throw Invalid(
                    "expected string array");
            }

            result.Add(
                item.GetValue<string>());
        }

        return result;
    }


    static string ConnectionString()
    {
        return Environment.GetEnvironmentVariable(
                   "COURSE_DB_CONNECTION")
               ?? throw new InvalidOperationException(
                   "COURSE_DB_CONNECTION is not configured.");
    }


    static string ResolveSchemaPath()
    {
        var candidates =
            new[]
            {
                "/app/contracts/course-1/workflow-map.schema.json",

                Path.Combine(
                    Directory.GetCurrentDirectory(),
                    "contracts",
                    "course-1",
                    "workflow-map.schema.json")
            };

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException(
            "workflow-map.schema.json was not found");
    }


    static WorkflowMapInvalidException Invalid(
        string reason)
    {
        return new WorkflowMapInvalidException(
            reason);
    }


    static int InvalidArguments()
    {
        return WriteError(
            "cli.invalid_arguments",
            "invalid workflow command arguments");
    }


    static int WriteOk(
        object result)
    {
        Console.WriteLine(
            JsonSerializer.Serialize(
                new
                {
                    status = "ok",
                    result,
                    meta = new
                    {
                        contractVersion =
                            "course-1"
                    }
                }));

        return 0;
    }


    static int WriteError(
        string code,
        string message)
    {
        Console.WriteLine(
            JsonSerializer.Serialize(
                new
                {
                    status = "error",
                    code,
                    message,
                    retryable = false,
                    details = new { },
                    meta = new
                    {
                        contractVersion =
                            "course-1"
                    }
                }));

        return 1;
    }


    sealed class WorkflowMapInvalidException
        : Exception
    {
        public WorkflowMapInvalidException(
            string message)
            : base(message)
        {
        }

        public WorkflowMapInvalidException(
            string message,
            Exception inner)
            : base(message, inner)
        {
        }
    }
}