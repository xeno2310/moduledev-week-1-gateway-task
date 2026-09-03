using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Json.Schema;
using NpgsqlTypes;

return await RunAsync(args);

static async Task<int> RunAsync(string[] args)
{
    try
    {
        if (args.Length >= 2 &&
            args[0] == "flow")
        {
            return await WorkflowCli.RunAsync(args);
        }
        if (args.Length == 3 &&
            args[0] == "migration" &&
            args[1] == "apply")
        {
            return await ApplyMigrationsAsync(args[2]);
        }
        if (args.Length == 3 &&
            args[0] == "action" &&
            args[1] == "validate")
        {
            return await ValidateManifestAsync(args[2]);
        }

        if (args.Length == 3 &&
            args[0] == "action" &&
            args[1] == "publish")
        {
            return await PublishManifestAsync(args[2]);
        }
        if (args.Length == 2 &&
            args[0] == "action" &&
            args[1] == "list")
        {
            return await ListActionsAsync();
        }
        if (args.Length == 5 &&
            args[0] == "action" &&
            args[1] == "activate" &&
            args[3] == "--version" &&
            int.TryParse(args[4], out var activateVersion))
        {
            return await ActivateActionAsync(
                args[2],
                activateVersion);
        }
        if (args.Length >= 5 &&
            args[0] == "action" &&
            args[1] == "disable" &&
            args[3] == "--version" &&
            int.TryParse(args[4], out var disableVersion))
        {
            int? replacementVersion = null;

            if (args.Length == 7 &&
                args[5] == "--replacement-version" &&
                int.TryParse(args[6], out var parsedReplacement))
            {
                replacementVersion = parsedReplacement;
            }
            else if (args.Length != 5)
            {
                return WriteError(
                    "cli.invalid_command",
                    "Unknown or invalid command.");
            }

            return await DisableActionAsync(
                args[2],
                disableVersion,
                replacementVersion);
        }
        return WriteError(
            "cli.invalid_command",
            "Unknown or invalid command.");
    }
    catch (MigrationConflictException)
    {
        return WriteError(
            "migration.conflict",
            "Previously applied migration has different checksum.");
    }
    catch (DirectoryNotFoundException)
    {
        return WriteError(
            "migration.directory_not_found",
            "Migration directory does not exist.");
    }
    catch (ManifestConflictException)
    {
        return WriteError(
            "manifest.conflict",
            "published action version is immutable");
    }
    catch (ManifestInvalidException)
    {
        return WriteError(
            "manifest.invalid",
            "manifest does not match contract schema");
    }
    catch (FileNotFoundException)
    {
        return WriteError(
            "manifest.not_found",
            "manifest file does not exist");
    }
    catch (ActionNotFoundException)
    {
        return WriteError(
            "action.not_found",
            "action version does not exist");
    }
    catch (ReplacementRequiredException)
    {
        return WriteError(
            "action.replacement_required",
            "replacement version is required");
    }
    catch (InvalidActionKeyException)
    {
        return WriteError(
            "action.invalid_key",
            "action key must have form module.action");
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine(ex.Message);

        return WriteError(
            "migration.failed",
            "Migration operation failed.");
    }
}

static async Task<int> ApplyMigrationsAsync(string directory)
{
    if (!Directory.Exists(directory))
    {
        throw new DirectoryNotFoundException(directory);
    }

    var connectionString =
        Environment.GetEnvironmentVariable(
            "COURSE_MIGRATION_DB_CONNECTION")
        ?? Environment.GetEnvironmentVariable(
            "COURSE_DB_CONNECTION");

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        throw new InvalidOperationException(
            "COURSE_MIGRATION_DB_CONNECTION is not configured.");
    }

    var files = Directory
        .EnumerateFiles(directory, "*.sql", SearchOption.TopDirectoryOnly)
        .OrderBy(
            file => Path.GetFileName(file),
            StringComparer.Ordinal)
        .ToArray();

    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await EnsureMigrationTableAsync(connection);

    // Не даём двум CLI одновременно применять один набор миграций.
    await using var lockCommand = new NpgsqlCommand(
        "SELECT pg_advisory_lock(726001);",
        connection);

    await lockCommand.ExecuteNonQueryAsync();

    var applied = 0;
    var skipped = 0;

    try
    {
        foreach (var file in files)
        {
            var fileName = Path.GetFileName(file);

            var bytes = await File.ReadAllBytesAsync(file);

            var checksum = Convert
                .ToHexString(SHA256.HashData(bytes))
                .ToLowerInvariant();

            var existingChecksum =
                await GetExistingChecksumAsync(
                    connection,
                    fileName);

            if (existingChecksum is not null)
            {
                if (!string.Equals(
                        existingChecksum,
                        checksum,
                        StringComparison.Ordinal))
                {
                    throw new MigrationConflictException();
                }

                skipped++;
                continue;
            }

            var sql = Encoding.UTF8.GetString(bytes);

            // Убираем UTF-8 BOM, если он присутствует.
            if (sql.Length > 0 && sql[0] == '\uFEFF')
            {
                sql = sql[1..];
            }

            await using var transaction =
                await connection.BeginTransactionAsync();

            try
            {
                // Всё, что создаёт migration, будет принадлежать
                // NOLOGIN-роли course_owner.
                await using (var setRole = new NpgsqlCommand(
                    "SET LOCAL ROLE course_owner;",
                    connection,
                    transaction))
                {
                    await setRole.ExecuteNonQueryAsync();
                }

                await using (var migrationCommand =
                    new NpgsqlCommand(
                        sql,
                        connection,
                        transaction))
                {
                    migrationCommand.CommandTimeout = 120;

                    await migrationCommand.ExecuteNonQueryAsync();
                }

                await using (var resetRole = new NpgsqlCommand(
                    "RESET ROLE;",
                    connection,
                    transaction))
                {
                    await resetRole.ExecuteNonQueryAsync();
                }

                await using (var historyCommand =
                    new NpgsqlCommand(
                        """
                        INSERT INTO public.course_schema_migrations
                            (file_name, checksum)
                        VALUES
                            (@file_name, @checksum);
                        """,
                        connection,
                        transaction))
                {
                    historyCommand.Parameters.AddWithValue(
                        "file_name",
                        fileName);

                    historyCommand.Parameters.AddWithValue(
                        "checksum",
                        checksum);

                    await historyCommand.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();

                applied++;
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
    }
    finally
    {
        await using var unlockCommand = new NpgsqlCommand(
            "SELECT pg_advisory_unlock(726001);",
            connection);

        await unlockCommand.ExecuteNonQueryAsync();
    }

    return WriteOk(new
    {
        resource = "migration",
        operation = "applied",
        applied,
        skipped
    });
}

static async Task EnsureMigrationTableAsync(
    NpgsqlConnection connection)
{
    const string sql =
        """
        CREATE TABLE IF NOT EXISTS public.course_schema_migrations
        (
            file_name   text        PRIMARY KEY,
            checksum    text        NOT NULL,
            applied_at  timestamptz NOT NULL DEFAULT now(),

            CHECK (checksum ~ '^[0-9a-f]{64}$')
        );

        REVOKE ALL
        ON public.course_schema_migrations
        FROM PUBLIC;
        """;

    await using var command =
        new NpgsqlCommand(sql, connection);

    await command.ExecuteNonQueryAsync();
}

static async Task<string?> GetExistingChecksumAsync(
    NpgsqlConnection connection,
    string fileName)
{
    const string sql =
        """
        SELECT checksum
        FROM public.course_schema_migrations
        WHERE file_name = @file_name;
        """;

    await using var command =
        new NpgsqlCommand(sql, connection);

    command.Parameters.AddWithValue(
        "file_name",
        fileName);

    var result = await command.ExecuteScalarAsync();

    return result as string;
}
static async Task<int> ValidateManifestAsync(string path)
{
    var manifest = await ReadManifestAsync(path);

    return WriteOk(new
    {
        resource = "action",
        operation = "validated",
        key = $"{manifest.Module}.{manifest.Action}",
        version = manifest.Version
    });
}

static async Task<int> PublishManifestAsync(string path)
{
    var manifest = await ReadManifestAsync(path);

    var connectionString =
        Environment.GetEnvironmentVariable("COURSE_DB_CONNECTION");

    if (string.IsNullOrWhiteSpace(connectionString))
    {
        throw new InvalidOperationException(
            "COURSE_DB_CONNECTION is not configured.");
    }

    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await using var transaction =
        await connection.BeginTransactionAsync();

    const string existingSql =
        """
        SELECT manifest_hash
        FROM catalog.action_definitions
        WHERE module = @module
          AND action = @action
          AND version = @version;
        """;

    await using (var existingCommand =
        new NpgsqlCommand(
            existingSql,
            connection,
            transaction))
    {
        existingCommand.Parameters.AddWithValue(
            "module",
            manifest.Module);

        existingCommand.Parameters.AddWithValue(
            "action",
            manifest.Action);

        existingCommand.Parameters.AddWithValue(
            "version",
            manifest.Version);

        var existingHash =
            await existingCommand.ExecuteScalarAsync();

        if (existingHash is string hash)
        {
            if (!string.Equals(
                    hash,
                    manifest.Hash,
                    StringComparison.Ordinal))
            {
                throw new ManifestConflictException();
            }

            await transaction.CommitAsync();

            return WriteOk(new
            {
                resource = "action",
                operation = "published",
                key = $"{manifest.Module}.{manifest.Action}",
                version = manifest.Version
            });
        }
    }

    const string insertSql =
        """
        INSERT INTO catalog.action_definitions
        (
            module,
            action,
            version,
            contract_version,
            http_method,
            target_schema,
            target_function,
            request_schema,
            response_schema,
            outcomes,
            required_policy,
            idempotency_mode,
            idempotency_scope,
            timeout_ms,
            enabled,
            is_default,
            manifest_hash
        )
        VALUES
        (
            @module,
            @action,
            @version,
            @contract_version,
            @http_method,
            @target_schema,
            @target_function,
            @request_schema,
            @response_schema,
            @outcomes,
            @required_policy,
            @idempotency_mode,
            @idempotency_scope,
            @timeout_ms,
            @enabled,
            @is_default,
            @manifest_hash
        );
        """;

    await using (var command =
        new NpgsqlCommand(
            insertSql,
            connection,
            transaction))
    {
        command.Parameters.AddWithValue(
            "module",
            manifest.Module);

        command.Parameters.AddWithValue(
            "action",
            manifest.Action);

        command.Parameters.AddWithValue(
            "version",
            manifest.Version);

        command.Parameters.AddWithValue(
            "contract_version",
            manifest.ContractVersion);

        command.Parameters.AddWithValue(
            "http_method",
            manifest.HttpMethod);

        command.Parameters.AddWithValue(
            "target_schema",
            manifest.TargetSchema);

        command.Parameters.AddWithValue(
            "target_function",
            manifest.TargetFunction);

        command.Parameters
            .Add("request_schema", NpgsqlDbType.Jsonb)
            .Value = manifest.RequestSchema;

        command.Parameters
            .Add("response_schema", NpgsqlDbType.Jsonb)
            .Value = manifest.ResponseSchema;

        command.Parameters
            .Add("outcomes", NpgsqlDbType.Jsonb)
            .Value = manifest.Outcomes;

        command.Parameters
            .Add("required_policy", NpgsqlDbType.Jsonb)
            .Value = manifest.RequiredPolicy;

        command.Parameters.AddWithValue(
            "idempotency_mode",
            manifest.IdempotencyMode);

        command.Parameters.AddWithValue(
            "idempotency_scope",
            manifest.IdempotencyScope);

        command.Parameters.AddWithValue(
            "timeout_ms",
            manifest.TimeoutMs);

        command.Parameters.AddWithValue(
            "enabled",
            manifest.Enabled);

        command.Parameters.AddWithValue(
            "is_default",
            manifest.IsDefault);

        command.Parameters.AddWithValue(
            "manifest_hash",
            manifest.Hash);

        await command.ExecuteNonQueryAsync();
    }

    await transaction.CommitAsync();

    return WriteOk(new
    {
        resource = "action",
        operation = "published",
        key = $"{manifest.Module}.{manifest.Action}",
        version = manifest.Version
    });
}

static async Task<ActionManifest> ReadManifestAsync(
    string path)
{
    if (!File.Exists(path))
    {
        throw new FileNotFoundException(path);
    }

    string json;

    try
    {
        json = await File.ReadAllTextAsync(path);
    }
    catch
    {
        throw new ManifestInvalidException();
    }

    using JsonDocument document = ParseJson(json);

    var schemaPath = Path.Combine(
        AppContext.BaseDirectory,
        "contracts",
        "course-1",
        "action-manifest.schema.json");

    if (!File.Exists(schemaPath))
    {
        throw new InvalidOperationException(
            "Action manifest schema is unavailable.");
    }

    try
    {
        var schema = JsonSchema.FromFile(schemaPath);

        var result =
            schema.Evaluate(document.RootElement);

        if (!result.IsValid)
        {
            throw new ManifestInvalidException();
        }
    }
    catch (ManifestInvalidException)
    {
        throw;
    }
    catch
    {
        throw new ManifestInvalidException();
    }

    var root = document.RootElement;

    var canonical = Canonicalize(root);

    var hash = Convert
        .ToHexString(
            SHA256.HashData(canonical))
        .ToLowerInvariant();

    return new ActionManifest(
        root.GetProperty("contract_version").GetString()!,
        root.GetProperty("module").GetString()!,
        root.GetProperty("action").GetString()!,
        root.GetProperty("version").GetInt32(),
        root.GetProperty("http_method").GetString()!,
        root.GetProperty("target_schema").GetString()!,
        root.GetProperty("target_function").GetString()!,
        root.GetProperty("request_schema").GetRawText(),
        root.GetProperty("response_schema").GetRawText(),
        root.GetProperty("outcomes").GetRawText(),
        root.GetProperty("required_policy").GetRawText(),
        root.GetProperty("idempotency_mode").GetString()!,
        root.GetProperty("idempotency_scope").GetString()!,
        root.GetProperty("timeout_ms").GetInt32(),
        root.GetProperty("enabled").GetBoolean(),
        root.GetProperty("is_default").GetBoolean(),
        hash
    );
}

static JsonDocument ParseJson(string json)
{
    try
    {
        return JsonDocument.Parse(json);
    }
    catch (JsonException)
    {
        throw new ManifestInvalidException();
    }
}

static byte[] Canonicalize(JsonElement element)
{
    using var stream = new MemoryStream();

    using (var writer =
        new Utf8JsonWriter(
            stream,
            new JsonWriterOptions
            {
                Indented = false
            }))
    {
        WriteCanonical(writer, element);
    }

    return stream.ToArray();
}

static void WriteCanonical(
    Utf8JsonWriter writer,
    JsonElement element)
{
    switch (element.ValueKind)
    {
        case JsonValueKind.Object:
            writer.WriteStartObject();

            foreach (var property in
                element
                    .EnumerateObject()
                    .OrderBy(
                        x => x.Name,
                        StringComparer.Ordinal))
            {
                writer.WritePropertyName(
                    property.Name);

                WriteCanonical(
                    writer,
                    property.Value);
            }

            writer.WriteEndObject();
            break;

        case JsonValueKind.Array:
            writer.WriteStartArray();

            foreach (var item in
                element.EnumerateArray())
            {
                WriteCanonical(
                    writer,
                    item);
            }

            writer.WriteEndArray();
            break;

        case JsonValueKind.String:
            writer.WriteStringValue(
                element.GetString());
            break;

        case JsonValueKind.Number:
            writer.WriteRawValue(
                element.GetRawText(),
                skipInputValidation: true);
            break;

        case JsonValueKind.True:
            writer.WriteBooleanValue(true);
            break;

        case JsonValueKind.False:
            writer.WriteBooleanValue(false);
            break;

        case JsonValueKind.Null:
            writer.WriteNullValue();
            break;

        default:
            throw new ManifestInvalidException();
    }
}
static async Task<int> ListActionsAsync()
{
    var connectionString =
        GetConnectionString();

    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    const string sql =
        """
        SELECT
            module,
            action,
            version,
            enabled,
            is_default
        FROM catalog.action_definitions
        ORDER BY module, action, version;
        """;

    await using var command =
        new NpgsqlCommand(sql, connection);

    await using var reader =
        await command.ExecuteReaderAsync();

    var items = new List<object>();

    while (await reader.ReadAsync())
    {
        items.Add(new
        {
            module = reader.GetString(0),
            action = reader.GetString(1),
            version = reader.GetInt32(2),
            enabled = reader.GetBoolean(3),
            isDefault = reader.GetBoolean(4)
        });
    }

    return WriteOk(new
    {
        resource = "action",
        operation = "listed",
        items
    });
}
static async Task<int> ActivateActionAsync(
    string key,
    int version)
{
    var (module, action) = ParseActionKey(key);

    var connectionString =
        GetConnectionString();

    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await using var transaction =
        await connection.BeginTransactionAsync();

    const string existsSql =
        """
        SELECT EXISTS (
            SELECT 1
            FROM catalog.action_definitions
            WHERE module = @module
              AND action = @action
              AND version = @version
        );
        """;

    await using (var exists =
        new NpgsqlCommand(
            existsSql,
            connection,
            transaction))
    {
        exists.Parameters.AddWithValue("module", module);
        exists.Parameters.AddWithValue("action", action);
        exists.Parameters.AddWithValue("version", version);

        var found =
            (bool)(await exists.ExecuteScalarAsync())!;

        if (!found)
        {
            throw new ActionNotFoundException();
        }
    }

    const string clearDefaultSql =
        """
        UPDATE catalog.action_definitions
        SET is_default = false
        WHERE module = @module
          AND action = @action
          AND is_default = true;
        """;

    await using (var clear =
        new NpgsqlCommand(
            clearDefaultSql,
            connection,
            transaction))
    {
        clear.Parameters.AddWithValue("module", module);
        clear.Parameters.AddWithValue("action", action);

        await clear.ExecuteNonQueryAsync();
    }

    const string activateSql =
        """
        UPDATE catalog.action_definitions
        SET
            enabled = true,
            is_default = true
        WHERE module = @module
          AND action = @action
          AND version = @version;
        """;

    await using (var activate =
        new NpgsqlCommand(
            activateSql,
            connection,
            transaction))
    {
        activate.Parameters.AddWithValue("module", module);
        activate.Parameters.AddWithValue("action", action);
        activate.Parameters.AddWithValue("version", version);

        await activate.ExecuteNonQueryAsync();
    }

    await transaction.CommitAsync();

    return WriteOk(new
    {
        resource = "action",
        operation = "activated",
        key,
        version
    });
}
static async Task<int> DisableActionAsync(
    string key,
    int version,
    int? replacementVersion)
{
    var (module, action) = ParseActionKey(key);

    var connectionString =
        GetConnectionString();

    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await using var transaction =
        await connection.BeginTransactionAsync();

    const string targetSql =
        """
        SELECT enabled, is_default
        FROM catalog.action_definitions
        WHERE module = @module
          AND action = @action
          AND version = @version
        FOR UPDATE;
        """;

    bool isDefault;

    await using (var target =
        new NpgsqlCommand(
            targetSql,
            connection,
            transaction))
    {
        target.Parameters.AddWithValue("module", module);
        target.Parameters.AddWithValue("action", action);
        target.Parameters.AddWithValue("version", version);

        await using var reader =
            await target.ExecuteReaderAsync();

        if (!await reader.ReadAsync())
        {
            throw new ActionNotFoundException();
        }

        isDefault = reader.GetBoolean(1);
    }

    if (isDefault && replacementVersion is null)
    {
        throw new ReplacementRequiredException();
    }

    if (replacementVersion is not null)
    {
        if (replacementVersion == version)
        {
            throw new ReplacementRequiredException();
        }

        const string replacementSql =
            """
            SELECT EXISTS (
                SELECT 1
                FROM catalog.action_definitions
                WHERE module = @module
                  AND action = @action
                  AND version = @replacement
            );
            """;

        await using var replacement =
            new NpgsqlCommand(
                replacementSql,
                connection,
                transaction);

        replacement.Parameters.AddWithValue(
            "module",
            module);

        replacement.Parameters.AddWithValue(
            "action",
            action);

        replacement.Parameters.AddWithValue(
            "replacement",
            replacementVersion.Value);

        var found =
            (bool)(await replacement.ExecuteScalarAsync())!;

        if (!found)
        {
            throw new ActionNotFoundException();
        }
    }

    const string disableSql =
        """
        UPDATE catalog.action_definitions
        SET
            enabled = false,
            is_default = false
        WHERE module = @module
          AND action = @action
          AND version = @version;
        """;

    await using (var disable =
        new NpgsqlCommand(
            disableSql,
            connection,
            transaction))
    {
        disable.Parameters.AddWithValue("module", module);
        disable.Parameters.AddWithValue("action", action);
        disable.Parameters.AddWithValue("version", version);

        await disable.ExecuteNonQueryAsync();
    }

    if (replacementVersion is not null)
    {
        const string clearSql =
            """
            UPDATE catalog.action_definitions
            SET is_default = false
            WHERE module = @module
              AND action = @action;
            """;

        await using (var clear =
            new NpgsqlCommand(
                clearSql,
                connection,
                transaction))
        {
            clear.Parameters.AddWithValue("module", module);
            clear.Parameters.AddWithValue("action", action);

            await clear.ExecuteNonQueryAsync();
        }

        const string replacementSql =
            """
            UPDATE catalog.action_definitions
            SET
                enabled = true,
                is_default = true
            WHERE module = @module
              AND action = @action
              AND version = @replacement;
            """;

        await using (var replacement =
            new NpgsqlCommand(
                replacementSql,
                connection,
                transaction))
        {
            replacement.Parameters.AddWithValue("module", module);
            replacement.Parameters.AddWithValue("action", action);
            replacement.Parameters.AddWithValue(
                "replacement",
                replacementVersion.Value);

            await replacement.ExecuteNonQueryAsync();
        }
    }

    await transaction.CommitAsync();

    return WriteOk(new
    {
        resource = "action",
        operation = "disabled",
        key,
        version,
        replacementVersion
    });
}
static string GetConnectionString()
{
    return Environment.GetEnvironmentVariable(
        "COURSE_DB_CONNECTION")
        ?? throw new InvalidOperationException(
            "COURSE_DB_CONNECTION is not configured.");
}

static (string Module, string Action)
    ParseActionKey(string key)
{
    var parts = key.Split(
        '.',
        StringSplitOptions.RemoveEmptyEntries);

    if (parts.Length != 2)
    {
        throw new InvalidActionKeyException();
    }

    return (parts[0], parts[1]);
}

static int WriteOk(object result)
{
    Console.WriteLine(
        JsonSerializer.Serialize(new
        {
            status = "ok",
            result,
            meta = new
            {
                contractVersion = "course-1"
            }
        }));

    return 0;
}

static int WriteError(
    string code,
    string message)
{
    Console.WriteLine(
        JsonSerializer.Serialize(new
        {
            status = "error",
            code,
            message,
            meta = new
            {
                contractVersion = "course-1"
            }
        }));

    return 1;
}

sealed class ActionNotFoundException : Exception;

sealed class ReplacementRequiredException : Exception;

sealed class InvalidActionKeyException : Exception;
sealed class MigrationConflictException : Exception;
sealed class ManifestConflictException : Exception;

sealed class ManifestInvalidException : Exception;

sealed record ActionManifest(
    string ContractVersion,
    string Module,
    string Action,
    int Version,
    string HttpMethod,
    string TargetSchema,
    string TargetFunction,
    string RequestSchema,
    string ResponseSchema,
    string Outcomes,
    string RequiredPolicy,
    string IdempotencyMode,
    string IdempotencyScope,
    int TimeoutMs,
    bool Enabled,
    bool IsDefault,
    string Hash
);