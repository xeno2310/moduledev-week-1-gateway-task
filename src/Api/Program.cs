using System.Text;
using System.Text.Json;
using Json.Schema;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

var connectionString =
    builder.Configuration["COURSE_DB_CONNECTION"]
    ?? throw new InvalidOperationException(
        "COURSE_DB_CONNECTION is not configured.");

var jwtIssuer =
    builder.Configuration["COURSE_JWT_ISSUER"]
    ?? throw new InvalidOperationException(
        "COURSE_JWT_ISSUER is not configured.");

var jwtAudience =
    builder.Configuration["COURSE_JWT_AUDIENCE"]
    ?? throw new InvalidOperationException(
        "COURSE_JWT_AUDIENCE is not configured.");

var jwtSigningKey =
    builder.Configuration["COURSE_JWT_SIGNING_KEY"]
    ?? throw new InvalidOperationException(
        "COURSE_JWT_SIGNING_KEY is not configured.");

builder.Services
    .AddAuthentication(
        JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.MapInboundClaims = false;

        options.TokenValidationParameters =
            new TokenValidationParameters
            {
                ValidateIssuer = true,
                ValidIssuer = jwtIssuer,

                ValidateAudience = true,
                ValidAudience = jwtAudience,

                ValidateIssuerSigningKey = true,
                IssuerSigningKey =
                    new SymmetricSecurityKey(
                        Encoding.UTF8.GetBytes(
                            jwtSigningKey)),

                ValidateLifetime = true,
                RequireExpirationTime = true,

                ClockSkew = TimeSpan.Zero,

                ValidAlgorithms =
                    new[]
                    {
                        SecurityAlgorithms.HmacSha256
                    }
            };

        options.Events =
            new JwtBearerEvents
            {
                OnChallenge = async context =>
                {
                    context.HandleResponse();

                    if (!context.Response.HasStarted)
                    {
                        await WriteErrorAsync(
                            context.HttpContext,
                            StatusCodes.Status401Unauthorized,
                            "auth.invalid",
                            "authentication token is invalid",
                            null,
                            null);
                    }
                }
            };
    });

builder.Services.AddAuthorization();

var app = builder.Build();


app.MapGet("/health/live", () =>
{
    return Results.Ok(new
    {
        status = "ok"
    });
});


app.MapGet("/health/ready", async () =>
{
    try
    {
        await using var connection =
            new NpgsqlConnection(connectionString);

        await connection.OpenAsync();

        await using var command =
            new NpgsqlCommand(
                "SELECT 1",
                connection);

        await command.ExecuteScalarAsync();

        return Results.Ok(new
        {
            status = "ready"
        });
    }
    catch
    {
        return Results.StatusCode(
            StatusCodes.Status503ServiceUnavailable);
    }
});


app.UseAuthentication();
app.UseAuthorization();


app.MapPost(
        "/api/{module}/{action}",
        async (
            HttpContext http,
            string module,
            string action) =>
        {
            return await ExecuteActionAsync(
                http,
                module,
                action,
                connectionString);
        })
    .RequireAuthorization();

app.MapGet(
    "/openapi/default.json",
    async () =>
    {
        try
        {
            var definitions =
                await LoadOpenApiDefaultsAsync(
                    connectionString);

            return Results.Json(
                BuildOpenApiDocument(definitions));
        }
        catch (NpgsqlException)
        {
            return Error(
                503,
                "dependency.unavailable",
                "database dependency is unavailable",
                null,
                null);
        }
    });


app.MapGet(
    "/openapi/actions/{module}/{action}/{version:int}.json",
    async (
        string module,
        string action,
        int version) =>
    {
        try
        {
            var definition =
                await LoadOpenApiActionAsync(
                    connectionString,
                    module,
                    action,
                    version);

            if (definition is null)
            {
                return Error(
                    404,
                    "action.not_found",
                    "action or version not found",
                    null,
                    null);
            }

            return Results.Json(
                BuildOpenApiDocument(
                    new[] { definition }));
        }
        catch (NpgsqlException)
        {
            return Error(
                503,
                "dependency.unavailable",
                "database dependency is unavailable",
                null,
                null);
        }
    });

app.Run();


static async Task<IResult> ExecuteActionAsync(
    HttpContext http,
    string module,
    string action,
    string connectionString)
{
    // Authentication middleware проверил подпись,
    // issuer, audience и lifetime.
    // Здесь дополнительно проверяем JSON-типы claims.
    if (!TryReadTrustedIdentity(
            http,
            out var identity))
    {
        return Error(
            401,
            "auth.invalid",
            "authentication token is invalid",
            null,
            null);
    }

    var correlationId =
        Guid.NewGuid().ToString();

    if (!TryReadVersion(
            http,
            out var requestedVersion))
    {
        return Error(
            400,
            "request.invalid",
            "action version header is invalid",
            correlationId,
            null);
    }

    ActionDefinition? definition;

    try
    {
        definition =
            await ResolveActionAsync(
                connectionString,
                module,
                action,
                requestedVersion,
                http.RequestAborted);
    }
    catch (NpgsqlException)
    {
        return Error(
            503,
            "dependency.unavailable",
            "database dependency is unavailable",
            correlationId,
            null);
    }

    if (definition is null)
    {
        return Error(
            404,
            "action.not_found",
            "action or version not found",
            correlationId,
            null);
    }

    // HTTP-boundary policy check.
    foreach (var requiredScope
             in definition.RequiredPolicy)
    {
        if (!identity.Scopes.Contains(
                requiredScope))
        {
            return Error(
                403,
                "access.denied",
                "required policy is not satisfied",
                correlationId,
                definition.Version);
        }
    }

    if (!TryReadIdempotencyKey(
            http,
            out var requestId))
    {
        return Error(
            400,
            "request.invalid",
            "idempotency header is invalid",
            correlationId,
            definition.Version);
    }

    if (definition.IdempotencyMode ==
            "required"
        && string.IsNullOrWhiteSpace(
            requestId))
    {
        return Error(
            400,
            "idempotency.required",
            "idempotency key is required",
            correlationId,
            definition.Version);
    }

    JsonDocument payload;

    try
    {
        payload =
            await JsonDocument.ParseAsync(
                http.Request.Body,
                cancellationToken:
                    http.RequestAborted);
    }
    catch (JsonException)
    {
        return Error(
            400,
            "request.invalid",
            "request body is not valid JSON",
            correlationId,
            definition.Version);
    }

    using (payload)
    {
        try
        {
            var requestSchema =
                SchemaCache.Get(
                    definition.RequestSchema);

            var evaluation =
                requestSchema.Evaluate(
                    payload.RootElement);

            if (!evaluation.IsValid)
            {
                return Error(
                    422,
                    "payload.invalid",
                    "payload does not match schema",
                    correlationId,
                    definition.Version);
            }
        }
        catch
        {
            return Error(
                500,
                "action.contract_violation",
                "published request schema is invalid",
                correlationId,
                definition.Version);
        }

        var deadline =
            DateTimeOffset.UtcNow
                .AddMilliseconds(
                    definition.TimeoutMs)
                .ToString("O");

        var trustedContext =
            JsonSerializer.Serialize(
                new
                {
                    principal =
                        identity.Principal,

                    consumer =
                        identity.Consumer,

                    scopes =
                        identity.Scopes
                            .OrderBy(x => x)
                            .ToArray(),

                    correlationId,

                    requestId,

                    deadline
                });

        try
        {
            await using var connection =
                new NpgsqlConnection(
                    connectionString);

            await connection.OpenAsync(
                http.RequestAborted);

            await using var transaction =
                await connection
                    .BeginTransactionAsync(
                        http.RequestAborted);

            try
            {
                await using (
                    var timeoutCommand =
                        new NpgsqlCommand(
                            """
                            SELECT set_config(
                                'statement_timeout',
                                @timeout,
                                true
                            );
                            """,
                            connection,
                            transaction))
                {
                    timeoutCommand.Parameters
                        .AddWithValue(
                            "timeout",
                            $"{definition.TimeoutMs}ms");

                    await timeoutCommand
                        .ExecuteNonQueryAsync(
                            http.RequestAborted);
                }

                await using var invoke =
                    new NpgsqlCommand(
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

                invoke.Parameters.AddWithValue(
                    "module",
                    module);

                invoke.Parameters.AddWithValue(
                    "action",
                    action);

                invoke.Parameters.AddWithValue(
                    "version",
                    definition.Version);

                invoke.Parameters
                    .Add(
                        "context",
                        NpgsqlDbType.Jsonb)
                    .Value = trustedContext;

                invoke.Parameters
                    .Add(
                        "payload",
                        NpgsqlDbType.Jsonb)
                    .Value =
                    payload.RootElement
                        .GetRawText();

                var scalar =
                    await invoke.ExecuteScalarAsync(
                        http.RequestAborted);

                if (scalar is not string
                    responseJson)
                {
                    await transaction
                        .RollbackAsync();

                    return Error(
                        500,
                        "action.contract_violation",
                        "action returned invalid envelope",
                        correlationId,
                        definition.Version);
                }

                using var responseDocument =
                    JsonDocument.Parse(
                        responseJson);

                var root =
                    responseDocument.RootElement;

                if (!root.TryGetProperty(
                        "status",
                        out var statusElement)
                    || statusElement.ValueKind
                        != JsonValueKind.String)
                {
                    await transaction
                        .RollbackAsync();

                    return Error(
                        500,
                        "action.contract_violation",
                        "action returned invalid envelope",
                        correlationId,
                        definition.Version);
                }

                var status =
                    statusElement.GetString();

                // Controlled error from api.invoke/target:
                // rollback all target changes first.
                if (status == "error")
                {
                    var errorCopy =
                        root.Clone();

                    await transaction
                        .RollbackAsync();

                    return ErrorFromAction(
                        errorCopy,
                        correlationId,
                        definition.Version);
                }

                if (status != "ok"
                    || !root.TryGetProperty(
                        "outcome",
                        out var outcomeElement)
                    || outcomeElement.ValueKind
                        != JsonValueKind.String
                    || !root.TryGetProperty(
                        "result",
                        out var resultElement))
                {
                    await transaction
                        .RollbackAsync();

                    return Error(
                        500,
                        "action.contract_violation",
                        "action returned invalid envelope",
                        correlationId,
                        definition.Version);
                }

                var outcome =
                    outcomeElement.GetString()!;

                if (!definition.Outcomes.Contains(
                        outcome))
                {
                    await transaction
                        .RollbackAsync();

                    return Error(
                        500,
                        "action.contract_violation",
                        "action returned undeclared outcome",
                        correlationId,
                        definition.Version);
                }

                bool validResult;

                try
                {
                    var responseSchema =
                        SchemaCache.Get(
                            definition.ResponseSchema);

                    validResult =
                        responseSchema
                            .Evaluate(
                                resultElement)
                            .IsValid;
                }
                catch
                {
                    validResult = false;
                }

                if (!validResult)
                {
                    await transaction
                        .RollbackAsync();

                    return Error(
                        500,
                        "action.contract_violation",
                        "action result does not match schema",
                        correlationId,
                        definition.Version);
                }

                var resultCopy =
                    resultElement.Clone();

                await transaction
                    .CommitAsync(
                        http.RequestAborted);

                // Meta формируем сами.
                // Target не может подменить correlationId/version.
                return Results.Json(
                    new
                    {
                        status = "ok",
                        outcome,
                        result = resultCopy,
                        meta = new
                        {
                            correlationId,
                            actionVersion =
                                definition.Version
                        }
                    },
                    statusCode: 200);
            }
            catch
            {
                try
                {
                    await transaction.RollbackAsync();
                }
                catch
                {
                }

                throw;
            }
        }
        catch (PostgresException ex)
            when (ex.SqlState == "57014")
        {
            return Error(
                504,
                "action.timeout",
                "action execution timed out",
                correlationId,
                definition.Version);
        }
        catch (NpgsqlException)
        {
            return Error(
                503,
                "dependency.unavailable",
                "database dependency is unavailable",
                correlationId,
                definition.Version);
        }
        catch (OperationCanceledException)
            when (!http.RequestAborted
                .IsCancellationRequested)
        {
            return Error(
                504,
                "action.timeout",
                "action execution timed out",
                correlationId,
                definition.Version);
        }
        catch
        {
            return Error(
                500,
                "internal.error",
                "internal server error",
                correlationId,
                definition.Version);
        }
    }
}


static async Task<ActionDefinition?>
    ResolveActionAsync(
        string connectionString,
        string module,
        string action,
        int? version,
        CancellationToken cancellationToken)
{
    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync(
        cancellationToken);

    await using var command =
        new NpgsqlCommand(
            """
            SELECT api.resolve_action(
                @module,
                @action,
                @version
            )::text;
            """,
            connection);

    command.Parameters.AddWithValue(
        "module",
        module);

    command.Parameters.AddWithValue(
        "action",
        action);

    var versionParameter =
        command.Parameters.Add(
            "version",
            NpgsqlDbType.Integer);

    versionParameter.Value =
        version.HasValue
            ? version.Value
            : DBNull.Value;

    var scalar =
        await command.ExecuteScalarAsync(
            cancellationToken);

    if (scalar is not string json
        || string.IsNullOrWhiteSpace(json))
    {
        return null;
    }

    using var document =
        JsonDocument.Parse(json);

    var root =
        document.RootElement;

    return new ActionDefinition(
        root.GetProperty("version")
            .GetInt32(),

        root.GetProperty("request_schema")
            .GetRawText(),

        root.GetProperty("response_schema")
            .GetRawText(),

        root.GetProperty("outcomes")
            .EnumerateArray()
            .Select(x => x.GetString()!)
            .ToHashSet(
                StringComparer.Ordinal),

        root.GetProperty("required_policy")
            .EnumerateArray()
            .Select(x => x.GetString()!)
            .ToArray(),

        root.GetProperty("idempotency_mode")
            .GetString()!,

        root.GetProperty("timeout_ms")
            .GetInt32()
    );
}


static bool TryReadVersion(
    HttpContext http,
    out int? version)
{
    version = null;

    if (!http.Request.Headers.TryGetValue(
            "X-Action-Version",
            out var values))
    {
        return true;
    }

    if (values.Count != 1)
    {
        return false;
    }

    var raw =
        values[0];

    if (string.IsNullOrWhiteSpace(raw)
        || !int.TryParse(raw, out var parsed)
        || parsed < 1)
    {
        return false;
    }

    version = parsed;

    return true;
}


static bool TryReadIdempotencyKey(
    HttpContext http,
    out string? key)
{
    key = null;

    if (!http.Request.Headers.TryGetValue(
            "Idempotency-Key",
            out var values))
    {
        return true;
    }

    if (values.Count != 1)
    {
        return false;
    }

    key = values[0];

    if (key is { Length: > 128 })
    {
        return false;
    }

    return true;
}


static bool TryReadTrustedIdentity(
    HttpContext http,
    out TrustedIdentity identity)
{
    identity =
        new TrustedIdentity(
            "",
            "",
            new HashSet<string>());

    var authorization =
        http.Request.Headers.Authorization
            .ToString();

    if (!authorization.StartsWith(
            "Bearer ",
            StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    var token =
        authorization["Bearer ".Length..]
            .Trim();

    var parts =
        token.Split('.');

    if (parts.Length != 3)
    {
        return false;
    }

    try
    {
        var bytes =
            DecodeBase64Url(parts[1]);

        using var document =
            JsonDocument.Parse(bytes);

        var root =
            document.RootElement;

        if (!TryGetString(
                root,
                "sub",
                out var principal)
            || !TryGetString(
                root,
                "consumer",
                out var consumer)
            || !TryGetString(
                root,
                "scope",
                out var scope)
            || !root.TryGetProperty(
                "iat",
                out var iat)
            || iat.ValueKind
                != JsonValueKind.Number
            || !iat.TryGetInt64(out _)
            || !root.TryGetProperty(
                "exp",
                out var exp)
            || exp.ValueKind
                != JsonValueKind.Number
            || !exp.TryGetInt64(out _))
        {
            return false;
        }

        var scopes =
            scope.Split(
                    ' ',
                    StringSplitOptions
                        .RemoveEmptyEntries
                    | StringSplitOptions
                        .TrimEntries)
                .ToHashSet(
                    StringComparer.Ordinal);

        identity =
            new TrustedIdentity(
                principal,
                consumer,
                scopes);

        return true;
    }
    catch
    {
        return false;
    }
}


static bool TryGetString(
    JsonElement root,
    string name,
    out string value)
{
    value = "";

    if (!root.TryGetProperty(
            name,
            out var element)
        || element.ValueKind
            != JsonValueKind.String)
    {
        return false;
    }

    value =
        element.GetString() ?? "";

    return true;
}


static byte[] DecodeBase64Url(
    string value)
{
    var normalized =
        value.Replace('-', '+')
            .Replace('_', '/');

    normalized +=
        (normalized.Length % 4) switch
        {
            2 => "==",
            3 => "=",
            _ => ""
        };

    return Convert.FromBase64String(
        normalized);
}


static IResult ErrorFromAction(
    JsonElement root,
    string correlationId,
    int actionVersion)
{
    var code =
        root.TryGetProperty(
            "code",
            out var codeElement)
        && codeElement.ValueKind
            == JsonValueKind.String
            ? codeElement.GetString()!
            : "internal.error";

    var message =
        root.TryGetProperty(
            "message",
            out var messageElement)
        && messageElement.ValueKind
            == JsonValueKind.String
            ? messageElement.GetString()!
            : "action failed";

    var retryable =
        root.TryGetProperty(
            "retryable",
            out var retryElement)
        && retryElement.ValueKind
            is JsonValueKind.True
            or JsonValueKind.False
            && retryElement.GetBoolean();

    JsonElement? details = null;

    if (root.TryGetProperty(
            "details",
            out var detailsElement))
    {
        details =
            detailsElement.Clone();
    }

    var httpStatus =
        code switch
        {
            "idempotency.required" => 400,
            "access.denied" => 403,
            "action.not_found" => 404,
            "idempotency.conflict" => 409,
            "action.contract_violation" => 500,
            "action.timeout" => 504,
            _ => 422
        };

    return Results.Json(
        new
        {
            status = "error",
            code,
            message,
            retryable,
            details =
                details
                ?? JsonSerializer
                    .SerializeToElement(
                        new { }),
            meta = new
            {
                correlationId,
                actionVersion
            }
        },
        statusCode: httpStatus);
}


static IResult Error(
    int httpStatus,
    string code,
    string message,
    string? correlationId,
    int? actionVersion)
{
    return Results.Json(
        new
        {
            status = "error",
            code,
            message,
            retryable = false,
            details = new { },
            meta = new
            {
                correlationId,
                actionVersion
            }
        },
        statusCode: httpStatus);
}


static async Task WriteErrorAsync(
    HttpContext http,
    int httpStatus,
    string code,
    string message,
    string? correlationId,
    int? actionVersion)
{
    http.Response.StatusCode =
        httpStatus;

    http.Response.ContentType =
        "application/json";

    await JsonSerializer.SerializeAsync(
        http.Response.Body,
        new
        {
            status = "error",
            code,
            message,
            retryable = false,
            details = new { },
            meta = new
            {
                correlationId,
                actionVersion
            }
        });
}

static async Task<List<OpenApiAction>>
    LoadOpenApiDefaultsAsync(
        string connectionString)
{
    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await using var command =
        new NpgsqlCommand(
            """
            SELECT api.openapi_default_actions()::text;
            """,
            connection);

    var scalar =
        await command.ExecuteScalarAsync();

    if (scalar is not string json)
    {
        return new List<OpenApiAction>();
    }

    using var document =
        JsonDocument.Parse(json);

    return document.RootElement
        .EnumerateArray()
        .Select(ParseOpenApiAction)
        .ToList();
}


static async Task<OpenApiAction?>
    LoadOpenApiActionAsync(
        string connectionString,
        string module,
        string action,
        int version)
{
    await using var connection =
        new NpgsqlConnection(connectionString);

    await connection.OpenAsync();

    await using var command =
        new NpgsqlCommand(
            """
            SELECT api.openapi_action(
                @module,
                @action,
                @version
            )::text;
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
        version);

    var scalar =
        await command.ExecuteScalarAsync();

    if (scalar is not string json
        || string.IsNullOrWhiteSpace(json))
    {
        return null;
    }

    using var document =
        JsonDocument.Parse(json);

    return ParseOpenApiAction(
        document.RootElement);
}


static OpenApiAction ParseOpenApiAction(
    JsonElement root)
{
    return new OpenApiAction(
        root.GetProperty("module")
            .GetString()!,
        root.GetProperty("action")
            .GetString()!,
        root.GetProperty("version")
            .GetInt32(),
        root.GetProperty("request_schema")
            .Clone(),
        root.GetProperty("response_schema")
            .Clone(),
        root.GetProperty("outcomes")
            .EnumerateArray()
            .Select(x => x.GetString()!)
            .ToArray(),
        root.GetProperty("idempotency_mode")
            .GetString()!
    );
}


static object BuildOpenApiDocument(
    IEnumerable<OpenApiAction> definitions)
{
    var paths =
        new Dictionary<string, object>();

    foreach (var definition
             in definitions)
    {
        var path =
            $"/api/{definition.Module}/{definition.Action}";

        var parameters =
            new List<object>
            {
                new
                {
                    name = "X-Action-Version",
                    @in = "header",
                    required = false,
                    schema = new
                    {
                        type = "integer",
                        minimum = 1
                    }
                }
            };

        if (definition.IdempotencyMode != "none")
        {
            parameters.Add(
                new
                {
                    name = "Idempotency-Key",
                    @in = "header",
                    required =
                        definition.IdempotencyMode
                            == "required",
                    schema = new
                    {
                        type = "string",
                        maxLength = 128
                    }
                });
        }

        paths[path] =
            new
            {
                post = new
                {
                    operationId =
                        $"{definition.Module}_{definition.Action}_v{definition.Version}",

                    parameters,

                    requestBody = new
                    {
                        required = true,
                        content = new Dictionary<string, object>
                        {
                            ["application/json"] =
                                new
                                {
                                    schema =
                                        definition
                                            .RequestSchema
                                }
                        }
                    },

                    responses =
                        new Dictionary<string, object>
                        {
                            ["200"] =
                                new
                                {
                                    description =
                                        "Successful action execution",

                                    content =
                                        new Dictionary<string, object>
                                        {
                                            ["application/json"] =
                                                new
                                                {
                                                    schema =
                                                        new
                                                        {
                                                            type =
                                                                "object",

                                                            properties =
                                                                new
                                                                {
                                                                    status =
                                                                        new
                                                                        {
                                                                            type =
                                                                                "string",

                                                                            @const =
                                                                                "ok"
                                                                        },

                                                                    outcome =
                                                                        new
                                                                        {
                                                                            type =
                                                                                "string",

                                                                            @enum =
                                                                                definition
                                                                                    .Outcomes
                                                                        },

                                                                    result =
                                                                        definition
                                                                            .ResponseSchema,

                                                                    meta =
                                                                        new
                                                                        {
                                                                            type =
                                                                                "object",

                                                                            properties =
                                                                                new
                                                                                {
                                                                                    correlationId =
                                                                                        new
                                                                                        {
                                                                                            type =
                                                                                                "string",

                                                                                            format =
                                                                                                "uuid"
                                                                                        },

                                                                                    actionVersion =
                                                                                        new
                                                                                        {
                                                                                            type =
                                                                                                "integer"
                                                                                        }
                                                                                }
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                }
            };
    }

    return new
    {
        openapi = "3.1.0",

        info = new
        {
            title = "ModuleDev Action API",
            version = "course-1"
        },

        paths
    };
}

sealed record TrustedIdentity(
    string Principal,
    string Consumer,
    HashSet<string> Scopes);


sealed record ActionDefinition(
    int Version,
    string RequestSchema,
    string ResponseSchema,
    HashSet<string> Outcomes,
    string[] RequiredPolicy,
    string IdempotencyMode,
    int TimeoutMs);

sealed record OpenApiAction(
    string Module,
    string Action,
    int Version,
    JsonElement RequestSchema,
    JsonElement ResponseSchema,
    string[] Outcomes,
    string IdempotencyMode);

static class SchemaCache
{
    private static readonly
        ConcurrentDictionary<
            string,
            Lazy<JsonSchema>>
        Cache = new(
            StringComparer.Ordinal);

    public static JsonSchema Get(
        string schemaText)
    {
        return Cache
            .GetOrAdd(
                schemaText,
                static text =>
                    new Lazy<JsonSchema>(
                        () =>
                            JsonSchema.FromText(
                                text),
                        LazyThreadSafetyMode
                            .ExecutionAndPublication))
            .Value;
    }
}