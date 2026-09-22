using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json.Nodes;


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
