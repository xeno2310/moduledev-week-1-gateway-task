var builder = WebApplication.CreateBuilder(args);

var apiBaseUrl =
    builder.Configuration["API_BASE_URL"]
    ?? "http://api:8080";

builder.Services.AddHttpClient("api", client =>
{
    client.BaseAddress = new Uri(apiBaseUrl);
});

var app = builder.Build();

app.MapGet("/health/live", () =>
{
    return Results.Ok(new
    {
        status = "ok"
    });
});

app.MapGet("/health/ready", async (IHttpClientFactory factory) =>
{
    try
    {
        var client = factory.CreateClient("api");

        using var response =
            await client.GetAsync("/health/ready");

        if (response.IsSuccessStatusCode)
        {
            return Results.Ok(new
            {
                status = "ready"
            });
        }

        return Results.StatusCode(
            StatusCodes.Status503ServiceUnavailable);
    }
    catch
    {
        return Results.StatusCode(
            StatusCodes.Status503ServiceUnavailable);
    }
});

app.MapPost(
    "/api/{module}/{action}",
    ProxyToApi);

app.MapGet(
    "/openapi/default.json",
    ProxyToApi);

app.MapGet(
    "/openapi/actions/{module}/{action}/{version:int}.json",
    ProxyToApi);

app.Run();

static async Task ProxyToApi(
    HttpContext context,
    IHttpClientFactory factory)
{
    var client = factory.CreateClient("api");

    var targetUri =
        context.Request.Path +
        context.Request.QueryString;

    using var requestMessage =
        new HttpRequestMessage(
            new HttpMethod(context.Request.Method),
            targetUri);

    if (context.Request.ContentLength > 0 ||
        context.Request.Headers.ContainsKey("Transfer-Encoding"))
    {
        requestMessage.Content =
            new StreamContent(context.Request.Body);
    }

    foreach (var header in context.Request.Headers)
    {
        if (header.Key.Equals(
                "Host",
                StringComparison.OrdinalIgnoreCase))
        {
            continue;
        }

        if (!requestMessage.Headers.TryAddWithoutValidation(
                header.Key,
                header.Value.ToArray()))
        {
            requestMessage.Content?.Headers.TryAddWithoutValidation(
                header.Key,
                header.Value.ToArray());
        }
    }

    using var responseMessage =
        await client.SendAsync(
            requestMessage,
            HttpCompletionOption.ResponseHeadersRead,
            context.RequestAborted);

    context.Response.StatusCode =
        (int)responseMessage.StatusCode;

    foreach (var header in responseMessage.Headers)
    {
        context.Response.Headers[header.Key] =
            header.Value.ToArray();
    }

    foreach (var header in responseMessage.Content.Headers)
    {
        context.Response.Headers[header.Key] =
            header.Value.ToArray();
    }

    context.Response.Headers.Remove("transfer-encoding");

    await responseMessage.Content.CopyToAsync(
        context.Response.Body,
        context.RequestAborted);
}