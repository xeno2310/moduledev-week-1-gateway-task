FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ["src/Workflow.Worker/Workflow.Worker.csproj", "src/Workflow.Worker/"]

RUN dotnet restore "src/Workflow.Worker/Workflow.Worker.csproj"

COPY . .

WORKDIR /src/src/Workflow.Worker

RUN dotnet publish "Workflow.Worker.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false


FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/publish .

USER $APP_UID

ENTRYPOINT ["dotnet", "Workflow.Worker.dll"]