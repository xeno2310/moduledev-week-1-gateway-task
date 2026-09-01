FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY ["src/Cli/Cli.csproj", "src/Cli/"]
RUN dotnet restore "src/Cli/Cli.csproj"

COPY . .

WORKDIR "/src/src/Cli"

RUN dotnet publish "Cli.csproj" \
    -c Release \
    -o /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/publish .

COPY --from=build /src/contracts/course-1/action-manifest.schema.json /app/contracts/course-1/action-manifest.schema.json
ENTRYPOINT ["dotnet", "Cli.dll"]