#!/bin/sh
set -eu

# Normal CLI invocation used by the checker:
#
#   docker compose run --rm cli action ...
#   docker compose run --rm cli flow ...
#
# Preserve the original public CLI contract exactly.
if [ "${1:-}" != "bootstrap" ]; then
    exec dotnet /app/Cli.dll "$@"
fi

if [ "$#" -ne 1 ]; then
    echo "bootstrap accepts no additional arguments" >&2
    exit 2
fi


# Forward-only Week 4 changes are checksummed by the production runner.
# Historical 001-020 remain PostgreSQL init scripts and are never replayed.
dotnet /app/Cli.dll migration apply /app/db/migrations/week4

# ------------------------------------------------------------
# Publish every tracked action manifest.
#
# This intentionally includes the Week 1 actions
# (payment.request and operation.get) as well as Week 3.
# ------------------------------------------------------------

action_count=0

for manifest in /app/manifests/*.action.json
do
    [ -f "$manifest" ] || continue

    dotnet /app/Cli.dll \
        action \
        publish \
        "$manifest" \
        >/dev/null

    action_count=$((action_count + 1))
done

if [ "$action_count" -lt 13 ]; then
    echo "bootstrap found too few action manifests: $action_count" >&2
    exit 3
fi


# ------------------------------------------------------------
# Publish tracked production workflow maps.
# ------------------------------------------------------------

flow_count=0

for flow in /app/contracts/course-1/*-v*.flow.yaml
do
    [ -f "$flow" ] || continue

    dotnet /app/Cli.dll \
        flow \
        publish \
        "$flow" \
        >/dev/null

    flow_count=$((flow_count + 1))
done

if [ "$flow_count" -lt 2 ]; then
    echo "bootstrap found too few workflow maps: $flow_count" >&2
    exit 4
fi


# ------------------------------------------------------------
# Required Week 3 active versions.
# workflow.activate_flow is idempotent for the same version.
# ------------------------------------------------------------

dotnet /app/Cli.dll \
    flow \
    activate \
    payment-processing \
    --version \
    1 \
    >/dev/null

dotnet /app/Cli.dll \
    flow \
    activate \
    payment-review \
    --version \
    1 \
    >/dev/null


printf \
    '{"status":"ok","result":{"resource":"bootstrap","operation":"completed","actions":%s,"flows":%s},"meta":{"contractVersion":"course-1"}}\n' \
    "$action_count" \
    "$flow_count"
