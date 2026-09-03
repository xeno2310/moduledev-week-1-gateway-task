# C4 Container diagram — Week 2 Workflow Runtime

## Scope

Система состоит из generic action runtime недели 1 и устойчивого
workflow runtime недели 2.

Workflow definitions и runtime state хранятся в PostgreSQL.
Карты исполняются generic worker без C#-ветвления по именам flow,
step, action или outcome.

Единственная внешняя HTTP-точка системы — `gateway` на порту `8080`.

## Container diagram

~~~mermaid
flowchart LR
    Client["External client"]

    subgraph System["ModuleDev Workflow Runtime"]
        Gateway["gateway
ASP.NET Core
public :8080"]

        API["api
ASP.NET Core
generic action runtime
internal only"]

        CLI["cli
.NET Course CLI
publication + workflow commands"]

        WorkerA["worker-a
.NET Workflow Worker
workflow_worker role"]

        WorkerB["worker-b
.NET Workflow Worker
workflow_worker role"]

        DB[("postgres
PostgreSQL
catalog + workflow state
named volume")]
    end

    Client -->|"HTTP
actions / OpenAPI / health"| Gateway

    Gateway -->|"Internal HTTP
Compose DNS: api:8080"| API

    API -->|"Npgsql
course_runtime"| DB

    CLI -->|"Npgsql
migrations / publication /
flow validate, publish, activate,
start, get, signal"| DB

    WorkerA -->|"Npgsql
claim_jobs
api.invoke
finish_job
fail_job"| DB

    WorkerB -->|"Npgsql
claim_jobs
api.invoke
finish_job
fail_job"| DB
~~~

## Responsibilities

| Container | Responsibility |
|---|---|
| `gateway` | Единственная внешняя HTTP-точка входа. Публикует host port `8080`, применяет route whitelist и проксирует разрешённые запросы во внутренний `api`. |
| `api` | Generic action runtime недели 1. Проверяет trusted context и policy, разрешает опубликованный action из PostgreSQL catalog и выполняет `api.invoke(...)`. |
| `cli` | Выполняет миграции и contract commands. Для workflow: validation, publication, activation, start, get и signal. |
| `worker-a` / `worker-b` | Два экземпляра одного worker image. Конкурентно получают jobs через lease, строят payload из published map, вызывают action и завершают job через fencing-aware functions. |
| `postgres` | Авторитетное persistent state: action catalog, published workflow definitions, process/step/job/attempt/signal/event state и предметные данные. Данные сохраняются в named volume. |

## Network boundary

Только `gateway` публикует host port `8080`.

`api`, `worker-a`, `worker-b` и PostgreSQL не публикуют application
ports наружу и доступны только внутри Compose network.

Workers не вызывают `api` по HTTP. Они используют PostgreSQL runtime
boundary и вызывают зарегистрированный action через `api.invoke(...)`
в той же Npgsql transaction, в которой при успехе выполняется
`workflow.finish_job(...)`.

## Worker database boundary

Workers подключаются под отдельной ролью `workflow_worker`.

Этой роли разрешён EXECUTE только для runtime boundaries:

- `workflow.claim_jobs`;
- `api.invoke`;
- `workflow.finish_job`;
- `workflow.fail_job`.

Прямой DML к workflow, catalog и предметным таблицам worker не нужен.

## Persistence and version pinning

PostgreSQL является единственным авторитетным хранилищем workflow state.

Пересоздание `gateway`, `api`, `worker-a` или `worker-b` не уничтожает
process state, потому что PostgreSQL использует named volume.

При `flow start` process закрепляет конкретную published flow version.
Изменение active version влияет только на новые processes.

## Lease and fencing

Job выполняется только владельцем действующей lease.

Каждый claim/reclaim увеличивает `lease_version`. Вызовы
`finish_job` и `fail_job` принимаются только от текущего
`lease_owner` с текущим `lease_version`.

После истечения lease другой worker может reclaim job. Предыдущая
RUNNING attempt становится `STALE`, а поздний результат старого
владельца отклоняется fencing-проверкой.

Подробное решение описано в
[`ADR-003`](../adr/003-workflow-lease-fencing.md).