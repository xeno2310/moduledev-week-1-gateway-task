# ModuleDev Week 4 — reliability and recovery

Решение в одном репозитории: Week 1 — PostgreSQL action catalog и generic Gateway/API; Week 2 — workflow и workers; Week 3 — Python payment perimeter с Outbox/Inbox; Week 4 — конкурентная доставка, recovery, observability и diagnostics. PostgreSQL хранит durable runtime. Текущий кандидат: ветка `week4-reliability`; официальный Week 4 checker — **139/139 PASS, exit 0**. Результаты ниже относятся к рабочему дереву, не к будущему commit.

## Архитектура

| Compose service | Ответственность |
|---|---|
| `postgres` | База `course`, init migrations 001–020, роли и named volume `course_postgres_data` |
| `cli` | Одноразовый bootstrap: production CLI migrations 021–026, action manifests и workflow maps |
| `gateway` | Единственный опубликованный на host HTTP port; proxy к API |
| `api` | JWT, schemas, generic `api.invoke`, receipts, diagnostics и SQL metrics snapshot |
| `worker-a`, `worker-b` | Один C# image, конкурентные jobs, heartbeat и fencing |
| `provider-simulator` | Закреплённый provider v0.2.0, payment request и callback |
| `outbox-dispatcher`, `outbox-dispatcher-b` | Один Python image, SQL claim/ack и provider HTTP; разные owners |
| `receipt-adapter` | Тот же Python image, legacy callback → signed receipt → Gateway/API |
| `inbox-reconciler`, `inbox-reconciler-b` | Тот же Python image, конкурентное применение Inbox |

Путь: client → Gateway → generic API → PostgreSQL catalog/runtime → worker replicas → Outbox → dispatcher replicas → provider → callback → receipt adapter → Inbox → reconciler replicas → workflow signal/manual decision → terminal process. Только Gateway публикует host port. Участники стартуют после успешного `cli` bootstrap; provider не является readiness dependency.

## Fresh start

Нужны Docker Engine с Linux containers и Compose v2, .NET SDK 10, Python 3.11+, Bash. Для официального checker дополнительно нужен host `psql`. `.env` игнорируется Git. Из корня решения создайте новый project с random synthetic credentials; команда не выводит значения:

```bash
PYTHONPATH=tests python3 - <<'PY'
from pathlib import Path
from uuid import uuid4
from week3_e2e import synthetic_environment
env = synthetic_environment()
env['COMPOSE_PROJECT_NAME'] = 'week4-local-' + uuid4().hex
env['CALLBACK_MAX_ATTEMPTS'] = '5'
Path('.env').write_text(''.join(f'{key}={value}\n' for key, value in sorted(env.items())), encoding='utf-8')
print('Created ignored .env with synthetic credentials and a fresh project name')
PY
docker compose --env-file .env up -d --build
docker compose --env-file .env ps -a
gateway_address=$(docker compose --env-file .env port gateway 8080)
curl --fail "http://$gateway_address/health/ready"
```

Windows PowerShell; каждый блок сам задаёт `$RepoPath`:

```powershell
$RepoPath = (Resolve-Path .).Path
Set-Location $RepoPath
@'
from pathlib import Path
from uuid import uuid4
import sys
sys.path.insert(0, 'tests')
from week3_e2e import synthetic_environment
env = synthetic_environment()
env['COMPOSE_PROJECT_NAME'] = 'week4-local-' + uuid4().hex
env['CALLBACK_MAX_ATTEMPTS'] = '5'
Path('.env').write_text(''.join(f'{key}={value}\n' for key, value in sorted(env.items())), encoding='utf-8')
print('Created ignored .env with synthetic credentials and a fresh project name')
'@ | python -
if ($LASTEXITCODE -ne 0) { throw '.env generation failed' }
```

```powershell
$RepoPath = (Resolve-Path .).Path
Set-Location $RepoPath
docker compose --env-file .env up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Compose startup failed' }
docker compose --env-file .env ps -a
$GatewayAddress = (docker compose --env-file .env port gateway 8080).Trim()
$Deadline = [DateTime]::UtcNow.AddSeconds(120)
do {
    try { $Ready = (Invoke-RestMethod "http://$GatewayAddress/health/ready" -TimeoutSec 5).status -eq 'ready' }
    catch { $Ready = $false }
    if (-not $Ready) { Start-Sleep -Milliseconds 500 }
} until ($Ready -or [DateTime]::UtcNow -ge $Deadline)
if (-not $Ready) { throw 'Gateway readiness deadline exceeded' }
```

Новый `COMPOSE_PROJECT_NAME` создаёт fresh named volume. `cli` должен завершиться с exit 0. Для просмотра и безопасной остановки без удаления volume:

```bash
docker compose --env-file .env ps -a
docker compose --env-file .env down
```

```powershell
$RepoPath = (Resolve-Path .).Path
Set-Location $RepoPath
docker compose --env-file .env ps -a
docker compose --env-file .env down
if ($LASTEXITCODE -ne 0) { throw 'Compose stop failed' }
```

Callback JWT из synthetic helper живёт два часа; обновите его перед длительным ручным запуском. Не публикуйте `.env`, `docker compose config`, tokens или callback body.

## Existing Week 3 volume → Week 4

Используйте прежние `COMPOSE_PROJECT_NAME` и **прежние DB passwords**. Новый PostgreSQL image не выполняет init scripts на существующем volume. Администратор явно обновляет role grants, затем production CLI применяет только 021–026. Не удаляйте volume для upgrade.

```bash
docker compose --env-file .env build postgres cli
docker compose --env-file .env up -d --no-deps postgres
docker compose --env-file .env exec -T postgres sh /docker-entrypoint-initdb.d/000_week4_roles.sh
docker compose --env-file .env run --rm --no-deps cli migration apply /app/db/migrations/week4
docker compose --env-file .env run --rm --no-deps cli migration apply /app/db/migrations/week4
docker compose --env-file .env exec -T postgres psql -U postgres -d course -Atc 'SELECT file_name, checksum FROM public.course_schema_migrations ORDER BY file_name'
docker compose --env-file .env up -d --build
```

Второй `migration apply` возвращает `applied=0, skipped=6`; изменение checksum применённого файла вызывает conflict. `cli bootstrap` тоже безопасно повторяет 021–026 и публикацию. Init 001–020 уже выполнены и не передаются CLI повторно. Domain data и named volume сохраняются; history хранит шесть файлов 021–026. Role bootstrap идемпотентно восстанавливает grants.

```powershell
$RepoPath = (Resolve-Path .).Path
Set-Location $RepoPath
docker compose --env-file .env build postgres cli
if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
docker compose --env-file .env up -d --no-deps postgres
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL startup failed' }
docker compose --env-file .env exec -T postgres sh /docker-entrypoint-initdb.d/000_week4_roles.sh
if ($LASTEXITCODE -ne 0) { throw 'Role bootstrap failed' }
foreach ($Apply in 1..2) {
    docker compose --env-file .env run --rm --no-deps cli migration apply /app/db/migrations/week4
    if ($LASTEXITCODE -ne 0) { throw 'Migration apply failed' }
}
docker compose --env-file .env exec -T postgres psql -U postgres -d course -Atc 'SELECT file_name, checksum FROM public.course_schema_migrations ORDER BY file_name'
docker compose --env-file .env up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Upgrade startup failed' }
```

## Configuration

Числовые настройки — десятичные целые. Явно заданная пустая policy variable отклоняется. Профиль `COURSE_TEST_PROFILE=1` ускоряет проверки. `—` означает обязательное значение или Compose service URL. Ниже указаны имена секретов без их значений.

| Variable | Service | Purpose | Normal / test default | Range or format | Secret |
|---|---|---|---|---|---|
| `COURSE_TEST_PROFILE` | all | Timing/failpoint profile | 0 / 1 | 0 or 1 | no |
| `COURSE_JOB_LEASE_MS` | postgres, workers | Job lease | 10000 / 2000 ms | 100–300000 | no |
| `COURSE_WORKER_POLL_MS` | workers | Claim interval | 250 / 50 ms | 1–60000 | no |
| `COURSE_OUTBOX_LEASE_MS` | postgres, dispatchers | Delivery lease | 20000 / 2000 ms | 100–300000 | no |
| `COURSE_OUTBOX_POLL_MS` | dispatchers | Claim interval | 400 / 100 ms | 1–60000 | no |
| `COURSE_OUTBOX_MAX_ATTEMPTS` | postgres, dispatchers | Attempts incl. first | 4 / 4 | 1–100 | no |
| `COURSE_OUTBOX_BACKOFF_BASE_MS` | postgres | Retry base | 1000 / 200 ms | 1–300000 | no |
| `COURSE_OUTBOX_BACKOFF_MAX_MS` | postgres | Cap before jitter | 30000 / 800 ms | 1–300000, ≥ base | no |
| `COURSE_OUTBOX_JITTER_MAX_MS` | postgres | Additive jitter | 1000 / 100 ms | 0–300000 | no |
| `COURSE_PROVIDER_TIMEOUT_MS` | dispatchers | HTTP timeout | 10000 / 500 ms | 1–300000, < lease | no |
| `COURSE_INBOX_POLL_MS` | reconcilers | Inbox interval | 400 / 100 ms | 1–60000 | no |
| `WORKER_INSTANCE_ID` | workers | Job owner | worker-a / worker-b | distinct nonempty ID | no |
| `OUTBOX_OWNER`, `OUTBOX_OWNER_B` | dispatchers | Delivery owner | service names | distinct nonempty ID | no |
| `COURSE_FAILPOINT` | API, workers, dispatchers | Crash barrier | disabled | exact name below | no |
| `COURSE_GATEWAY_PORT` | gateway | Loopback host port | 8080 | TCP port; 0 = random local | no |
| `ASPNETCORE_URLS` | gateway, API | Internal HTTP | http://+:8080 | URL | no |
| `PORT` | adapter | Callback listener | 8082 | TCP port | no |
| `PROVIDER_URL` | dispatchers | Provider base | service URL | URL | no |
| `RECEIPT_API_URL` | adapter | Receipt endpoint | Gateway URL | URL | no |
| `PROVIDER_CALLBACK_CAPABILITY` | adapter, provider | Callback path capability | — | opaque string | yes |
| `PROVIDER_CALLBACK_TOKEN` | adapter | Receipt JWT | — | JWT, receipt:write | yes |
| `PROVIDER_HMAC_SECRET` | adapter, API | Receipt signing | — | opaque key | yes |
| `PROVIDER_AUDIT_TOKEN` | provider | Internal audit | — | opaque token | yes |
| `SIMULATOR_MODE` | provider | Behavior | success | simulator mode | no |
| `CALLBACK_DELAY`, `CALLBACK_TIMEOUT`, `CALLBACK_RETRY_DELAY` | provider | Callback timing | 200ms, 1s, 200ms | Go duration | no |
| `CALLBACK_MAX_ATTEMPTS` | provider | Callback retry budget | 5 | positive integer | no |
| `COURSE_JWT_ISSUER`, `COURSE_JWT_AUDIENCE` | C# services | JWT claims | — | nonempty string | no |
| `COURSE_JWT_SIGNING_KEY` | C# services | JWT signature | — | opaque key | yes |
| `COURSE_POSTGRES_PASSWORD`, `COURSE_MIGRATOR_PASSWORD`, `COURSE_PUBLISHER_PASSWORD`, `COURSE_RUNTIME_PASSWORD` | postgres/consumers | DB roles | local-only Compose placeholders | password | yes |
| `COURSE_WORKER_PASSWORD`, `COURSE_OUTBOX_PASSWORD`, `COURSE_INBOX_PASSWORD`, `COURSE_AUTOCHECK_PASSWORD` | postgres/consumers | DB roles | local-only placeholders except autocheck | password | yes |
| `COURSE_DB_CONNECTION`, `COURSE_MIGRATION_DB_CONNECTION` | API/workers/CLI | Role-specific DB | Compose-built | connection string | yes |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | dispatchers/reconcilers | Role-specific DB | service DNS, 5432, course, role, password | PostgreSQL setting | password only |

Gateway, API, workers и Python observability используют internal 8080; adapter 8082; provider 8081; PostgreSQL 5432. Эти дополнительные порты на host не публикуются.

## Lease, retry and recovery

Job lease равен `COURSE_JOB_LEASE_MS` (test 2000 ms); task timeout к lease **не добавляется**. Живой worker продлевает его отдельной короткой transaction/connection, независимо от action transaction. Conditional update требует `LEASED`, owner, `leaseVersion` и неистёкший lease. Heartbeat не увеличивает `leaseVersion` и прекращается при timeout, finish или lease loss. После crash другой worker reclaim; fencing отклоняет stale finish/fail. `executionId` стабилен при повторах, attempt и lease version указывают текущую reservation.

Outbox допускает четыре попытки **включая первую**. Первая сразу; test backoff: 200/400/800 ms плюс 0–100 ms jitter. SQL делит budget и lease между обеими dispatcher replicas. External request ID и exact body не меняются; provider idempotency сохраняет `paymentCount=1` даже после lost response. `DEAD` не значит `REJECTED`: operation остаётся `PROCESSING`, process — `WAITING_SIGNAL`. Поздний valid receipt продолжает chain, сохраняя `DEAD` evidence.

## Health and OpenMetrics

| Service | `/health/live` | `/health/ready` dependency | `/metrics` | Internal port |
|---|---|---|---|---:|
| `api` | process | bounded PostgreSQL probe | SQL snapshot | 8080 |
| `worker-a`, `worker-b` | process | bounded PostgreSQL probe | `process_up` | 8080 |
| `outbox-dispatcher`, `outbox-dispatcher-b` | process | bounded PostgreSQL probe | `process_up` | 8080 |
| `receipt-adapter` | process | bounded Gateway probe | `process_up` | 8082 |
| `inbox-reconciler`, `inbox-reconciler-b` | process | bounded PostgreSQL probe | `process_up` | 8080 |

Liveness не опрашивает внешние зависимости. Недоступный PostgreSQL даёт безопасный readiness 503; после восстановления первый API action работает одним запросом. Provider outage не выключает participant readiness. Metrics: `Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8`, завершающий `# EOF`, без уникальных IDs в labels. API даёт шесть series: `workflow_jobs_ready` (доступные jobs), `workflow_job_oldest_age_seconds`, `workflow_processes_waiting` (WAITING_SIGNAL/MANUAL), `outbox_pending` (PENDING/LEASED/RETRY_WAIT), `outbox_oldest_age_seconds`, `workflow_failures_total` (terminal failures). Python/worker metrics дают `process_up`.

## Diagnostics

Оба action идут через generic Gateway и требуют JWT scope `diagnostics:read`. Передайте свой токен в переменной `DIAGNOSTICS_TOKEN`; не записывайте его в отчёт:

```bash
gateway_address=$(docker compose --env-file .env port gateway 8080)
curl -sS -X POST "http://$gateway_address/api/diagnostics/stalled" \
  -H "Authorization: Bearer $DIAGNOSTICS_TOKEN" -H 'Content-Type: application/json' -d '{}'
curl -sS -X POST "http://$gateway_address/api/diagnostics/trace" \
  -H "Authorization: Bearer $DIAGNOSTICS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"identifier":"<operationId-or-other-supported-id>"}'
```

`diagnostics.stalled` version 1 принимает только `{}`. Пустой `result` — `{"items":[]}`. Выборка: operation `PROCESSING`, process `WAITING_SIGNAL`, Outbox `DEAD`; строка содержит только `operationId`, `processId`, `externalRequestId`. Сортировка по `operationId`, без duplicate. Late receipt убирает chain из списка. Нет/неверный JWT → 401, нет scope → 403, лишнее поле → 422 `payload.invalid`.

`diagnostics.trace` version 1 принимает один identifier из 11 типов: `correlationId`, `requestId`, `operationId`, `processId`, `stepInstanceId`, `jobId`, `executionId`, `attemptId`, `externalRequestId`, `messageId`, `decisionId`. `query.matchedBy` — array всех совпавших типов **одной** chain. Результат содержит ровно `query`, `dispatches`, `operation`, `operationEvents`, `process`, `steps`, `jobs`, `attempts`, `outbox`, `inbox`, `receipts`, `decisions`; массивы имеют детерминированный порядок времени/ID. Same-chain multi-match допустим; разные chains не объединяются, дают 409 `diagnostics.trace_ambiguous`. Unknown → 404 `diagnostics.trace_not_found`; invalid payload → 422. Нет payloads, headers, signatures, tokens, manual reason или raw errors. Формы полей задают `contracts/course-1/diagnostics-*.schema.json`.

## Crash/failpoint runbook

`COURSE_FAILPOINT` отключён по умолчанию и срабатывает только при `COURSE_TEST_PROFILE=1`. Harness ждёт structured `failpoint.reached`, не произвольный sleep, убивает одну replica; другая продолжает. Provider между request и retry не пересоздаётся. Для восстановления удалите failpoint из isolated override и пересоздайте только затронутый service, сохранив volume.

| Failpoint | Service | Transaction boundary | Recovery / evidence |
|---|---|---|---|
| `after_job_claim` | worker | Committed claim, до action | Expired lease, reclaim; job/attempt и stable execution ID |
| `after_action_before_finish` | worker | Action committed, до finish | Reclaim и idempotent replay; один domain effect |
| `after_outbox_claim` | dispatcher | Committed claim, до provider | Lease reclaim; один provider payment |
| `after_provider_response` | dispatcher | Provider ответил, до SQL ack | Same ID/body retry; requests ≥2, paymentCount=1 |
| `after_inbox_saved` | API | Inbox/receipt сохранены, до ответа | Replay callback; Inbox=1, receipt=1, SignalAccepted=1, SignalApplied=1 |
| `after_manual_decision` | API | Decision внутри transaction, **до commit** | Первая transaction rollback; retry создаёт одну decision, opposite outcome conflict |

## Troubleshooting

Начните с `docker compose --env-file .env ps -a`, readiness/metrics и ограниченных JSON logs: `docker compose --env-file .env logs --no-color --tail=100 <service>`. В отчёт переносите только safe IDs, state/count и error codes; не переносите .env, passwords, JWT, headers, signatures, callback message, receipt body или raw payment payload.

| Component | Probe / action | Safe SQL or evidence |
|---|---|---|
| CLI/migrations | `cli` exit 0 | `public.course_schema_migrations`: file_name/checksum |
| Gateway/API | Gateway/API readiness, action HTTP status, API metrics | correlation ID, safe error code |
| Workers | both readiness, ready jobs metric | `autocheck.jobs`, `autocheck.attempts`: IDs, leaseVersion |
| Dispatchers | readiness, pending metric | `autocheck.outbox`: state/count/externalRequestId |
| Adapter | Gateway readiness, callback status | `autocheck.receipts`: messageId/signatureValid |
| Reconcilers | readiness, waiting metric | `autocheck.inbox`: state/applied count |
| PostgreSQL | `pg_isready`, CLI history | read-only `autocheck` views and row counts |
| Provider | internal `GET /internal/audit/{externalRequestId}` with `X-Audit-Token` | requestCount/paymentCount, without token |

SQL example: `docker compose --env-file .env exec -T postgres psql -U postgres -d course -c 'SELECT file_name, checksum FROM public.course_schema_migrations ORDER BY file_name'`. Workflow: `docker compose --env-file .env run --rm -T cli flow get <processId>`, затем `autocheck.jobs/attempts/outbox/inbox`. Provider audit остаётся internal; не открывайте для него host port.

## Verification

Подтверждённое evidence текущего рабочего дерева:

| Проверка | Результат |
|---|---|
| Release build | PASS |
| Worker LogicTests | 53/53 |
| Python perimeter | 28/28 |
| Week 4 observability | 6/6 |
| Week 4 SQL + lease | 8/8 |
| Worker lease runtime | 10/10 (`tests/week2_regression.py`) |
| diagnostics.stalled | 8/8 |
| diagnostics.trace | 7/7 |
| Crash matrix | 6/6 |
| Existing-volume upgrade 024→026 | PASS |
| Migrations 001–026 | SHA-256 проверены |
| Официальный Week 4 checker (reference `e1ae7e03c6a4f2da5dc2ee9f02ebe3f26b2d16a3`) | **139/139 PASS, non-passed=0, exit 0** |
| Week 3 E2E | 6/6 |
| Week 2 regression | 10/10 |
| Signal concurrency | 4/4 |

После полного прогона менялись только cleanup-фрагменты `tests/week3_e2e.py` и `tests/signal_process_concurrency.py`: узкие проверки Week 3 success 1/1 и signal concurrency 4/4 прошли, новых Docker resources после cleanup — 0.

Одна каноническая команда из корня решения:

```bash
./check.sh --reference "/path/to/moduledev-week-4-reliability-task-reference"
```

`--reference` указывает отдельный checkout точного Week 4 checker. Wrapper вызывает его документированным `./check.sh --repo <solution>`, затем Release build, C#/Python unit/static checks, participant Week 4 harness (observability, SQL/lease, stalled, trace, crash matrix), Week 3 E2E, Week 2 regression и signal concurrency. Каждый harness владеет isolated Compose project/cleanup; wrapper останавливается на первом failure. Официальный checker пишет `week-4-public-report.json` в решение; wrapper удаляет созданный отчёт при завершении и отказывается запускаться, если файл уже существует.

Windows PowerShell через WSL Bash (пути заново задаются в блоке):

```powershell
$RepoPath = (Resolve-Path .).Path
$ReferencePath = (Resolve-Path '..\moduledev-week-4-reliability-task-reference').Path
$RepoWsl = (wsl wslpath -a $RepoPath).Trim()
$SourceWsl = (wsl wslpath -a $ReferencePath).Trim()
$ReferenceWsl = '/tmp/week4-reference-' + [guid]::NewGuid().ToString('N')
wsl bash -lc "git -c core.autocrlf=false clone --local --no-hardlinks '$SourceWsl' '$ReferenceWsl' && cd '$RepoWsl' && ./check.sh --reference '$ReferenceWsl'"
if ($LASTEXITCODE -ne 0) { throw 'Full verification failed' }
```

В имеющемся Windows checkout reference shell scripts и fixture стали CRLF. PowerShell блок создаёт отдельный временный checkout из Git objects с `core.autocrlf=false`, чтобы checker видел исходные LF bytes; исходный reference не меняется. Не копируйте fixtures через CRLF conversion и не меняйте committed contracts ради Windows. `check.sh` имеет LF и executable mode `100755`. Для read-only перечисления сценариев: `python tests/week4_crash_matrix.py --list` и `--list` остальных harness. Исторический Week 2 checker не является финальной проверкой Week 4.
