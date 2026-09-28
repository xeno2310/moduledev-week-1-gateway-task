# Week 3. PostgreSQL workflow и Python payment perimeter

Generic C# Gateway/API и два workflow workers исполняют опубликованные actions
и workflow-карты. PostgreSQL хранит операции, jobs/attempts, Outbox, Inbox,
receipts и решения. Python dispatcher доставляет платёж provider v0.2.0, adapter
преобразует legacy callback в подписанный receipt, reconciler применяет Inbox.

Production parent — `61a3b8affbb7d351329a64296f903e0091b6277a`.
Кандидат Week 3 recheck поверх него содержит четыре файла:

- `README.md`;
- `db/migrations/020_fix_signal_process_message_id_race.sql`;
- `tests/signal_process_concurrency.py`;
- `tests/week3_e2e.py`.

Подтверждённые результаты текущего кандидата:

| Проверка | Результат |
|---|---|
| Solution build | PASS |
| Workflow.Worker production logic | 17/17 |
| Python perimeter | 8/8 |
| Week 2 regression | 9/9 |
| `signal_process` concurrency | 4/4 |
| Participant-owned Week 3 E2E | 6/6, exit code 0 |
| Официальный `week-3-public-check/0.3` | 58/58, exit code 0 |
| `git diff --check` | PASS |
| Cleanup всех изолированных прогонов | PASS |

Это результаты кандидата с migration 020. Final exact-commit fresh-clone
verification выполняется после создания commit; новый final SHA пока не заявлен.
Ниже приведены команды для воспроизведения и ожидаемые результаты.

## Состав Week 3

| Service | Назначение |
|---|---|
| `postgres` | PostgreSQL 17, SQL runtime, migrations, durable evidence |
| `cli` | Одноразовый bootstrap: actions и две production workflow-карты |
| `gateway` | Единственная HTTP-точка с опубликованным host-портом |
| `api` | JWT, schemas, HMAC exact bytes, generic `api.invoke` |
| `worker-a`, `worker-b` | Один C# image, конкурентное выполнение jobs с fencing |
| `outbox-dispatcher` | Python → SQL claim → provider HTTP → SQL acknowledgement |
| `receipt-adapter` | Python HTTP callback → receipt v1 + HMAC → Gateway/API |
| `inbox-reconciler` | Python → `delivery.reconcile_inbox` |
| `provider-simulator` | Выданный provider v0.2.0, pinned digest из Compose |

Provider и три Python services не публикуют host-порты. Provider audit доступен
из Compose network: `GET /internal/audit/{externalRequestId}` с
`X-Audit-Token`. Память simulator не переживает его recreate; при проверке
recovery provider должен оставаться тем же контейнером.

## Воспроизводимое локальное окружение

Нужны Docker Engine/Desktop с Linux containers и Compose v2, .NET SDK 10,
Python 3.10+; для shell entrypoint checker — Bash и POSIX Python. Команды PowerShell выполняются
из корня репозитория. Для нового E2E ручная подготовка env **не нужна**:
он сам создаёт credentials, JWT, уникальный project и свежий volume.

Для ручного stack и Week 2 regression подготовьте synthetic env в текущей
PowerShell-сессии. Helper использует только Python standard library и не запускает
Docker при загрузке. JSON с секретами захватывается в переменную, не выводится:

```powershell
$RepoPath = (Get-Location).Path
$SyntheticJson = @'
import json, runpy
helpers = runpy.run_path("tests/week3_e2e.py", run_name="week3_env_helpers")
print(json.dumps(helpers["synthetic_environment"]()))
'@ | python -
if ($LASTEXITCODE -ne 0) { throw "Synthetic environment generation failed" }
$SyntheticEnv = $SyntheticJson | ConvertFrom-Json
foreach ($Property in $SyntheticEnv.PSObject.Properties) {
    Set-Item -LiteralPath ("Env:" + $Property.Name) -Value $Property.Value
}
$env:COMPOSE_PROJECT_NAME = "week3-local-" + [guid]::NewGuid().ToString("N")
$env:COMPOSE_FILE = Join-Path $RepoPath "compose.yaml"
$SyntheticJson = $null
$SyntheticEnv = $null
$Property = $null
```

Задаются все семь DB passwords, JWT issuer/audience/signing key,
`PROVIDER_CALLBACK_TOKEN` с principal `receipt-provider` и scope `receipt:write`,
HMAC secret, callback capability, audit token, внутренние URL, worker scopes и
`COURSE_TEST_PROFILE=1`. Callback token действителен два часа. Порт Gateway
выбирается Docker автоматически (`COURSE_GATEWAY_PORT=0`). Не выводите env,
tokens или `docker compose config` в публикуемые логи.

### Fresh volume

Уникальный project выше создаёт новый volume, не затрагивая существующие stacks:

```powershell
docker compose -f compose.yaml up -d --build
if ($LASTEXITCODE -ne 0) { throw "Compose startup failed" }
$GatewayAddress = (docker compose -f compose.yaml port gateway 8080).Trim()
$ReadyDeadline = [DateTime]::UtcNow.AddSeconds(120)
$Ready = $false
do {
    try {
        $Health = Invoke-RestMethod ("http://" + $GatewayAddress + "/health/ready") -TimeoutSec 5
        $Ready = $Health.status -eq "ready"
    } catch { $Ready = $false }
    if (-not $Ready) { Start-Sleep -Milliseconds 500 }
} until ($Ready -or [DateTime]::UtcNow -ge $ReadyDeadline)
if (-not $Ready) { throw "Gateway readiness deadline exceeded" }
$Health
docker compose -f compose.yaml ps -a
```

Ожидаются `status=ready`, работающие runtime services и завершённый с exit code 0
`cli`. PostgreSQL при первом init автоматически применяет migrations 001–020
из `db/migrations`, затем CLI публикует 13 actions и две карты, активируя
`payment-processing:1` и
`payment-review:1`. Отдельно повторять полный набор migration SQL не требуется.

### Уже существующий PostgreSQL volume

Используйте прежний `COMPOSE_PROJECT_NAME` и **те же DB passwords**. Генерация
новых переменных не меняет passwords уже созданных PostgreSQL roles. Для volume
production parent 61a3, инициализированного migrations 001–019, примените **только
020** через production `cli migration apply`. CLI image должен быть собран из
текущего кандидата. Rebuild PostgreSQL image не применяет init scripts к
существующему volume.

Пример из корня кандидата для существующего project с подготовленными прежними
credentials. В TEMP копируется только файл 020; PostgreSQL volume сохраняется:

```powershell
$RepoPath = (Get-Location).Path
$MigrationSource = Join-Path $RepoPath "db/migrations/020_fix_signal_process_message_id_race.sql"
$UpgradeProject = $env:COMPOSE_PROJECT_NAME
if ([string]::IsNullOrWhiteSpace($UpgradeProject)) { throw "Set the existing Compose project first" }
$ComposeArguments = @("--project-name", $UpgradeProject, "-f", (Join-Path $RepoPath "compose.yaml"))
$TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar)
$UpgradeDirectory = Join-Path $TempRoot ("week3-upgrade-" + [guid]::NewGuid().ToString("N"))
$UpgradeMount = $UpgradeDirectory + ":/upgrade:ro"
$UpgradeCreated = $false
try {
    New-Item -ItemType Directory -Path $UpgradeDirectory -ErrorAction Stop | Out-Null
    $UpgradeCreated = $true
    Copy-Item -LiteralPath $MigrationSource -Destination $UpgradeDirectory -ErrorAction Stop

    docker compose @ComposeArguments build cli
    if ($LASTEXITCODE -ne 0) { throw "Candidate CLI build failed" }
    docker compose @ComposeArguments up -d --no-build --wait --wait-timeout 120 postgres
    if ($LASTEXITCODE -ne 0) { throw "Existing PostgreSQL startup failed" }

    # First apply: applied=1, skipped=0; repeat: applied=0, skipped=1.
    foreach ($Apply in 1..2) {
        $MigrationOutput = docker compose @ComposeArguments run --rm -T --no-deps -v $UpgradeMount cli migration apply /upgrade 2>$null
        if ($LASTEXITCODE -ne 0) { throw "Migration apply failed; output withheld" }
        $Envelope = ($MigrationOutput -join "`n") | ConvertFrom-Json
        $ExpectedApplied = if ($Apply -eq 1) { 1 } else { 0 }
        if ($Envelope.status -ne "ok" -or $Envelope.result.applied -ne $ExpectedApplied -or
            $Envelope.result.skipped -ne (1 - $ExpectedApplied)) {
            throw "Unexpected migration apply result"
        }
        Write-Output ("apply {0}: applied={1}, skipped={2}" -f $Apply, $Envelope.result.applied, $Envelope.result.skipped)
    }
} finally {
    if ($UpgradeCreated) {
        $ResolvedUpgrade = [IO.Path]::GetFullPath($UpgradeDirectory)
        if ([IO.Path]::GetDirectoryName($ResolvedUpgrade) -ne $TempRoot) { throw "Unsafe TEMP cleanup path" }
        Remove-Item -LiteralPath $ResolvedUpgrade -Recurse -Force -ErrorAction Stop
    }
}
```

Первый apply возвращает `applied=1, skipped=0`, повторный —
`applied=0, skipped=1`. Для более старой БД отдельно определите недостающие
migrations. Docker init не заполняет CLI history
`public.course_schema_migrations` для 001–019, поэтому нельзя вслепую применять
весь `/app/db/migrations` поверх такого volume: старые `CREATE TABLE` будут
выполнены повторно.

Изолированная проверка upgrade 019→020 через production CLI подтвердила оба
envelope и сохранение прежнего OID функции, полного ACL, owner `course_owner`,
`SECURITY DEFINER`, `search_path=pg_catalog, workflow`, volatility `VOLATILE`
и language `plpgsql`. `course_publication` сохранила EXECUTE, у PUBLIC его нет.
В history осталась одна запись 020 с SHA-256 exact bytes файла. Числовой OID
зависит от конкретной БД.

### Migration 020: глобальный messageId

Прежний `workflow.signal_process` делал check-then-insert по глобальному
`message_id`. `SELECT ... FOR UPDATE` не блокирует отсутствующую строку:
конкурентные вызовы для разных процессов могли дойти до INSERT, и проигравший
получал raw PostgreSQL `unique_violation`.

Migration 020 использует `INSERT ... ON CONFLICT (message_id) DO NOTHING`.
При конфликте отдельный SELECT перечитывает committed winner. Совпадение
`process_id`, `signal_type`, `body_hash` и `body` даёт `duplicate`;
несовпадение — `workflow.signal_conflict`. Оба результата возвращаются до
записи `SignalAccepted`, поэтому повтор не добавляет signal/event или историю.

Решение рассчитано на используемый `READ COMMITTED`: INSERT ждёт конкурента,
следующий SELECT после его COMMIT видит строку; после ROLLBACK INSERT может
принять сигнал сам. Advisory lock намеренно не добавлен, чтобы не вводить
встречный порядок process/signal locks. Гарантия относится к одному durable
signal/event и идемпотентному результату, а не к exactly-once execution.

## Команды проверки и ожидаемые summaries

Команды ниже воспроизводят проверки; подтверждённые результаты текущего
кандидата перечислены в начале README.

### Solution и production logic tests

```powershell
dotnet build ModuleDev.sln --configuration Release
dotnet run --project tests/Workflow.Worker.LogicTests/Workflow.Worker.LogicTests.csproj --configuration Release
python -m unittest discover -s src/PythonPerimeter -p tests.py -v
```

Ожидаются `Build succeeded.`, `passed=17`, `failed=0`,
`WORKER LOGIC REGRESSION PASSED`; для Python — `Ran 8 tests` и `OK`.
Worker suite вызывает production `ValidateSchema`/`BuildPayload` через reflection;
Python suite вызывает production helpers. Эти suites не являются Docker E2E.

### Собственные 9 Week 2 regression scenarios

Нужен disposable ручной stack из раздела выше. Сценарии останавливают workers
и временно меняют SQL probe fixture, поэтому не запускайте их на ценных данных.
Сначала опубликуйте fixture из **этого** репозитория; встроенный checker создаёт
свой отдельный project, его запуск не подготавливает данный stack:

```powershell
$RepoPath = (Get-Location).Path
$Fixture = Get-Content -Raw autocheck/fixtures/fixture.json | ConvertFrom-Json
$FixtureMount = (Join-Path $RepoPath "autocheck/fixtures") + ":/audit:ro"
docker compose run --rm -T --no-deps -v $FixtureMount cli migration apply /audit/migrations
docker compose run --rm -T --no-deps -v $FixtureMount cli action publish ("/audit/" + $Fixture.files.actionManifest)
docker compose run --rm -T --no-deps -v $FixtureMount cli flow publish ("/audit/" + $Fixture.files.flowV1Json)
docker compose run --rm -T --no-deps cli flow activate $Fixture.flowName --version 1
python tests/week2_regression.py
```

Ожидаются `Ran 9 tests` и `OK`. Это отдельные CLI/worker processes и PostgreSQL,
включая длинный action, crash/rollback/reclaim, retry и fencing; provider HTTP
perimeter этими девятью сценариями не проверяется.

### Signal process concurrency: четыре сценария

```powershell
python tests/signal_process_concurrency.py --list
python -m py_compile tests/signal_process_concurrency.py
python tests/signal_process_concurrency.py
```

Подтверждённый и ожидаемый успешный summary:

```text
SUMMARY | passed=4 total=4 result=PASSED
```

| Сценарий | Действие T1 | Результат T2 |
|---|---|---|
| `same-signal-commit`: те же process/type/body/hash | COMMIT | `duplicate` |
| `other-process-commit`: другой существующий processId | COMMIT | `workflow.signal_conflict` |
| `other-body-commit`: другой body/hash | COMMIT | `workflow.signal_conflict` |
| `other-process-rollback`: другой существующий processId | ROLLBACK | `accepted` |

Тест использует три независимые persistent psql sessions: T1, T2 и observer,
с `READ COMMITTED`. T1 освобождает блокировку только после подтверждения через
`pg_blocking_pids`, что T2 действительно ждёт; sleep для синхронизации нет.
Проверяются contract envelopes без SQL exception, одна signal row, один
`SignalAccepted` и отсутствие лишней истории. Для одного process T2 ждёт
process row, для разных — незакоммиченный global unique key.

Harness сам создаёт synthetic credentials и отдельный Compose project/fresh
volume, собирает только PostgreSQL и CLI, вызывает production SQL boundary
через роль `course_publication`. Проверяется установленное тело migration 020.
Project/volume/network удаляются в `finally` с проверкой отсутствия ресурсов.
`--list` не запускает Docker.

### Participant-owned Week 3 DB/HTTP E2E

```powershell
python tests/week3_e2e.py --help
python tests/week3_e2e.py --list
python tests/week3_e2e.py
```

Подтверждённый и ожидаемый успешный итог полного запуска:

```text
PASS | success
PASS | reject
PASS | lost-response
PASS | early-duplicate-conflict
PASS | manual-concurrency
PASS | stop-recreate
CLEANUP | isolated project and volume removed
WEEK3 E2E | passed=6 total=6 result=PASSED
```

Для отдельного сценария, например:

```powershell
python tests/week3_e2e.py --scenario early-duplicate-conflict
python tests/week3_e2e.py --scenario lost-response --scenario stop-recreate
```

Harness независимо от ручного окружения создаёт `week3-e2e-<uuid>`, private image
tags, synthetic credentials и ephemeral Gateway port. Он использует настоящий
Compose stack, HTTP Gateway/API, workers, три Python services, SQL evidence и
provider audit. Reference repository и checker не импортируются и не требуются.
`--help`/`--list` не запускают Docker и не создают stack. Полный запуск собирает
images; последующий recreate трёх Python services выполняется с `--no-build`.

Cleanup в `finally` делает `down --volumes --remove-orphans` только для своего
project, включая обработанный Ctrl+C/SIGTERM; global prune не используется.
При недоступном Docker daemon cleanup сообщает собственный project и завершает
запуск с ошибкой. Принудительное убийство самого Python/хоста не может гарантировать
выполнение `finally`. Build cache/images cleanup не удаляет. Сырые HTTP bodies,
subprocess output, credentials и tokens не печатаются; ошибки имеют безопасные
phase labels. При неудаче scenario можно повторить отдельно в новом project.
Subprocess вызываются списком аргументов без shell, поэтому Windows paths с
пробелами не требуют ручного quoting. HTTP имеет timeout 8/10 s, SQL — session
`statement_timeout=15s` и `lock_timeout=5s`, Docker SQL exec — 30 s. Обычные Compose
команды ограничены 120 s, build — 1200 s, первый up — 240 s; `--timeout` (10–300 s,
по умолчанию 90 s) задаёт срок ожидания сходимости состояния. Один уже начавшийся
probe может выйти за этот срок в пределах собственного timeout. После up/recreate
проверяется Running-состояние workers/Python containers, при cleanup — отсутствие
контейнеров, volume и network с label своего project.

| Сценарий | Assertions |
|---|---|
| `success` | HTTP/SQL operation `COMPLETED`, workflow `COMPLETED`, Inbox `APPLIED`, Outbox/external request `CONFIRMED`, один `OPERATION_COMPLETED`, provider `paymentCount=1` |
| `reject` | Operation `REJECTED`, завершённый reject path, один `OPERATION_REJECTED`, ноль `OPERATION_COMPLETED`, один provider effect |
| `lost-response` | Adapter остановлен до наблюдаемого retry; audit `requestCount>=2`, SQL `attempt_count>=2`, тот же external ID, `paymentCount=1`, затем применённый receipt |
| `early-duplicate-conflict` | Durable receipt до wait; применение настоящими workers; повтор provider callback формирует receipt с теми же `messageId`/`externalRequestId`/`outcome`/`occurredAt` и receipt body hash; этот повтор и fresh-key domain duplicate не меняют history; оба conflict paths возвращают `409 idempotency.conflict`. Исходные raw callback bytes не сравниваются |
| `manual-concurrency` | Approve/reject отправлены двумя HTTP threads после barrier; один winner, один `409 workflow.decision_conflict`, одно решение с principal/reason hash и только ветка победителя |
| `stop-recreate` | При одном recreate сохранены pending Inbox A и unsent Outbox B; изменились IDs всех трёх Python containers, image IDs прежние; provider не пересоздан; обе доставки завершаются с одним effect каждая |

Особенности первого прогона:

- В `lost-response` остановленный adapter исключает ранний `CONFIRMED`, который
  иначе законно отменил бы retry. `CALLBACK_MAX_ATTEMPTS=100` — допустимый максимум
  provider simulator; `CALLBACK_RETRY_DELAY` остаётся `200ms`. Это даёт примерно
  20 секунд одних межпопыточных задержек плюс длительность самих запросов;
  budget не гарантирует покрытие всего 45-секундного scenario deadline на любом
  окружении. Если audit не показывает повтор за 45 s, сценарий
  завершается ошибкой и не выдаёт проверку dispatcher retry за успешную.
- Штатный worker атомарно коммитит `prepare_external` и вход в wait. Поэтому
  `early-duplicate-conflict` при остановленных workers завершает первый реальный
  job через `claim_jobs` → `api.invoke` → `finish_job`, затем коммитит
  `api.invoke(payment.prepare_external)` для следующего **реального executionId**
  без finish. Это staging через существующие SQL boundaries, а не обычное
  transaction window worker. Тест не добавляет SQL functions/maps и не делает
  прямой DML таблиц. Он проверяет `Inbox=RECEIVED`, отсутствие signal и process
  до wait, после чего workers reclaim/finish и применяют ранний receipt.
- Provider пересоздаётся при смене mode только между завершёнными сценариями;
  его audit не сравнивается через этот reset. В `stop-recreate` provider container
  сохраняется, и audit сравнивается до/после recovery.

### Встроенный Week 2 checker и внешний Week 3 checker

**`./check.sh` в этом репозитории — встроенный Week 2 checker, не Week 3 checker.**
Он записывает `week-2-public-report.json`:

```powershell
bash ./check.sh
```

Официальный Week 3 checker запускается из отдельного reference checkout.
Стандартная shell-команда для Linux/POSIX Python и checkout с LF fixtures
(из корня кандидата; reference рядом):

```bash
RepoPath="$(pwd)"
Week3Reference="../moduledev-week-3-python-perimeter-task-reference"
git -C "$Week3Reference" rev-parse HEAD
bash "$Week3Reference/check.sh" --repo "$RepoPath"
```

Reference HEAD — `9751b01e82d6fb4c3bfea06c27b3ab86cbd6aafd`.
Fixture digest — `1f83c074ea60ae3941f75dd48e1526030ebdd5e467a66753e1c6a1c06c67c059`.
Текущий кандидат **с migration 020** фактически прошёл
`week-3-public-check/0.3`: `status=passed`, total=58, passed=58,
non-passed=0, exit code 0.

Стандартный shell entrypoint записывает `week-3-public-report.json` в candidate
repository. Выполненный source-preserving прогон на Windows использовал
неизменённый `autocheck/public_check.py` с `--repo`, `--fixtures`, `--output`
и `--compose-wrapper`: LF-копия fixtures (только CRLF→LF и UTF-8 без BOM),
report и wrapper находились в TEMP. Transport —
`C:\Program Files\Docker\cli-plugins\docker-compose.exe`;
`COMPOSE_DISABLE_ENV_FILE=1` действовал только для процесса checker.
Это отличается от запуска POSIX shell wrapper под native Windows Python.
Report и остальные TEMP-артефакты удалены cleanup; сохранённого report в
repository нет. Reference checkout нужен только для официального checker,
не для собственных Python tests.

## Диагностика Week 3

Цепочка корреляции:

`operationId → processId → jobId/executionId/attemptId → externalRequestId → messageId → provider audit`.

```powershell
docker compose ps -a
docker compose logs --no-color worker-a worker-b outbox-dispatcher inbox-reconciler
$ProcessId = "<processId>"
docker compose run --rm -T --no-deps cli flow get $ProcessId
docker compose exec -T postgres psql -U postgres -d course
```

В psql замените `<operationId>` своим UUID. Запрос показывает IDs, states и hashes,
не раскрывая body, reason или credentials:

```sql
SELECT o.operation_id, o.status, p.process_id, p.state AS process_state,
       j.job_id, j.execution_id, a.attempt_id, a.lease_version,
       a.status AS attempt_status, a.error_code,
       x.external_request_id, b.state AS outbox_state, b.attempt_count,
       r.message_id, i.state AS inbox_state, r.outcome, r.body_hash
FROM autocheck.operations o
LEFT JOIN autocheck.processes p ON p.process_id = o.process_id
LEFT JOIN autocheck.jobs j ON j.process_id = p.process_id
LEFT JOIN autocheck.attempts a ON a.job_id = j.job_id
LEFT JOIN payment.external_requests x ON x.execution_id = j.execution_id
LEFT JOIN autocheck.outbox b ON b.external_request_id = x.external_request_id
LEFT JOIN autocheck.receipts r ON r.external_request_id = x.external_request_id
LEFT JOIN autocheck.inbox i ON i.message_id = r.message_id
WHERE o.operation_id = '<operationId>'::uuid
ORDER BY j.job_id, a.attempt_number;
```

Дополнительно смотрите `autocheck.operation_events`, `autocheck.workflow_events`
по соответствующим IDs и `autocheck.decisions` для manual review. Один Outbox row
не доказывает один внешний effect: запросите provider audit из Python service,
используя audit token только через stdin (PowerShell, то же ручное окружение):

```powershell
$ExternalRequestId = "<externalRequestId>"
$AuditInput = @{ id = $ExternalRequestId; token = $env:PROVIDER_AUDIT_TOKEN } | ConvertTo-Json -Compress
$AuditCode = @'
import json, sys, urllib.parse, urllib.request
data = json.load(sys.stdin)
url = "http://provider-simulator:8081/internal/audit/" + urllib.parse.quote(data["id"], safe="")
request = urllib.request.Request(url, headers={"X-Audit-Token": data["token"]})
with urllib.request.urlopen(request, timeout=5) as response:
    audit = json.load(response)
print(json.dumps({key: audit.get(key) for key in ("operationId", "idempotencyKey", "providerPaymentId", "requestCount", "paymentCount")}))
'@
$AuditInput | docker compose exec -T receipt-adapter python -c $AuditCode
$AuditInput = $null
```

Ожидается совпадение `operationId`/`idempotencyKey` provider с externalRequestId,
`providerPaymentId` с messageId и `paymentCount=1`; `requestCount` может быть больше
одного. Не публикуйте raw adapter logs с capability path.

## Workflow lease и recovery

Миграция 018 и production worker сохраняют формулу:

```text
lease_until = clock_timestamp() + (p_lease_ms + timeout_ms) * interval '1 millisecond'
p_lease_ms = recovery/finish margin: 2000 ms при COURSE_TEST_PROFILE=1, иначе 10000 ms
```

Две секунды в test profile — **margin**, не полный workflow TTL. Task с timeout
2 s получает lease 4 s (обычно 12 s); timeout 5 s — lease 7/15 s; максимум 30 s —
lease 32/40 s. Polling составляет 50/250 ms. Отдельный delivery Outbox lease
составляет 2 s и не является workflow lease.

Worker ограничивает время `api.invoke` task timeout; margin покрывает накладные
расходы и finish. Нет heartbeat/renewal. После смерти worker следующий worker
ждёт оставшуюся lease, затем polling/очередь/locks; это trade-off защиты длинного
action и скорости recovery, а не обещание восстановления за две секунды.
Stalled живой worker тоже может потерять lease. Owner/version fencing и одна
транзакция action+finish предотвращают commit устаревшего результата после reclaim.

`test_zz_long_action_survives_old_standard_lease` в `tests/week2_regression.py`
выполняет action 2.6 s при timeout 5000 ms с двумя workers, проверяя одну успешную
attempt и один effect. Он прошёл в составе 9/9 на кандидате с migration 020.
Lease implementation и migration 018 не изменены; migration 020 касается только
гонки `signal_process`.

## Закрытие feedback

| Замечание | Изменение и подтверждение |
|---|---|
| Week 2 messageId race | Migration 020 + concurrency 4/4 |
| Week 2 validator/mapper tests | Вызовы production logic, 17/17 |
| Week 3 own E2E | Собственные DB/HTTP сценарии, 6/6 |
| Week 3 lease concern | Timeout-aware migration 018 + long-action regression в 9/9 |
| Week 3 runbook | Services, synthetic env, fresh init/upgrade, команды, диагностика и ограничения выше |
| Official compatibility | `week-3-public-check/0.3`, 58/58, exit 0 |

---

## История Week 1 / Week 2

Ниже сохранены исходное задание и описание предыдущих этапов. Для текущего
запуска, конфигурации и проверок используйте актуальный Week 3 runbook выше.

### Неделя 1. Database-first action runtime

Открытое задание первой недели ModuleDev. Здесь опубликованы условие, machine-readable contracts и открытая black-box проверка. Готового решения, внутренней шкалы и закрытых fixtures в репозитории нет.

С 22 по 26 августа 2026 года постройте C# gateway и generic C# runtime, который публикует зарегистрированные PostgreSQL-функции как HTTP actions.

Задание выполнено, если проверка после сборки может добавить функцию и manifest с неизвестными вашему C#-коду именами, вызвать новый action через общий маршрут и получить контрактный результат без изменения и пересборки API.

## Главный инвариант

> Клиент выбирает опубликованный action и передаёт payload, но не выбирает базу, schema, функцию, SQL, policy или предметный результат.

```text
POST /api/{module}/{action}
  -> gateway на :8080
  -> внутренний api по Compose DNS
  -> JWT и server-side context
  -> action manifest и request schema
  -> одна Npgsql transaction
  -> api.invoke(...)
  -> зарегистрированная PostgreSQL-функция
  -> outcome и response schema
  -> commit или rollback
```

## Срок и сдача

- начало работы: 22 августа 2026 года;
- дедлайн: 26 августа 2026 года, 23:59 по московскому времени;
- до дедлайна отправьте куратору URL приватного Git-репозитория, ветку и полный SHA коммита;
- заранее предоставьте куратору доступ к репозиторию;
- проверяется указанный коммит.

Архив в чате, ссылка без доступа или сообщение без полного SHA не считаются сдачей.

Регистрация, сдача ссылки и получение результата выполняются через production-бот курса. Пошаговая инструкция: [как пользоваться ботом ModuleDev](docs/bot-guide.md).

## Технологии и свобода реализации

Обязательны C#, актуальный LTS .NET, ASP.NET Core, PostgreSQL и Docker Compose.

На ваше усмотрение остаются:

- структура solution и число C# projects;
- ORM, query builder или чистый Npgsql;
- физические таблицы, индексы и внутренние имена;
- кеширование manifest;
- внутренние классы и интерфейсы;
- устройство idempotency storage;
- библиотеки JSON Schema и OpenAPI;
- собственные unit и integration tests.

Автопроверка не требует Clean Architecture и не ищет конкретные классы. Небольшая реализация с доказанными инвариантами предпочтительнее большого scaffold. Собственные tests рекомендуются, но не заменяют `./check.sh`.

## Обязательный интерфейс запуска

В корне решения должны находиться `compose.yaml` или `docker-compose.yml` и четыре сервиса:

| Сервис | Обязательное свойство |
|---|---|
| `gateway` | C# ASP.NET Core gateway; единственный публикует host-порт `8080` |
| `api` | Внутренний C# action runtime без опубликованных host-портов |
| `cli` | Course CLI; entrypoint принимает contract commands |
| `postgres` | PostgreSQL с базой `course`, ролью `postgres` и утилитой `psql` |

Данные PostgreSQL хранятся в named volume и переживают удаление контейнеров `gateway` и `api`. После чистого запуска не нужны ручные SQL-команды или публикация встроенных actions:

```bash
docker compose up -d --build
```

`api` читает:

| Переменная | Значение открытого профиля |
|---|---|
| `COURSE_JWT_ISSUER` | `moduledev-course` |
| `COURSE_JWT_AUDIENCE` | `moduledev-api` |
| `COURSE_JWT_SIGNING_KEY` | HS256 key длиной не менее 32 байт |

Реальный секрет в репозиторий не помещается. Проверка подменяет signing key через Compose override.

## Обязательные контракты

Точные сигнатуры, JSON envelopes, error matrix, CLI semantics, action contracts и колонки проверочных views находятся в [нормативном справочнике](docs/contract-reference.md). Machine schemas находятся в [`contracts/course-1`](contracts/course-1).

### Gateway

- внешний клиент обращается только к `http://localhost:8080`;
- gateway принимает по whitelist только actions, OpenAPI и health routes;
- gateway передаёт запрос и контрактные заголовки во внутренний `api` без изменения смысла;
- gateway обращается к `api` по Compose DNS, а не через `localhost`;
- gateway не содержит catalog, предметную логику и доступ к PostgreSQL;
- JWT, credentials и полный payload не попадают в логи;
- `GET /health/live` проверяет процесс, `GET /health/ready` — готовность внутреннего API.

### Action runtime

- один generic route: `POST /api/{module}/{action}`;
- explicit version передаётся только в `X-Action-Version`, без заголовка выбирается default;
- target разрешается только из immutable action catalog;
- request schema проверяется до предметного вызова;
- `api.invoke` повторно проверяет policy и вызывает только зарегистрированную функцию;
- outcome и `result` проверяются до commit;
- error, неизвестный outcome и invalid result откатывают весь предметный эффект;
- runtime-роль выполняет `api.invoke`, но не получает прямой DML к предметным таблицам.

### Trust boundary

- `principal`, `consumer` и `scopes` берутся только из проверенного JWT;
- claims проверяются по обязательности, JSON-типу и значению;
- `correlationId` и `deadline` формируются runtime;
- поля payload не могут подменить context, policy, target или результат;
- после аутентификации ответ содержит server-side `correlationId`;
- ошибки не раскрывают SQL, stack trace, connection string и внутренние targets.

### Версии и идемпотентность

- опубликованная версия manifest неизменяема;
- `enabled` и `is_default` меняются только атомарными CLI-командами;
- у включённого route ровно одна default-версия;
- одинаковые key и payload возвращают исходный результат без второго эффекта;
- тот же key с другим payload возвращает `409 idempotency.conflict`;
- конкурентная уникальность защищается PostgreSQL, а не process-local lock.

### Обязательные actions

- `payment.request` version 1 создаёт operation и одно событие `OPERATION_CREATED`;
- `operation.get` version 1 читает operation по `operationId`;
- клиент не передаёт workflow name/version и финальный status;
- точные payload/result schemas находятся в `contracts/course-1`.

### OpenAPI, health и evidence

- `/openapi/default.json` содержит только включённые default routes;
- `/openapi/actions/{module}/{action}/{version}.json` описывает одну точную версию;
- readiness успешен только при доступном PostgreSQL и завершённой инициализации;
- после восстановления PostgreSQL readiness возвращается без пересоздания API;
- schema `autocheck` публикует пять read-only views из нормативного справочника;
- `course_runtime` не может изменять operations или удалять events;
- object owner для `SECURITY DEFINER` имеет `NOLOGIN`.

## Что реализовать

- C# services `gateway`, `api`, `cli` и PostgreSQL в Compose;
- checksummed migrations и отдельные publication/runtime roles;
- immutable action catalog и `api.invoke(...)`;
- generic HTTP executor с request/response schema validation;
- JWT, server-side context и policy на двух границах;
- transactional idempotency и rollback contract;
- `payment.request` и `operation.get` как PostgreSQL-функции;
- единый HTTP error envelope;
- manifest-driven OpenAPI и health endpoints;
- пять read-only views schema `autocheck`;
- C4 Container diagram;
- ADR о trust boundary;
- ADR о техническом и предметном результате;
- README с запуском, диагностикой и проверкой.

## Открытая проверка

Запуск из корня решения:

```bash
./check.sh
```

| Область | Что проверяется |
|---|---|
| Compose | Чистая сборка, четыре сервиса, единственный внешний порт gateway |
| Publication | Migration, две версии нового action, default/explicit version, activate/disable |
| Security | JWT, claims, policy в HTTP и `api.invoke`, отсутствие секретов в логах |
| Contracts | Schemas, outcomes, error envelopes, rollback до commit |
| PostgreSQL | Конкурентная идемпотентность, operation, event, dispatch, права ролей |
| Recovery | Recreate gateway/API, остановка и восстановление PostgreSQL, readiness |
| Documentation | Manifest-driven OpenAPI и доступность точных version documents |

Проверка после дедлайна использует те же опубликованные контракты на новых данных, именах actions и конкурентных interleavings. Внутренние fixtures, сценарии и система оценки не публикуются.

## Оформление решения

В корневом `README.md` сдачи создайте секцию `## Решение` с подразделами:

- `### Архитектура` — ответственность сервисов и направление вызовов;
- `### Запуск` — prerequisites, команда запуска, адрес и ожидаемый результат;
- `### Конфигурация` — используемые переменные без реальных секретов;
- `### Миграции` — когда и каким сервисом они применяются;
- `### Проверка` — `./check.sh` и, при наличии, собственные tests;
- `### Диагностика` — gateway, api, PostgreSQL, health и OpenAPI;
- `### Ограничения` — известные ограничения и технические решения.

Исходный текст задания можно оставить ниже или перенести в `TASK.md`.

## Checklist перед сдачей

- все команды выполняются из корня чистого clone;
- `docker compose up -d --build` не требует ручной настройки после запуска;
- `./check.sh` завершается успешно;
- gateway использует Compose DNS, а не `localhost`;
- readiness действительно проверяет зависимости, а не только process liveness;
- API публикует JSON OpenAPI documents, Swagger UI их не заменяет;
- конкурентные повторы создают одну operation и одно начальное событие;
- ссылки на C4 и ADR открываются из секции `Решение`;
- в Git нет `.env`, реальных секретов, `bin`, `obj`, IDE files, логов и `week-1-public-report.json`;
- куратору доступны репозиторий, ветка и полный SHA проверяемого commit.

Admission checks выполняются до функционального прогона. Они проверяют Git commit, безопасный состав репозитория, Compose interface, секцию `Решение` и воспроизводимые команды.

## Условия незачёта

- контур не запускается по README;
- основной API реализован не на C#;
- авторитетное состояние находится не в PostgreSQL;
- предметные endpoints реализованы отдельными C# controllers без action catalog;
- клиент может выбрать БД, schema, функцию или SQL;
- повтор создаёт новый предметный эффект;
- operation и event расходятся;
- новый зарегистрированный action требует пересборки API;
- в репозитории или журналах находятся реальные секреты.

## Не входит в неделю

- workflow maps, process instances и worker;
- `payment.submit` и provider-simulator;
- Outbox/Inbox, receipts и manual actions;
- lease, retries и failpoints;
- отдельные C# handlers/controllers для предметных actions;
- Consul, CORS transformations, micro-cache, rate limiting и защитные очереди gateway.

Не проектируйте эти компоненты заранее ценой усложнения action runtime.

## Нормативные источники

1. Этот README определяет границы задания, запуск и сдачу.
2. [`docs/contract-reference.md`](docs/contract-reference.md) фиксирует точные runtime contracts.
3. JSON Schema в [`contracts/course-1`](contracts/course-1) определяет точную форму machine payloads и manifest.

Если документы противоречат друг другу, сообщите куратору: это дефект задания, а не повод угадывать поведение проверки.

## Решение

Историческое описание реализации Week 1/2. Актуальный состав из десяти services,
synthetic env и команды Week 3 приведены в начале README.

### Архитектура

Решение состоит из шести сервисов Docker Compose: базового action runtime и двух workflow workers.

* `gateway` — единственная публичная точка входа. Публикует `http://localhost:8080` и проксирует разрешённые HTTP-маршруты в `api`.
* `api` — generic action runtime. Проверяет JWT, policy/scopes, версию action, JSON Schema, idempotency и вызывает единственную бизнес-точку входа PostgreSQL `api.invoke(...)`.
* `cli` — служебный интерфейс для применения миграций и управления action manifests: validate, publish, list, activate и disable.
* `worker-a` и `worker-b` — два экземпляра одного generic C# worker image. Конкурентно исполняют automatic workflow jobs через lease/fencing.
* `postgres` — хранит каталог actions, idempotency state, payment operations, operation events и autocheck evidence.

Архитектурные и контрактные материалы:

- [C4 Container diagram](docs/architecture/c4-container.md)
- [ADR-001: Trust boundary](docs/adr/001-trust-boundary.md)
- [ADR-002: Technical and domain result](docs/adr/002-technical-domain-result.md)
- [ADR-003: Workflow lease and fencing](docs/adr/003-workflow-lease-fencing.md)
- [Action manifest JSON Schema](contracts/course-1/action-manifest.schema.json)
- [Payment request JSON Schema](contracts/course-1/payment-request.payload.schema.json)
- [Operation get JSON Schema](contracts/course-1/operation-get.payload.schema.json)
- [Operation result JSON Schema](contracts/course-1/operation.result.schema.json)

Наружу публикуется только порт `gateway:8080`. `api`, `cli`, `worker-a`, `worker-b` и `postgres` не публикуют host-порты.

#### C4

Контейнерная схема:

```text
[Client]
   |
   | HTTP :8080
   v
[gateway]
   |
   | internal HTTP
   v
[api]
   |
   | PostgreSQL protocol
   v
[postgres]

[cli] --------------------> [postgres]
 migrations / publication
[worker-a] ---------------> [postgres]
[worker-b] ---------------> [postgres]
```

`gateway` не содержит бизнес-логики. `api` не выбирает SQL target из пользовательского payload: target определяется только опубликованным manifest в PostgreSQL. Доменное выполнение проходит через `api.invoke(...)`.

#### Дополнительное решение — PostgreSQL как runtime action catalog

Решение хранит опубликованные action definitions и активную версию в PostgreSQL, а не в коде `gateway` или `api`.

Это позволяет переключать default action version через `cli action activate` без пересборки и без рестарта runtime. OpenAPI также строится из опубликованного каталога и изменяется вместе с активными actions.

#### Дополнительное решение — идемпотентность внутри PostgreSQL

Идемпотентность выполняется внутри транзакции PostgreSQL в `api.invoke(...)`. Для конкурентных одинаковых запросов используется PostgreSQL advisory transaction lock.

Такой подход обеспечивает один доменный side effect даже при нескольких одновременных HTTP-запросах с одинаковым `Idempotency-Key`. Повтор с тем же payload возвращает сохранённый результат, а повтор с другим payload возвращает `idempotency.conflict`.

### Запуск

Для сборки и запуска всего решения:

```bash
docker compose up -d --build
```

Публичная точка входа:

```text
http://localhost:8080
```

Проверка liveness:

```text
GET /health/live
```

Проверка readiness:

```text
GET /health/ready
```

OpenAPI активных default actions:

```text
GET /openapi/default.json
```

Локальные проверки этапа Week 2 (подготовка fixtures описана в runbook выше):

Собственные regression tests Week 2:

    python tests/week2_regression.py


```bash
./check.sh
```

### Конфигурация

Основные переменные окружения API:

```text
COURSE_DB_CONNECTION
COURSE_JWT_ISSUER
COURSE_JWT_AUDIENCE
COURSE_JWT_SIGNING_KEY
```
Основные переменные окружения CLI:

```text
COURSE_DB_CONNECTION
COURSE_MIGRATION_DB_CONNECTION
```

В локальном Docker Compose используются:

```text
COURSE_JWT_ISSUER=moduledev-course
COURSE_JWT_AUDIENCE=moduledev-api
```

`gateway` получает внутренний адрес `api` через `API_BASE_URL`.

Runtime `api` подключается к PostgreSQL от ограниченной роли `course_runtime`. Пользовательский HTTP payload не содержит имени SQL schema/function и не может выбирать исполняемый target.

CLI использует два отдельных подключения к PostgreSQL:

- `COURSE_MIGRATION_DB_CONNECTION` — административное подключение только для `migration apply`;
- `COURSE_DB_CONNECTION` в сервисе `cli` — подключение роли `course_publication` для `action publish`, `list`, `activate` и `disable`.

Роль `course_publication` имеет доступ к action catalog, но не получает runtime-доступ к предметным таблицам. Для существующих action definitions ей разрешено изменять только операционное состояние `enabled` и `is_default`.

Сервис `api` использует отдельную ограниченную роль `course_runtime` и не имеет migration credentials.

### Миграции

SQL-миграции находятся в:

```text
db/migrations/
```

При первом старте чистого PostgreSQL они автоматически применяются через `/docker-entrypoint-initdb.d`, поэтому после:

```bash
docker compose up -d --build
```

ручное применение встроенных миграций или публикация `payment.request` / `operation.get` не требуется.

CLI также предоставляет contract-команду checksummed migration runner для
отдельного каталога дополнительных миграций, доступного внутри контейнера:

```bash
docker compose run --rm cli migration apply /path/to/additional-migrations
```

Она используется для явного применения дополнительных миграций и проверок migration contract.

Migration runner хранит checksum применённой им миграции. Повтор неизменённой
миграции, уже записанной в его history, безопасен; изменение файла приводит к
conflict. Docker init не заполняет эту history: полный `/app/db/migrations`
поверх fresh-init volume повторно запускать нельзя.

Основные объекты БД включают:

* action catalog;
* `api.resolve_action(...)`;
* `api.invoke(...)`;
* idempotency records;
* payment operations;
* operation events;
* autocheck evidence views.

### Проверка

Встроенная проверка Week 2; для Week 3 используйте внешний checker с `--repo`
из актуального runbook выше:

```bash
./check.sh
```

Дополнительные ручные проверки:

```text
http://localhost:8080/health/live
http://localhost:8080/health/ready
http://localhost:8080/openapi/default.json
```

В решении проверены:

* JWT authentication и malformed/expired tokens;
* policy/scopes;
* action version routing;
* hot switch default version без рестарта;
* disabled/unknown action;
* JSON Schema request validation;
* response contract validation;
* idempotency replay и conflict;
* конкурентные одинаковые `payment.request`;
* `payment.request` и `operation.get`;
* запрет прямой мутации runtime-проекций;
* recovery после пересоздания `api` и `gateway`;
* недоступность `postgres` и восстановление readiness.

### Диагностика

Состояние сервисов:

```bash
docker compose ps
```

Логи:

```bash
docker compose logs gateway api
```

Проверка API:

```text
GET http://localhost:8080/health/live
GET http://localhost:8080/health/ready
GET http://localhost:8080/openapi/default.json
```

Если `/health/live` отвечает `200`, а `/health/ready` не отвечает `200`, процесс `gateway` работает, но downstream dependency (`api` или `postgres`) недоступна.

Для диагностики каталога actions можно использовать:

```bash
docker compose run --rm cli action list
```

Для диагностики PostgreSQL:

```bash
docker compose exec -T postgres psql -U postgres -d course
```

### Ограничения

* Локальные credentials и signing key в Compose предназначены только для учебного окружения и не должны использоваться как production secrets.
* JWT реализован для контракта задания с HS256; production-система обычно использовала бы внешний identity provider и управляемую ротацию ключей.
* Система реализует только actions, необходимые контракту `course-1`.
* OpenAPI генерируется из опубликованного action catalog и описывает runtime-контракт, а не произвольные дополнительные HTTP endpoints.
* PostgreSQL является обязательной dependency для action runtime; при его недоступности readiness становится unhealthy, а action execution возвращает `dependency.unavailable`.

## Workflow-карты

Workflow-карты описывают versioned process definition: шаги, переходы,
automatic tasks, wait_signal, manual и end. Карта проходит schema и semantic
validation перед публикацией; запущенный process закрепляется за своей версией.

## Worker

Worker выполняет automatic steps через lease/fencing, вызывает api.invoke и
атомарно завершает workflow job. Два экземпляра worker-a и worker-b используют
один C# image и роль workflow_worker с минимальными EXECUTE-привилегиями.

### Workflow diagnostics

Start an investigation from the stable `processId`. First inspect the
public process view:

    docker compose run --rm -T cli flow get <processId>

Then correlate the durable job and attempt history in PostgreSQL:

    SELECT
        j.process_id,
        j.job_id,
        j.execution_id,
        j.state AS job_state,
        a.attempt_id,
        a.lease_version,
        a.attempt_number,
        a.status AS attempt_status,
        a.outcome,
        a.error_code
    FROM workflow.workflow_jobs AS j
    LEFT JOIN workflow.task_attempts AS a
      ON a.job_id = j.job_id
    WHERE j.process_id = '<processId>'::uuid
    ORDER BY
        j.created_at,
        a.attempt_number;

The durable workflow event sequence can be inspected independently:

    SELECT
        event_type,
        metadata,
        occurred_at
    FROM workflow.workflow_events
    WHERE process_id = '<processId>'::uuid
    ORDER BY
        occurred_at,
        event_id;

`TaskSucceeded`, `TaskRetryScheduled` and `TaskFailed` metadata includes
`processId`, `jobId`, `executionId`, `attemptId` and `leaseVersion`.
This allows an event to be matched to the exact durable attempt and
fencing token.

Worker failures are emitted as structured JSON diagnostics. Inspect them
with:

    docker compose logs --no-color worker-a worker-b

`worker.job_failure` contains `workerId`, `processId`, `jobId`,
`executionId`, `attemptId`, `leaseVersion`, `attemptNumber`, `errorCode`,
`retryable` and `exceptionType`. Raw request payloads, credentials and
raw exception messages are intentionally not part of this diagnostic
record.
