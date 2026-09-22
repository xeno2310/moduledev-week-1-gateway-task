# Неделя 1. Database-first action runtime

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

Полная локальная проверка задания:

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

CLI также предоставляет contract-команду checksummed migration runner:

```bash
docker compose run --rm cli migration apply /app/db/migrations
```

Она используется для явного применения дополнительных миграций и проверок migration contract.

Migration runner хранит checksum применённой миграции. Повторное применение неизменённой миграции безопасно, а изменение уже применённого файла приводит к conflict.

Основные объекты БД включают:

* action catalog;
* `api.resolve_action(...)`;
* `api.invoke(...)`;
* idempotency records;
* payment operations;
* operation events;
* autocheck evidence views.

### Проверка

Основная проверка:

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
