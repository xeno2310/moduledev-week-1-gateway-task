# ADR-003: Workflow lease and fencing

**Status:** Accepted

## Context

Workflow jobs выполняются несколькими одинаковыми worker instances.

Worker может завершиться после claim, во время action либо после
предметного effect, но до durable workflow transition.

Без fencing старый worker после истечения lease мог бы поздно завершить
job, уже reclaimed другим worker.

## Decision

### Lease

`workflow_job` содержит:

- `lease_owner`;
- `lease_until`;
- монотонно увеличивающийся `lease_version`.

Worker получает job только через `workflow.claim_jobs(...)`.

Каждый claim или reclaim создаёт новую `task_attempt` и увеличивает
`attempt_count`.

### Reclaim

После истечения lease job может получить другой worker.

Предыдущая незавершённая attempt становится `STALE` с причиной
`workflow.lease_expired`.

Stale reclaim увеличивает `attempt_count`, но не `failure_count`,
поэтому не расходует retry budget action.

### Fencing

`lease_version` используется как fencing token.

`workflow.finish_job(...)` и `workflow.fail_job(...)` до изменения
durable state проверяют:

1. job всё ещё имеет состояние `LEASED`;
2. `lease_owner` совпадает;
3. `lease_version` совпадает;
4. соответствующая attempt всё ещё имеет состояние `RUNNING`.

Иначе возвращается `workflow.lease_stale`.

Поздний результат worker со старой lease поэтому не может завершить
reclaimed job.

### Successful transaction

Успешный путь worker:

1. начинает Npgsql transaction;
2. вызывает `api.invoke(...)`;
3. проверяет envelope, outcome и response schema;
4. вызывает `workflow.finish_job(...)`;
5. выполняет commit.

Action effect и успешный workflow transition фиксируются одной
PostgreSQL transaction.

### Failure path

При retryable/non-retryable action error либо runtime timeout
transaction action откатывается.

После rollback worker отдельно вызывает `workflow.fail_job(...)`.

`failure_count` считает action failures, но не stale lease reclaims.

### Retry budget

`max_attempts` включает первое фактическое выполнение action.

`delays_ms` содержит `max_attempts - 1` задержек.

Retry выполняется для retryable action errors и runtime timeout.

Mapping error, unknown outcome и response contract violation являются
non-retryable.

### Heartbeat

Обязательный heartbeat не используется.

Ограниченная по времени lease и reclaim достаточны для текущего
workflow protocol.

### Test profile and failpoints

Test profile использует короткую lease и частый polling.

Поддерживаются детерминированные failpoints:

- `after_job_claim`;
- `after_action_before_finish`.

После acknowledgement `failpoint.reached` worker блокируется до
остановки container.

Это позволяет воспроизводимо тестировать crash windows.

## Consequences

Преимущества:

- несколько workers безопасно конкурируют за jobs;
- падение worker не блокирует job навсегда;
- stale owner не может завершить reclaimed job;
- stale reclaim не расходует retry budget;
- action effect и workflow transition атомарны.

Цена:

- требуются отдельные job/attempt records;
- finish/fail должны передавать fencing token;
- необходима логика reclaim и состояния `STALE`.

## Alternatives considered

### Держать database lock во время всего action

Отклонено из-за плохой concurrency и recovery.

### Использовать только `lease_owner`

Отклонено: новый claim тем же owner нельзя отличить от старой lease.

### Action и `finish_job` в разных transactions

Отклонено: crash между transactions оставляет возможный action effect
без durable workflow transition.

### Обязательный heartbeat

Не выбран для Week 2: он добавляет отдельный renewal protocol и новые
race conditions без необходимости для обязательного контракта.