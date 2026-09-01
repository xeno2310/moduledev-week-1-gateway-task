# ADR-001: Trust boundary

**Status:** Accepted

## Context

HTTP-клиент является недоверенной стороной. Он может передать JWT, route, headers и произвольный JSON payload, но не должен иметь возможности подменить principal, policy, SQL target, server correlation data или предметный результат.

Runtime одновременно использует данные из JWT, action manifest и HTTP payload, поэтому необходимо явно определить границу доверия.

## Decision

Доверенный context создаётся только внутри C# `api` после успешной проверки JWT.

`principal`, `consumer` и `scopes` извлекаются только из проверенных claims. Обязательность claims, их JSON-типы, issuer, audience, lifetime и HS256 signature проверяются до выполнения action.

`correlationId` и `deadline` создаются runtime. `requestId` формируется из `Idempotency-Key`, когда он предусмотрен контрактом.

Поля пользовательского payload не могут изменять trusted context.

Клиент выбирает только `{module}/{action}` и, при необходимости, опубликованную версию через `X-Action-Version`. `target_schema`, `target_function`, required policy и schemas разрешаются только из PostgreSQL action catalog.

Policy проверяется дважды: на HTTP-границе и внутри `api.invoke(...)`.

Runtime PostgreSQL role может выполнять доверенную точку входа, но не получает прямого DML к предметным таблицам и прямого доступа к target-функциям. `SECURITY DEFINER` функции принадлежат `NOLOGIN` owner.

## Consequences

Компрометация или подделка payload сама по себе не позволяет выбрать SQL target или повысить privileges.

Action можно опубликовать без изменения C# runtime, поскольку C# не содержит hardcoded mapping предметных actions.

Ошибки разрешения и исполнения не должны раскрывать connection string, SQL, stack trace, JWT или внутренние target names.

Дополнительная сложность состоит в повторной проверке policy в PostgreSQL и явном управлении database privileges, но эта сложность сохраняет trust boundary независимо от HTTP-слоя.
