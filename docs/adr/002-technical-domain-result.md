# ADR-002: Separation of technical and domain result

**Status:** Accepted

## Context

Выполнение action имеет два разных вида результата.

Технический результат описывает, смог ли runtime корректно разрешить и выполнить action по контракту: authentication, authorization, schema validation, timeout, idempotency, database availability и соответствие возвращённого значения manifest.

Предметный результат создаётся зарегистрированной PostgreSQL target-функцией и выражается через объявленный `outcome` и schema-valid `result`.

Если смешать эти два уровня, target сможет фактически определять HTTP semantics или runtime сможет ошибочно commit-ить некорректный предметный результат.

## Decision

Target-функция возвращает единый JSON envelope через `api.invoke(...)`.

Для успешного предметного выполнения target возвращает `status = "ok"`, объявленный manifest `outcome` и `result`.

После вызова target C# runtime повторно проверяет envelope, допустимость `outcome` и соответствие `result` response schema. Transaction commit выполняется только после прохождения этих проверок.

Если target возвращает контролируемый `status = "error"`, неизвестный outcome или result, нарушающий response schema, transaction откатывается.

HTTP status и технический error `code` определяются runtime-контрактом. Пользовательский payload и предметная target-функция не выбирают HTTP status, SQL target или техническую классификацию ошибки.

Предметные эффекты остаются внутри той же PostgreSQL transaction, что и вызов `api.invoke(...)`, поэтому техническое нарушение контракта не может оставить частично зафиксированное предметное состояние.

## Consequences

Runtime может гарантировать одинаковые технические semantics для любых опубликованных actions.

Предметная функция отвечает за бизнес-результат, но не управляет транспортным контрактом.

Неизвестный outcome или invalid result становится `action.contract_violation` и приводит к rollback.

Контролируемая предметная ошибка также не фиксирует изменения, выполненные target в текущей transaction.

Новый action может быть добавлен через manifest и PostgreSQL function без отдельного C# controller или handler.
