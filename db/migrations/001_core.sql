CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- Application roles
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'course_owner'
    ) THEN
        CREATE ROLE course_owner NOLOGIN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'course_runtime'
    ) THEN
        CREATE ROLE course_runtime
            LOGIN
            PASSWORD 'course-runtime-local-only';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'course_publication'
    ) THEN
        CREATE ROLE course_publication
            LOGIN
            PASSWORD 'course-publication-local-only';
    END IF;
END
$$;

GRANT CREATE ON DATABASE course TO course_owner;

SET ROLE course_owner;

-- ============================================================
-- Schemas
-- ============================================================

CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS api;
CREATE SCHEMA IF NOT EXISTS payment;
CREATE SCHEMA IF NOT EXISTS autocheck;

-- ============================================================
-- Immutable action catalog
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog.action_definitions
(
    module              text        NOT NULL,
    action              text        NOT NULL,
    version             integer     NOT NULL CHECK (version >= 1),

    contract_version    text        NOT NULL,
    http_method         text        NOT NULL,

    target_schema       text        NOT NULL,
    target_function     text        NOT NULL,

    request_schema      jsonb       NOT NULL,
    response_schema     jsonb       NOT NULL,

    outcomes            jsonb       NOT NULL,
    required_policy     jsonb       NOT NULL,

    idempotency_mode    text        NOT NULL,
    idempotency_scope   text        NOT NULL,

    timeout_ms          integer     NOT NULL,

    enabled             boolean     NOT NULL,
    is_default          boolean     NOT NULL,

    manifest_hash       text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (module, action, version),

    CHECK (http_method = 'POST'),
    CHECK (contract_version = 'course-1'),
    CHECK (timeout_ms BETWEEN 1 AND 30000),

    CHECK (
        idempotency_mode IN (
            'none',
            'optional',
            'required'
        )
    ),

    CHECK (
        idempotency_scope IN (
            'none',
            'principal_action',
            'consumer_action',
            'global_action'
        )
    ),

    CHECK (
        NOT is_default OR enabled
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS
    ux_action_default
ON catalog.action_definitions(module, action)
WHERE enabled = true
  AND is_default = true;

-- ============================================================
-- Dispatch evidence
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog.action_dispatches
(
    correlation_id  uuid        NOT NULL,
    request_id      text,
    module          text        NOT NULL,
    action          text        NOT NULL,
    version         integer     NOT NULL,

    principal       text        NOT NULL,

    payload_hash    text        NOT NULL,

    status          text        NOT NULL,
    outcome         text,

    occurred_at     timestamptz NOT NULL DEFAULT now(),

    CHECK (status IN ('OK', 'ERROR')),
    CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

-- ============================================================
-- Payment domain
-- ============================================================

CREATE TABLE IF NOT EXISTS payment.operations
(
    operation_id    uuid        PRIMARY KEY,
    request_id      text        NOT NULL,

    operation_kind  text        NOT NULL,

    amount          numeric(18,2) NOT NULL,
    currency        text        NOT NULL,

    status          text        NOT NULL,

    process_id      uuid,

    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CHECK (
        operation_kind IN (
            'PAYMENT_EXECUTION',
            'PAYMENT_APPROVAL'
        )
    ),

    CHECK (
        status IN (
            'CREATED',
            'PROCESSING',
            'COMPLETED',
            'REJECTED'
        )
    ),

    CHECK (currency = 'RUB')
);

CREATE TABLE IF NOT EXISTS payment.operation_events
(
    event_id        uuid        PRIMARY KEY,
    operation_id    uuid        NOT NULL
        REFERENCES payment.operations(operation_id),

    event_type      text        NOT NULL,

    payload_hash    text        NOT NULL,

    occurred_at     timestamptz NOT NULL DEFAULT now(),

    CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

-- ============================================================
-- Idempotency
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog.idempotency_records
(
    scope_key       text        NOT NULL,
    module          text        NOT NULL,
    action          text        NOT NULL,
    version         integer     NOT NULL,

    request_id      text        NOT NULL,
    payload_hash    text        NOT NULL,

    response        jsonb,

    created_at      timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (
        scope_key,
        module,
        action,
        version,
        request_id
    ),

    CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);

-- ============================================================
-- Contract evidence
-- ============================================================

CREATE TABLE IF NOT EXISTS catalog.contract_info
(
    contract_version text        PRIMARY KEY,
    generated_at     timestamptz NOT NULL DEFAULT now()
);

INSERT INTO catalog.contract_info(contract_version)
VALUES ('course-1')
ON CONFLICT (contract_version) DO NOTHING;

-- ============================================================
-- Autocheck views
-- ============================================================

CREATE OR REPLACE VIEW autocheck.contract_info AS
SELECT
    contract_version,
    generated_at
FROM catalog.contract_info;

CREATE OR REPLACE VIEW autocheck.action_definitions AS
SELECT
    module,
    action,
    version,
    http_method,
    target_schema,
    target_function,
    outcomes,
    enabled,
    is_default
FROM catalog.action_definitions;

CREATE OR REPLACE VIEW autocheck.action_dispatches AS
SELECT
    correlation_id,
    request_id,
    module,
    action,
    version,
    principal,
    payload_hash,
    status,
    outcome,
    occurred_at
FROM catalog.action_dispatches;

CREATE OR REPLACE VIEW autocheck.operations AS
SELECT
    operation_id,
    request_id,
    operation_kind,
    amount,
    currency,
    status,
    process_id,
    created_at,
    updated_at
FROM payment.operations;

CREATE OR REPLACE VIEW autocheck.operation_events AS
SELECT
    event_id,
    operation_id,
    event_type,
    payload_hash,
    occurred_at
FROM payment.operation_events;

RESET ROLE;
-- ============================================================
-- Permissions
-- ============================================================

REVOKE ALL
ON ALL TABLES IN SCHEMA catalog
FROM course_runtime;

REVOKE ALL
ON ALL TABLES IN SCHEMA payment
FROM course_runtime;

GRANT USAGE ON SCHEMA api TO course_runtime;
GRANT USAGE ON SCHEMA autocheck TO course_runtime;

GRANT SELECT
ON ALL TABLES IN SCHEMA autocheck
TO course_runtime;