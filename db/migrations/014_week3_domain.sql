SET ROLE course_owner;

CREATE SCHEMA IF NOT EXISTS delivery
    AUTHORIZATION course_owner;

CREATE SCHEMA IF NOT EXISTS receipt
    AUTHORIZATION course_owner;


-- ============================================================
-- Server-side operation kind -> workflow binding
-- ============================================================

CREATE TABLE payment.operation_flow_bindings
(
    operation_kind text PRIMARY KEY,
    flow_name      text NOT NULL,

    CHECK (
        (operation_kind = 'PAYMENT_EXECUTION'
         AND flow_name = 'payment-processing')
        OR
        (operation_kind = 'PAYMENT_APPROVAL'
         AND flow_name = 'payment-review')
    )
);

INSERT INTO payment.operation_flow_bindings
(
    operation_kind,
    flow_name
)
VALUES
    ('PAYMENT_EXECUTION', 'payment-processing'),
    ('PAYMENT_APPROVAL', 'payment-review');


-- Every submitted operation is pinned to at most one workflow process.

ALTER TABLE payment.operations
ADD CONSTRAINT payment_operations_process_fk
FOREIGN KEY (process_id)
REFERENCES workflow.process_instances(process_id);

CREATE UNIQUE INDEX payment_operations_process_uidx
ON payment.operations(process_id)
WHERE process_id IS NOT NULL;


-- ============================================================
-- External payment request
-- ============================================================

CREATE TABLE payment.external_requests
(
    external_request_id text        PRIMARY KEY,

    operation_id        uuid        NOT NULL
        REFERENCES payment.operations(operation_id),

    process_id          uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    execution_id        uuid        NOT NULL,
    correlation_id      uuid        NOT NULL,

    amount              numeric(18,2) NOT NULL,
    currency            text        NOT NULL,

    state               text        NOT NULL DEFAULT 'CREATED',
    payload_hash        text        NOT NULL,

    provider_payment_id text,

    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    UNIQUE (operation_id),
    UNIQUE (process_id),
    UNIQUE (execution_id),

    CHECK (length(external_request_id) BETWEEN 1 AND 128),
    CHECK (position(chr(10) IN external_request_id) = 0),
    CHECK (position(chr(13) IN external_request_id) = 0),

    CHECK (
        state IN (
            'CREATED',
            'SENT',
            'CONFIRMED'
        )
    ),

    CHECK (currency = 'RUB'),
    CHECK (payload_hash ~ '^[0-9a-f]{64}$')
);


-- ============================================================
-- Durable Outbox
-- ============================================================

CREATE TABLE delivery.outbox
(
    outbox_id            uuid        PRIMARY KEY,

    external_request_id  text        NOT NULL UNIQUE
        REFERENCES payment.external_requests(external_request_id),

    state                text        NOT NULL DEFAULT 'PENDING',

    attempt_count        integer     NOT NULL DEFAULT 0,

    lease_owner          text,
    lease_version        bigint      NOT NULL DEFAULT 0,
    lease_until          timestamptz,

    next_attempt_at      timestamptz NOT NULL DEFAULT now(),
    last_error_code      text,

    provider_payment_id  text,

    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    delivered_at         timestamptz,

    CHECK (
        state IN (
            'PENDING',
            'LEASED',
            'RETRY_WAIT',
            'DELIVERED',
            'DEAD',
            'CONFIRMED'
        )
    ),

    CHECK (attempt_count >= 0),
    CHECK (lease_version >= 0)
);

CREATE INDEX delivery_outbox_claim_idx
ON delivery.outbox
(
    state,
    next_attempt_at,
    lease_until
);


-- ============================================================
-- Durable Inbox
-- ============================================================

CREATE TABLE delivery.inbox
(
    message_id           text        PRIMARY KEY,

    external_request_id  text        NOT NULL
        REFERENCES payment.external_requests(external_request_id),

    body_hash            text        NOT NULL,

    state                text        NOT NULL DEFAULT 'RECEIVED',

    received_at          timestamptz NOT NULL DEFAULT now(),
    applied_at           timestamptz,

    CHECK (length(message_id) BETWEEN 1 AND 128),
    CHECK (position(chr(10) IN message_id) = 0),
    CHECK (position(chr(13) IN message_id) = 0),

    CHECK (
        state IN (
            'RECEIVED',
            'APPLIED',
            'CONFLICT'
        )
    ),

    CHECK (body_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX delivery_inbox_reconcile_idx
ON delivery.inbox(state, received_at);


-- ============================================================
-- Normalized receipt evidence
-- ============================================================

CREATE TABLE payment.receipts
(
    message_id           text        PRIMARY KEY
        REFERENCES delivery.inbox(message_id),

    external_request_id  text        NOT NULL
        REFERENCES payment.external_requests(external_request_id),

    message_version      integer     NOT NULL,

    outcome              text        NOT NULL,

    provider_payment_id  text        NOT NULL,

    occurred_at_text     text        NOT NULL,

    signature_valid      boolean     NOT NULL,
    body_hash            text        NOT NULL,

    received_at          timestamptz NOT NULL DEFAULT now(),
    applied_at           timestamptz,

    CHECK (message_version = 1),

    CHECK (
        outcome IN (
            'COMPLETED',
            'REJECTED'
        )
    ),

    CHECK (provider_payment_id = message_id),
    CHECK (body_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX payment_receipts_external_idx
ON payment.receipts(external_request_id);


-- ============================================================
-- Automatic/manual decision audit
-- ============================================================

CREATE TABLE payment.decisions
(
    decision_id          uuid        PRIMARY KEY,

    process_id           uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    step_instance_id     uuid        NOT NULL
        REFERENCES workflow.step_instances(step_instance_id),

    source               text        NOT NULL,

    principal            text        NOT NULL,

    reason_hash          text,

    outcome              text        NOT NULL,

    rule_version         text,

    created_at           timestamptz NOT NULL DEFAULT now(),

    UNIQUE (step_instance_id),

    CHECK (
        source IN (
            'LIMIT_RULE',
            'MANUAL'
        )
    ),

    CHECK (
        outcome IN (
            'APPROVED',
            'REJECTED'
        )
    ),

    CHECK (
        (
            source = 'LIMIT_RULE'
            AND rule_version = 'course-limit-v1'
            AND reason_hash IS NULL
        )
        OR
        (
            source = 'MANUAL'
            AND rule_version IS NULL
            AND reason_hash ~ '^[0-9a-f]{64}$'
        )
    )
);


-- ============================================================
-- Stable Week 3 checker views
-- ============================================================

CREATE OR REPLACE VIEW autocheck.external_requests AS
SELECT
    external_request_id,
    operation_id,
    state,
    payload_hash,
    created_at
FROM payment.external_requests;


CREATE OR REPLACE VIEW autocheck.receipts AS
SELECT
    message_id,
    external_request_id,
    message_version,
    outcome,
    signature_valid,
    body_hash,
    received_at,
    applied_at
FROM payment.receipts;


CREATE OR REPLACE VIEW autocheck.outbox AS
SELECT
    outbox_id,
    external_request_id,
    state,
    attempt_count,
    next_attempt_at,
    last_error_code,
    created_at,
    delivered_at
FROM delivery.outbox;


CREATE OR REPLACE VIEW autocheck.inbox AS
SELECT
    message_id,
    body_hash,
    state,
    received_at,
    applied_at
FROM delivery.inbox;


CREATE OR REPLACE VIEW autocheck.decisions AS
SELECT
    decision_id,
    process_id,
    step_instance_id,
    source,
    principal,
    reason_hash,
    outcome,
    rule_version,
    created_at
FROM payment.decisions;


-- ============================================================
-- Physical tables are never public integration boundaries
-- ============================================================

REVOKE ALL ON payment.operation_flow_bindings FROM PUBLIC;
REVOKE ALL ON payment.external_requests FROM PUBLIC;
REVOKE ALL ON payment.receipts FROM PUBLIC;
REVOKE ALL ON payment.decisions FROM PUBLIC;

REVOKE ALL ON delivery.outbox FROM PUBLIC;
REVOKE ALL ON delivery.inbox FROM PUBLIC;


RESET ROLE;
