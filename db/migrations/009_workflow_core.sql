SET ROLE course_owner;

CREATE SCHEMA IF NOT EXISTS workflow
    AUTHORIZATION course_owner;

-- ============================================================
-- Published workflow definitions
-- ============================================================

CREATE TABLE workflow.flow_definitions
(
    flow_name       text        PRIMARY KEY,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE workflow.flow_versions
(
    flow_name       text        NOT NULL
        REFERENCES workflow.flow_definitions(flow_name),

    flow_version    integer     NOT NULL,
    status          text        NOT NULL DEFAULT 'PUBLISHED',
    is_active       boolean     NOT NULL DEFAULT false,

    start_step      text        NOT NULL,

    source_hash     text        NOT NULL,
    source_document jsonb       NOT NULL,

    published_at    timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (flow_name, flow_version),

    CHECK (flow_version > 0),
    CHECK (status = 'PUBLISHED'),
    CHECK (source_hash ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX workflow_one_active_version
ON workflow.flow_versions(flow_name)
WHERE is_active;

CREATE TABLE workflow.step_definitions
(
    flow_name        text    NOT NULL,
    flow_version     integer NOT NULL,

    step_key         text    NOT NULL,
    step_type        text    NOT NULL,

    signal_type      text,
    declared_outcome text,
    allowed_outcomes jsonb,

    PRIMARY KEY (flow_name, flow_version, step_key),

    FOREIGN KEY (flow_name, flow_version)
        REFERENCES workflow.flow_versions(flow_name, flow_version),

    CHECK (
        step_type IN (
            'AUTOMATIC',
            'WAIT_SIGNAL',
            'MANUAL',
            'END'
        )
    )
);

CREATE TABLE workflow.task_definitions
(
    flow_name        text    NOT NULL,
    flow_version     integer NOT NULL,
    step_key         text    NOT NULL,

    service          text    NOT NULL,
    module           text    NOT NULL,
    action           text    NOT NULL,
    action_version   integer NOT NULL,

    required_policy  jsonb   NOT NULL,

    timeout_ms       integer NOT NULL,
    max_attempts     integer NOT NULL,
    delays_ms        jsonb   NOT NULL,

    input_mapping    jsonb   NOT NULL,
    input_constants  jsonb   NOT NULL,

    PRIMARY KEY (flow_name, flow_version, step_key),

    FOREIGN KEY (flow_name, flow_version, step_key)
        REFERENCES workflow.step_definitions(
            flow_name,
            flow_version,
            step_key
        ),

    CHECK (service = 'postgres'),
    CHECK (action_version > 0),
    CHECK (timeout_ms BETWEEN 1 AND 30000),
    CHECK (max_attempts BETWEEN 1 AND 10)
);

CREATE TABLE workflow.transition_definitions
(
    flow_name       text    NOT NULL,
    flow_version    integer NOT NULL,

    from_step       text    NOT NULL,
    outcome         text    NOT NULL,
    to_step         text    NOT NULL,

    PRIMARY KEY (
        flow_name,
        flow_version,
        from_step,
        outcome
    ),

    FOREIGN KEY (flow_name, flow_version, from_step)
        REFERENCES workflow.step_definitions(
            flow_name,
            flow_version,
            step_key
        ),

    FOREIGN KEY (flow_name, flow_version, to_step)
        REFERENCES workflow.step_definitions(
            flow_name,
            flow_version,
            step_key
        )
);

-- ============================================================
-- Runtime workflow state
-- ============================================================

CREATE TABLE workflow.process_instances
(
    process_id        uuid        PRIMARY KEY,

    business_key      text        NOT NULL,

    flow_name         text        NOT NULL,
    flow_version      integer     NOT NULL,

    process_data      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    process_data_hash text        NOT NULL,

    state             text        NOT NULL,
    current_step_key  text,

    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    UNIQUE (flow_name, business_key),

    FOREIGN KEY (flow_name, flow_version)
        REFERENCES workflow.flow_versions(flow_name, flow_version),

    CHECK (
        state IN (
            'CREATED',
            'RUNNING',
            'WAITING_SIGNAL',
            'WAITING_MANUAL',
            'COMPLETED',
            'FAILED'
        )
    ),

    CHECK (process_data_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE workflow.step_instances
(
    step_instance_id uuid        PRIMARY KEY,
    process_id       uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    step_key         text        NOT NULL,
    step_type        text        NOT NULL,
    state            text        NOT NULL,

    outcome          text,

    entered_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,

    CHECK (
        step_type IN (
            'AUTOMATIC',
            'WAIT_SIGNAL',
            'MANUAL',
            'END'
        )
    ),

    CHECK (
        state IN (
            'PENDING',
            'READY',
            'RUNNING',
            'WAITING',
            'COMPLETED',
            'FAILED'
        )
    )
);

CREATE TABLE workflow.workflow_jobs
(
    job_id            uuid        PRIMARY KEY,
    process_id        uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    step_instance_id  uuid        NOT NULL
        REFERENCES workflow.step_instances(step_instance_id),

    execution_id      uuid        NOT NULL,

    state             text        NOT NULL,

    lease_owner       text,
    lease_version     bigint      NOT NULL DEFAULT 0,
    lease_until       timestamptz,

    attempt_count     integer     NOT NULL DEFAULT 0,

    -- В отличие от attempt_count не увеличивается при stale lease.
    failure_count     integer     NOT NULL DEFAULT 0,

    next_attempt_at   timestamptz,

    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),

    UNIQUE (step_instance_id),

    CHECK (
        state IN (
            'READY',
            'LEASED',
            'RETRY_WAIT',
            'SUCCEEDED',
            'DEAD'
        )
    ),

    CHECK (lease_version >= 0),
    CHECK (attempt_count >= 0),
    CHECK (failure_count >= 0)
);

CREATE INDEX workflow_jobs_claim_idx
ON workflow.workflow_jobs(state, next_attempt_at, lease_until);

CREATE TABLE workflow.task_attempts
(
    attempt_id      uuid        PRIMARY KEY,

    job_id          uuid        NOT NULL
        REFERENCES workflow.workflow_jobs(job_id),

    execution_id    uuid        NOT NULL,

    lease_version   bigint      NOT NULL,
    attempt_number  integer     NOT NULL,

    status          text        NOT NULL,

    outcome         text,
    error_code      text,

    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,

    UNIQUE (job_id, attempt_number),

    CHECK (
        status IN (
            'RUNNING',
            'SUCCEEDED',
            'FAILED',
            'STALE'
        )
    ),

    CHECK (attempt_number > 0),
    CHECK (lease_version > 0)
);

CREATE TABLE workflow.workflow_signals
(
    message_id       text        PRIMARY KEY,

    process_id       uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    signal_type      text        NOT NULL,

    body              jsonb       NOT NULL,
    body_hash         text        NOT NULL,

    status            text        NOT NULL,

    received_at       timestamptz NOT NULL DEFAULT now(),
    applied_at        timestamptz,

    CHECK (
        status IN (
            'ACCEPTED',
            'APPLIED'
        )
    ),

    CHECK (body_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE workflow.workflow_events
(
    event_id          uuid        PRIMARY KEY,

    process_id        uuid        NOT NULL
        REFERENCES workflow.process_instances(process_id),

    step_instance_id  uuid
        REFERENCES workflow.step_instances(step_instance_id),

    event_type        text        NOT NULL,
    metadata          jsonb       NOT NULL DEFAULT '{}'::jsonb,

    occurred_at       timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- Stable checker evidence views
-- ============================================================

CREATE OR REPLACE VIEW autocheck.flow_versions AS
SELECT
    flow_name,
    flow_version,
    status,
    is_active,
    published_at
FROM workflow.flow_versions;

CREATE OR REPLACE VIEW autocheck.processes AS
SELECT
    process_id,
    business_key,
    flow_name,
    flow_version,
    state,
    current_step_key,
    created_at,
    updated_at
FROM workflow.process_instances;

CREATE OR REPLACE VIEW autocheck.steps AS
SELECT
    step_instance_id,
    process_id,
    step_key,
    step_type,
    state,
    outcome,
    entered_at,
    completed_at
FROM workflow.step_instances;

CREATE OR REPLACE VIEW autocheck.jobs AS
SELECT
    job_id,
    process_id,
    step_instance_id,
    execution_id,
    state,
    lease_owner,
    lease_version,
    lease_until,
    attempt_count,
    next_attempt_at
FROM workflow.workflow_jobs;

CREATE OR REPLACE VIEW autocheck.attempts AS
SELECT
    attempt_id,
    job_id,
    execution_id,
    lease_version,
    attempt_number,
    status,
    outcome,
    error_code,
    started_at,
    finished_at
FROM workflow.task_attempts;

CREATE OR REPLACE VIEW autocheck.signals AS
SELECT
    message_id,
    process_id,
    signal_type,
    body_hash,
    status,
    received_at
FROM workflow.workflow_signals;

CREATE OR REPLACE VIEW autocheck.workflow_events AS
SELECT
    event_id,
    process_id,
    step_instance_id,
    event_type,
    occurred_at
FROM workflow.workflow_events;

RESET ROLE;