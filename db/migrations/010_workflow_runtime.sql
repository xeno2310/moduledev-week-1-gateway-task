SET ROLE course_owner;

-- ============================================================
-- Internal state-machine helper
-- ============================================================

CREATE OR REPLACE FUNCTION workflow._enter_step(
    p_process_id uuid,
    p_step_key text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow, catalog
AS $$
DECLARE
    v_process          workflow.process_instances%ROWTYPE;
    v_step             workflow.step_definitions%ROWTYPE;
    v_signal           workflow.workflow_signals%ROWTYPE;

    v_step_instance_id uuid;
    v_job_id           uuid;
    v_execution_id     uuid;

    v_next_step        text;
BEGIN
    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = p_process_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'workflow process does not exist';
    END IF;

    SELECT *
    INTO v_step
    FROM workflow.step_definitions
    WHERE flow_name = v_process.flow_name
      AND flow_version = v_process.flow_version
      AND step_key = p_step_key;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'workflow step does not exist';
    END IF;

    v_step_instance_id := gen_random_uuid();

    -- --------------------------------------------------------
    -- Automatic
    -- --------------------------------------------------------

    IF v_step.step_type = 'AUTOMATIC' THEN
        INSERT INTO workflow.step_instances
        (
            step_instance_id,
            process_id,
            step_key,
            step_type,
            state
        )
        VALUES
        (
            v_step_instance_id,
            p_process_id,
            p_step_key,
            'AUTOMATIC',
            'READY'
        );

        UPDATE workflow.process_instances
        SET
            state = 'RUNNING',
            current_step_key = p_step_key,
            updated_at = clock_timestamp()
        WHERE process_id = p_process_id;

        v_job_id := gen_random_uuid();
        v_execution_id := gen_random_uuid();

        INSERT INTO workflow.workflow_jobs
        (
            job_id,
            process_id,
            step_instance_id,
            execution_id,
            state,
            next_attempt_at
        )
        VALUES
        (
            v_job_id,
            p_process_id,
            v_step_instance_id,
            v_execution_id,
            'READY',
            clock_timestamp()
        );

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type,
            metadata
        )
        VALUES
        (
            gen_random_uuid(),
            p_process_id,
            v_step_instance_id,
            'JobReady',
            jsonb_build_object(
                'jobId', v_job_id,
                'executionId', v_execution_id
            )
        );

        RETURN;
    END IF;

    -- --------------------------------------------------------
    -- Wait signal
    -- --------------------------------------------------------

    IF v_step.step_type = 'WAIT_SIGNAL' THEN
        INSERT INTO workflow.step_instances
        (
            step_instance_id,
            process_id,
            step_key,
            step_type,
            state
        )
        VALUES
        (
            v_step_instance_id,
            p_process_id,
            p_step_key,
            'WAIT_SIGNAL',
            'WAITING'
        );

        UPDATE workflow.process_instances
        SET
            state = 'WAITING_SIGNAL',
            current_step_key = p_step_key,
            updated_at = clock_timestamp()
        WHERE process_id = p_process_id;

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type
        )
        VALUES
        (
            gen_random_uuid(),
            p_process_id,
            v_step_instance_id,
            'SignalWaiting'
        );

        -- Early signal may already have been accepted.
        SELECT *
        INTO v_signal
        FROM workflow.workflow_signals
        WHERE process_id = p_process_id
          AND signal_type = v_step.signal_type
          AND status = 'ACCEPTED'
        ORDER BY received_at, message_id
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
            UPDATE workflow.workflow_signals
            SET
                status = 'APPLIED',
                applied_at = clock_timestamp()
            WHERE message_id = v_signal.message_id;

            UPDATE workflow.step_instances
            SET
                state = 'COMPLETED',
                outcome = v_step.declared_outcome,
                completed_at = clock_timestamp()
            WHERE step_instance_id = v_step_instance_id;

            INSERT INTO workflow.workflow_events
            (
                event_id,
                process_id,
                step_instance_id,
                event_type,
                metadata
            )
            VALUES
            (
                gen_random_uuid(),
                p_process_id,
                v_step_instance_id,
                'SignalApplied',
                jsonb_build_object(
                    'messageId', v_signal.message_id,
                    'signalType', v_signal.signal_type
                )
            );

            v_next_step := NULL;

            SELECT to_step
            INTO v_next_step
            FROM workflow.transition_definitions
            WHERE flow_name = v_process.flow_name
              AND flow_version = v_process.flow_version
              AND from_step = p_step_key
              AND outcome = v_step.declared_outcome;

            IF v_next_step IS NULL THEN
                RAISE EXCEPTION 'workflow transition does not exist';
            END IF;

            PERFORM workflow._enter_step(
                p_process_id,
                v_next_step
            );
        END IF;

        RETURN;
    END IF;

    -- --------------------------------------------------------
    -- Manual
    -- --------------------------------------------------------

    IF v_step.step_type = 'MANUAL' THEN
        INSERT INTO workflow.step_instances
        (
            step_instance_id,
            process_id,
            step_key,
            step_type,
            state
        )
        VALUES
        (
            v_step_instance_id,
            p_process_id,
            p_step_key,
            'MANUAL',
            'WAITING'
        );

        UPDATE workflow.process_instances
        SET
            state = 'WAITING_MANUAL',
            current_step_key = p_step_key,
            updated_at = clock_timestamp()
        WHERE process_id = p_process_id;

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type
        )
        VALUES
        (
            gen_random_uuid(),
            p_process_id,
            v_step_instance_id,
            'ManualWaiting'
        );

        RETURN;
    END IF;

    -- --------------------------------------------------------
    -- End
    -- --------------------------------------------------------

    IF v_step.step_type = 'END' THEN
        INSERT INTO workflow.step_instances
        (
            step_instance_id,
            process_id,
            step_key,
            step_type,
            state,
            outcome,
            completed_at
        )
        VALUES
        (
            v_step_instance_id,
            p_process_id,
            p_step_key,
            'END',
            'COMPLETED',
            v_step.declared_outcome,
            clock_timestamp()
        );

        UPDATE workflow.process_instances
        SET
            state = 'COMPLETED',
            current_step_key = p_step_key,
            updated_at = clock_timestamp()
        WHERE process_id = p_process_id;

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type,
            metadata
        )
        VALUES
        (
            gen_random_uuid(),
            p_process_id,
            v_step_instance_id,
            'ProcessCompleted',
            jsonb_build_object(
                'outcome', v_step.declared_outcome
            )
        );

        RETURN;
    END IF;

    RAISE EXCEPTION 'unsupported workflow step type';
END
$$;


-- ============================================================
-- Start process
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.start_process(
    p_flow_name text,
    p_business_key text,
    p_data jsonb,
    p_data_hash text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_existing workflow.process_instances%ROWTYPE;

    v_flow_version integer;
    v_start_step text;

    v_process_id uuid;
BEGIN
    -- Serializes starts of the same logical process, including
    -- the "row does not exist yet" case.
    PERFORM pg_advisory_xact_lock(
        hashtextextended(
            p_flow_name || chr(31) || p_business_key,
            0
        )
    );

    SELECT *
    INTO v_existing
    FROM workflow.process_instances
    WHERE flow_name = p_flow_name
      AND business_key = p_business_key
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.process_data_hash <> p_data_hash THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'workflow.start_conflict',
                'message', 'business key already exists with different data',
                'retryable', false,
                'meta', jsonb_build_object(
                    'contractVersion', 'course-1'
                )
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'ok',
            'result', jsonb_build_object(
                'resource', 'process',
                'operation', 'started',
                'processId', v_existing.process_id,
                'flowName', v_existing.flow_name,
                'flowVersion', v_existing.flow_version,
                'state', v_existing.state
            ),
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    SELECT
        flow_version,
        start_step
    INTO
        v_flow_version,
        v_start_step
    FROM workflow.flow_versions
    WHERE flow_name = p_flow_name
      AND status = 'PUBLISHED'
      AND is_active = true;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.flow_not_active',
            'message', 'flow has no active version',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    v_process_id := gen_random_uuid();

    INSERT INTO workflow.process_instances
    (
        process_id,
        business_key,
        flow_name,
        flow_version,
        process_data,
        process_data_hash,
        state
    )
    VALUES
    (
        v_process_id,
        p_business_key,
        p_flow_name,
        v_flow_version,
        COALESCE(p_data, '{}'::jsonb),
        p_data_hash,
        'CREATED'
    );

    INSERT INTO workflow.workflow_events
    (
        event_id,
        process_id,
        step_instance_id,
        event_type,
        metadata
    )
    VALUES
    (
        gen_random_uuid(),
        v_process_id,
        NULL,
        'ProcessStarted',
        jsonb_build_object(
            'flowName', p_flow_name,
            'flowVersion', v_flow_version
        )
    );

    PERFORM workflow._enter_step(
        v_process_id,
        v_start_step
    );

    SELECT *
    INTO v_existing
    FROM workflow.process_instances
    WHERE process_id = v_process_id;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'process',
            'operation', 'started',
            'processId', v_existing.process_id,
            'flowName', v_existing.flow_name,
            'flowVersion', v_existing.flow_version,
            'state', v_existing.state
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Get process including durable runtime evidence
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.get_process(
    p_process_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_process workflow.process_instances%ROWTYPE;
BEGIN
    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = p_process_id;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.process_not_found',
            'message', 'process does not exist',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'process',
            'processId', v_process.process_id,
            'flowName', v_process.flow_name,
            'flowVersion', v_process.flow_version,
            'state', v_process.state,
            'currentStepKey', v_process.current_step_key,

            'process', jsonb_build_object(
                'processId', v_process.process_id,
                'businessKey', v_process.business_key,
                'flowName', v_process.flow_name,
                'flowVersion', v_process.flow_version,
                'state', v_process.state,
                'currentStepKey', v_process.current_step_key,
                'createdAt', v_process.created_at,
                'updatedAt', v_process.updated_at
            ),

            'steps', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'stepInstanceId', s.step_instance_id,
                            'stepKey', s.step_key,
                            'stepType', s.step_type,
                            'state', s.state,
                            'outcome', s.outcome,
                            'enteredAt', s.entered_at,
                            'completedAt', s.completed_at
                        )
                        ORDER BY s.entered_at, s.step_instance_id
                    )
                    FROM workflow.step_instances AS s
                    WHERE s.process_id = p_process_id
                ),
                '[]'::jsonb
            ),

            'jobs', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'jobId', j.job_id,
                            'stepInstanceId', j.step_instance_id,
                            'executionId', j.execution_id,
                            'state', j.state,
                            'leaseOwner', j.lease_owner,
                            'leaseVersion', j.lease_version,
                            'leaseUntil', j.lease_until,
                            'attemptCount', j.attempt_count,
                            'nextAttemptAt', j.next_attempt_at
                        )
                        ORDER BY j.created_at, j.job_id
                    )
                    FROM workflow.workflow_jobs AS j
                    WHERE j.process_id = p_process_id
                ),
                '[]'::jsonb
            ),

            'attempts', COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'attemptId', a.attempt_id,
                            'jobId', a.job_id,
                            'executionId', a.execution_id,
                            'leaseVersion', a.lease_version,
                            'attemptNumber', a.attempt_number,
                            'status', a.status,
                            'outcome', a.outcome,
                            'errorCode', a.error_code,
                            'startedAt', a.started_at,
                            'finishedAt', a.finished_at
                        )
                        ORDER BY a.started_at, a.attempt_id
                    )
                    FROM workflow.task_attempts AS a
                    JOIN workflow.workflow_jobs AS j
                      ON j.job_id = a.job_id
                    WHERE j.process_id = p_process_id
                ),
                '[]'::jsonb
            )
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Trusted local signal
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.signal_process(
    p_process_id uuid,
    p_signal_type text,
    p_message_id text,
    p_body jsonb,
    p_body_hash text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_process workflow.process_instances%ROWTYPE;
    v_existing workflow.workflow_signals%ROWTYPE;

    v_step_instance_id uuid;
    v_outcome text;
    v_next_step text;
BEGIN
    SELECT *
    INTO v_existing
    FROM workflow.workflow_signals
    WHERE message_id = p_message_id
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.process_id = p_process_id
           AND v_existing.signal_type = p_signal_type
           AND v_existing.body_hash = p_body_hash
           AND v_existing.body = COALESCE(p_body, '{}'::jsonb)
        THEN
            RETURN jsonb_build_object(
                'status', 'ok',
                'result', jsonb_build_object(
                    'resource', 'signal',
                    'processId', p_process_id,
                    'messageId', p_message_id,
                    'signalType', p_signal_type,
                    'status', 'duplicate'
                ),
                'meta', jsonb_build_object(
                    'contractVersion', 'course-1'
                )
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.signal_conflict',
            'message', 'message id already belongs to another signal',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = p_process_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.process_not_found',
            'message', 'process does not exist',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    -- Only signal types declared by the pinned map are accepted.
    IF NOT EXISTS (
        SELECT 1
        FROM workflow.step_definitions AS s
        WHERE s.flow_name = v_process.flow_name
          AND s.flow_version = v_process.flow_version
          AND s.step_type = 'WAIT_SIGNAL'
          AND s.signal_type = p_signal_type
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.signal_unknown',
            'message', 'signal type is not declared by pinned flow',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    INSERT INTO workflow.workflow_signals
    (
        message_id,
        process_id,
        signal_type,
        body,
        body_hash,
        status
    )
    VALUES
    (
        p_message_id,
        p_process_id,
        p_signal_type,
        COALESCE(p_body, '{}'::jsonb),
        p_body_hash,
        'ACCEPTED'
    );

    INSERT INTO workflow.workflow_events
    (
        event_id,
        process_id,
        step_instance_id,
        event_type,
        metadata
    )
    VALUES
    (
        gen_random_uuid(),
        p_process_id,
        NULL,
        'SignalAccepted',
        jsonb_build_object(
            'messageId', p_message_id,
            'signalType', p_signal_type
        )
    );

    -- If the process is already at the matching wait step,
    -- consume the signal immediately.
    IF v_process.state = 'WAITING_SIGNAL' THEN
        SELECT
            d.declared_outcome,
            i.step_instance_id
        INTO
            v_outcome,
            v_step_instance_id
        FROM workflow.step_definitions AS d
        JOIN workflow.step_instances AS i
          ON i.process_id = p_process_id
         AND i.step_key = d.step_key
         AND i.state = 'WAITING'
        WHERE d.flow_name = v_process.flow_name
          AND d.flow_version = v_process.flow_version
          AND d.step_key = v_process.current_step_key
          AND d.step_type = 'WAIT_SIGNAL'
          AND d.signal_type = p_signal_type
        ORDER BY i.entered_at DESC
        LIMIT 1
        FOR UPDATE OF i;

        IF FOUND THEN
            UPDATE workflow.workflow_signals
            SET
                status = 'APPLIED',
                applied_at = clock_timestamp()
            WHERE message_id = p_message_id;

            UPDATE workflow.step_instances
            SET
                state = 'COMPLETED',
                outcome = v_outcome,
                completed_at = clock_timestamp()
            WHERE step_instance_id = v_step_instance_id;

            INSERT INTO workflow.workflow_events
            (
                event_id,
                process_id,
                step_instance_id,
                event_type,
                metadata
            )
            VALUES
            (
                gen_random_uuid(),
                p_process_id,
                v_step_instance_id,
                'SignalApplied',
                jsonb_build_object(
                    'messageId', p_message_id,
                    'signalType', p_signal_type
                )
            );

            SELECT to_step
            INTO v_next_step
            FROM workflow.transition_definitions
            WHERE flow_name = v_process.flow_name
              AND flow_version = v_process.flow_version
              AND from_step = v_process.current_step_key
              AND outcome = v_outcome;

            IF v_next_step IS NULL THEN
                RAISE EXCEPTION 'workflow transition does not exist';
            END IF;

            PERFORM workflow._enter_step(
                p_process_id,
                v_next_step
            );
        END IF;
    END IF;

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'signal',
            'processId', p_process_id,
            'messageId', p_message_id,
            'signalType', p_signal_type,
            'status', 'accepted'
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Worker boundary: atomic concurrent claim
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.claim_jobs(
    p_owner text,
    p_limit integer,
    p_lease_ms integer
)
RETURNS TABLE
(
    job_id uuid,
    process_id uuid,
    step_instance_id uuid,
    execution_id uuid,
    attempt_id uuid,

    lease_version bigint,
    attempt_number integer,

    process_data jsonb,

    module text,
    action text,
    action_version integer,

    request_schema jsonb,
    response_schema jsonb,
    outcomes jsonb,
    required_policy jsonb,

    timeout_ms integer,
    max_attempts integer,
    delays_ms jsonb,

    input_mapping jsonb,
    input_constants jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow, catalog
AS $$
DECLARE
    v_job workflow.workflow_jobs%ROWTYPE;
    v_attempt_id uuid;
BEGIN
    IF p_owner IS NULL OR btrim(p_owner) = '' THEN
        RAISE EXCEPTION 'lease owner is required';
    END IF;

    IF p_limit < 1 OR p_limit > 100 THEN
        RAISE EXCEPTION 'invalid claim limit';
    END IF;

    IF p_lease_ms < 100 OR p_lease_ms > 300000 THEN
        RAISE EXCEPTION 'invalid lease duration';
    END IF;

    FOR v_job IN
        SELECT j.*
        FROM workflow.workflow_jobs AS j
        WHERE
            j.state = 'READY'
            OR (
                j.state = 'RETRY_WAIT'
                AND j.next_attempt_at <= clock_timestamp()
            )
            OR (
                j.state = 'LEASED'
                AND j.lease_until <= clock_timestamp()
            )
        ORDER BY j.created_at, j.job_id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    LOOP
        -- Reclaim: previous attempt becomes STALE, but does not
        -- consume the retry failure budget.
        IF v_job.state = 'LEASED' THEN
            UPDATE workflow.task_attempts
            SET
                status = 'STALE',
                error_code = 'workflow.lease_expired',
                finished_at = clock_timestamp()
            WHERE job_id = v_job.job_id
              AND lease_version = v_job.lease_version
              AND status = 'RUNNING';
        END IF;

        UPDATE workflow.workflow_jobs AS j
        SET
            state = 'LEASED',
            lease_owner = p_owner,
            lease_version = j.lease_version + 1,
            lease_until =
                clock_timestamp()
                + (p_lease_ms * interval '1 millisecond'),
            attempt_count = j.attempt_count + 1,
            next_attempt_at = NULL,
            updated_at = clock_timestamp()
        WHERE j.job_id = v_job.job_id
        RETURNING j.*
        INTO v_job;

        UPDATE workflow.step_instances
        SET state = 'RUNNING'
        WHERE step_instances.step_instance_id =
              v_job.step_instance_id
          AND state IN ('READY', 'RUNNING');

        v_attempt_id := gen_random_uuid();

        INSERT INTO workflow.task_attempts
        (
            attempt_id,
            job_id,
            execution_id,
            lease_version,
            attempt_number,
            status
        )
        VALUES
        (
            v_attempt_id,
            v_job.job_id,
            v_job.execution_id,
            v_job.lease_version,
            v_job.attempt_count,
            'RUNNING'
        );

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type,
            metadata
        )
        VALUES
        (
            gen_random_uuid(),
            v_job.process_id,
            v_job.step_instance_id,
            'TaskClaimed',
            jsonb_build_object(
                'jobId', v_job.job_id,
                'executionId', v_job.execution_id,
                'attemptId', v_attempt_id,
                'leaseOwner', p_owner,
                'leaseVersion', v_job.lease_version
            )
        );

        RETURN QUERY
        SELECT
            j.job_id,
            j.process_id,
            j.step_instance_id,
            j.execution_id,
            v_attempt_id,

            j.lease_version,
            j.attempt_count,

            p.process_data,

            t.module,
            t.action,
            t.action_version,

            a.request_schema,
            a.response_schema,
            a.outcomes,
            t.required_policy,

            t.timeout_ms,
            t.max_attempts,
            t.delays_ms,

            t.input_mapping,
            t.input_constants

        FROM workflow.workflow_jobs AS j

        JOIN workflow.process_instances AS p
          ON p.process_id = j.process_id

        JOIN workflow.step_instances AS si
          ON si.step_instance_id = j.step_instance_id

        JOIN workflow.task_definitions AS t
          ON t.flow_name = p.flow_name
         AND t.flow_version = p.flow_version
         AND t.step_key = si.step_key

        JOIN catalog.action_definitions AS a
          ON a.module = t.module
         AND a.action = t.action
         AND a.version = t.action_version

        WHERE j.job_id = v_job.job_id;
    END LOOP;
END
$$;


-- ============================================================
-- Worker boundary: successful completion + fencing
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.finish_job(
    p_job_id uuid,
    p_owner text,
    p_lease_version bigint,
    p_outcome text,
    p_result jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_job workflow.workflow_jobs%ROWTYPE;

    v_flow_name text;
    v_flow_version integer;
    v_step_key text;

    v_next_step text;
BEGIN
    SELECT *
    INTO v_job
    FROM workflow.workflow_jobs
    WHERE job_id = p_job_id
    FOR UPDATE;

    -- Fencing is deliberately checked before outcome handling.
    IF NOT FOUND
       OR v_job.state <> 'LEASED'
       OR v_job.lease_owner IS DISTINCT FROM p_owner
       OR v_job.lease_version <> p_lease_version
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.lease_stale',
            'message', 'lease no longer owns this job',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    SELECT
        p.flow_name,
        p.flow_version,
        s.step_key
    INTO
        v_flow_name,
        v_flow_version,
        v_step_key
    FROM workflow.process_instances AS p
    JOIN workflow.step_instances AS s
      ON s.process_id = p.process_id
    WHERE p.process_id = v_job.process_id
      AND s.step_instance_id = v_job.step_instance_id;

    v_next_step := NULL;

    SELECT to_step
    INTO v_next_step
    FROM workflow.transition_definitions
    WHERE flow_name = v_flow_name
      AND flow_version = v_flow_version
      AND from_step = v_step_key
      AND outcome = p_outcome;

    IF v_next_step IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.outcome_unknown',
            'message', 'runtime outcome has no declared transition',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM workflow.task_attempts
        WHERE job_id = p_job_id
          AND lease_version = p_lease_version
          AND status = 'RUNNING'
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.lease_stale',
            'message', 'active attempt no longer exists',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    UPDATE workflow.task_attempts
    SET
        status = 'SUCCEEDED',
        outcome = p_outcome,
        finished_at = clock_timestamp()
    WHERE job_id = p_job_id
      AND lease_version = p_lease_version
      AND status = 'RUNNING';

    UPDATE workflow.workflow_jobs
    SET
        state = 'SUCCEEDED',
        lease_owner = NULL,
        lease_until = NULL,
        next_attempt_at = NULL,
        updated_at = clock_timestamp()
    WHERE job_id = p_job_id;

    UPDATE workflow.step_instances
    SET
        state = 'COMPLETED',
        outcome = p_outcome,
        completed_at = clock_timestamp()
    WHERE step_instance_id = v_job.step_instance_id;

    INSERT INTO workflow.workflow_events
    (
        event_id,
        process_id,
        step_instance_id,
        event_type,
        metadata
    )
    VALUES
    (
        gen_random_uuid(),
        v_job.process_id,
        v_job.step_instance_id,
        'TaskSucceeded',
        jsonb_build_object(
            'jobId', p_job_id,
            'executionId', v_job.execution_id,
            'outcome', p_outcome,
            'leaseVersion', p_lease_version
        )
    );

    -- The next durable state is part of the same transaction.
    PERFORM workflow._enter_step(
        v_job.process_id,
        v_next_step
    );

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'job',
            'operation', 'finished',
            'jobId', p_job_id,
            'outcome', p_outcome
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Worker boundary: failure + bounded retry
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.fail_job(
    p_job_id uuid,
    p_owner text,
    p_lease_version bigint,
    p_error_code text,
    p_retryable boolean
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_job workflow.workflow_jobs%ROWTYPE;

    v_max_attempts integer;
    v_delays jsonb;

    v_failure_count integer;
    v_delay_ms integer;
BEGIN
    SELECT *
    INTO v_job
    FROM workflow.workflow_jobs
    WHERE job_id = p_job_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_job.state <> 'LEASED'
       OR v_job.lease_owner IS DISTINCT FROM p_owner
       OR v_job.lease_version <> p_lease_version
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.lease_stale',
            'message', 'lease no longer owns this job',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM workflow.task_attempts
        WHERE job_id = p_job_id
          AND lease_version = p_lease_version
          AND status = 'RUNNING'
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.lease_stale',
            'message', 'active attempt no longer exists',
            'retryable', false,
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    SELECT
        t.max_attempts,
        t.delays_ms
    INTO
        v_max_attempts,
        v_delays
    FROM workflow.process_instances AS p
    JOIN workflow.step_instances AS s
      ON s.process_id = p.process_id
    JOIN workflow.task_definitions AS t
      ON t.flow_name = p.flow_name
     AND t.flow_version = p.flow_version
     AND t.step_key = s.step_key
    WHERE p.process_id = v_job.process_id
      AND s.step_instance_id = v_job.step_instance_id;

    UPDATE workflow.task_attempts
    SET
        status = 'FAILED',
        error_code = p_error_code,
        finished_at = clock_timestamp()
    WHERE job_id = p_job_id
      AND lease_version = p_lease_version
      AND status = 'RUNNING';

    v_failure_count := v_job.failure_count + 1;

    -- Retry budget counts failures, not stale lease reclaims.
    IF p_retryable
       AND v_failure_count < v_max_attempts
    THEN
        v_delay_ms :=
            (v_delays ->> (v_failure_count - 1))::integer;

        UPDATE workflow.workflow_jobs
        SET
            state = 'RETRY_WAIT',
            lease_owner = NULL,
            lease_until = NULL,
            failure_count = v_failure_count,
            next_attempt_at =
                clock_timestamp()
                + (v_delay_ms * interval '1 millisecond'),
            updated_at = clock_timestamp()
        WHERE job_id = p_job_id;

        INSERT INTO workflow.workflow_events
        (
            event_id,
            process_id,
            step_instance_id,
            event_type,
            metadata
        )
        VALUES
        (
            gen_random_uuid(),
            v_job.process_id,
            v_job.step_instance_id,
            'TaskRetryScheduled',
            jsonb_build_object(
                'jobId', p_job_id,
                'executionId', v_job.execution_id,
                'errorCode', p_error_code,
                'delayMs', v_delay_ms,
                'failureCount', v_failure_count
            )
        );

        RETURN jsonb_build_object(
            'status', 'ok',
            'result', jsonb_build_object(
                'resource', 'job',
                'operation', 'retry_scheduled',
                'jobId', p_job_id,
                'state', 'RETRY_WAIT',
                'delayMs', v_delay_ms
            ),
            'meta', jsonb_build_object(
                'contractVersion', 'course-1'
            )
        );
    END IF;

    -- Terminal failure.
    UPDATE workflow.workflow_jobs
    SET
        state = 'DEAD',
        lease_owner = NULL,
        lease_until = NULL,
        failure_count = v_failure_count,
        next_attempt_at = NULL,
        updated_at = clock_timestamp()
    WHERE job_id = p_job_id;

    UPDATE workflow.step_instances
    SET
        state = 'FAILED',
        completed_at = clock_timestamp()
    WHERE step_instance_id = v_job.step_instance_id;

    UPDATE workflow.process_instances
    SET
        state = 'FAILED',
        updated_at = clock_timestamp()
    WHERE process_id = v_job.process_id;

    INSERT INTO workflow.workflow_events
    (
        event_id,
        process_id,
        step_instance_id,
        event_type,
        metadata
    )
    VALUES
    (
        gen_random_uuid(),
        v_job.process_id,
        v_job.step_instance_id,
        'TaskFailed',
        jsonb_build_object(
            'jobId', p_job_id,
            'executionId', v_job.execution_id,
            'errorCode', p_error_code,
            'failureCount', v_failure_count
        )
    );

    RETURN jsonb_build_object(
        'status', 'ok',
        'result', jsonb_build_object(
            'resource', 'job',
            'operation', 'failed',
            'jobId', p_job_id,
            'state', 'DEAD'
        ),
        'meta', jsonb_build_object(
            'contractVersion', 'course-1'
        )
    );
END
$$;


-- ============================================================
-- Privilege boundary
-- ============================================================

REVOKE ALL
ON ALL FUNCTIONS IN SCHEMA workflow
FROM PUBLIC;

ALTER DEFAULT PRIVILEGES
FOR ROLE course_owner
IN SCHEMA workflow
REVOKE EXECUTE ON FUNCTIONS
FROM PUBLIC;

GRANT USAGE
ON SCHEMA workflow
TO workflow_worker;

GRANT USAGE
ON SCHEMA api
TO workflow_worker;

-- Exactly the four worker execution boundaries.
GRANT EXECUTE
ON FUNCTION workflow.claim_jobs(text, integer, integer)
TO workflow_worker;

GRANT EXECUTE
ON FUNCTION api.invoke(text, text, integer, jsonb, jsonb)
TO workflow_worker;

GRANT EXECUTE
ON FUNCTION workflow.finish_job(
    uuid,
    text,
    bigint,
    text,
    jsonb
)
TO workflow_worker;

GRANT EXECUTE
ON FUNCTION workflow.fail_job(
    uuid,
    text,
    bigint,
    text,
    boolean
)
TO workflow_worker;


-- CLI boundary.  course_publication gets no direct workflow DML.
GRANT USAGE
ON SCHEMA workflow
TO course_publication;

GRANT EXECUTE
ON FUNCTION workflow.start_process(
    text,
    text,
    jsonb,
    text
)
TO course_publication;

GRANT EXECUTE
ON FUNCTION workflow.get_process(uuid)
TO course_publication;

GRANT EXECUTE
ON FUNCTION workflow.signal_process(
    uuid,
    text,
    text,
    jsonb,
    text
)
TO course_publication;

RESET ROLE;