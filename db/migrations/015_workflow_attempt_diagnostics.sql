SET ROLE course_owner;

-- ============================================================
-- Attempt-correlated workflow diagnostics
--
-- Runtime, retry and fencing semantics are unchanged.
-- Only durable task-event metadata is enriched.
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
            'processId', v_job.process_id,
            'jobId', p_job_id,
            'executionId', v_job.execution_id,
            'attemptId', (
                SELECT a.attempt_id
                FROM workflow.task_attempts AS a
                WHERE a.job_id = p_job_id
                  AND a.lease_version = p_lease_version
                LIMIT 1
            ),
            'leaseVersion', p_lease_version,
            'outcome', p_outcome
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
                'processId', v_job.process_id,
                'jobId', p_job_id,
                'executionId', v_job.execution_id,
                'attemptId', (
                    SELECT a.attempt_id
                    FROM workflow.task_attempts AS a
                    WHERE a.job_id = p_job_id
                      AND a.lease_version = p_lease_version
                    LIMIT 1
                ),
                'leaseVersion', p_lease_version,
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
            'processId', v_job.process_id,
            'jobId', p_job_id,
            'executionId', v_job.execution_id,
            'attemptId', (
                SELECT a.attempt_id
                FROM workflow.task_attempts AS a
                WHERE a.job_id = p_job_id
                  AND a.lease_version = p_lease_version
                LIMIT 1
            ),
            'leaseVersion', p_lease_version,
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

RESET ROLE;
