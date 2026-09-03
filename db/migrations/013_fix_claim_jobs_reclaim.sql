SET ROLE course_owner;

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
            UPDATE workflow.task_attempts AS ta
            SET
                status = 'STALE',
                error_code = 'workflow.lease_expired',
                finished_at = clock_timestamp()
            WHERE ta.job_id = v_job.job_id
              AND ta.lease_version = v_job.lease_version
              AND ta.status = 'RUNNING';
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

RESET ROLE;