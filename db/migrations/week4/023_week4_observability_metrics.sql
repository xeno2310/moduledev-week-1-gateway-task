SET ROLE course_owner;

-- One statement and one MVCC snapshot. STABLE prevents separate snapshots for
-- the component reads; statement_timestamp also fixes the due/age cutoff.
CREATE FUNCTION api.observability_metrics()
RETURNS TABLE (
    workflow_jobs_ready bigint,
    workflow_job_oldest_age_seconds double precision,
    workflow_processes_waiting bigint,
    outbox_pending bigint,
    outbox_oldest_age_seconds double precision,
    workflow_failures_total bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, api
AS $$
    WITH measured AS (SELECT statement_timestamp() AS at),
    ready_jobs AS (
        SELECT j.created_at
        FROM workflow.workflow_jobs AS j CROSS JOIN measured AS m
        WHERE j.state = 'READY'
           OR (j.state = 'RETRY_WAIT' AND j.next_attempt_at <= m.at)
           OR (j.state = 'LEASED' AND j.lease_until <= m.at)
    ),
    pending_outbox AS (
        SELECT o.created_at FROM delivery.outbox AS o
        WHERE o.state IN ('PENDING', 'LEASED', 'RETRY_WAIT')
    )
    SELECT
        (SELECT count(*) FROM ready_jobs),
        GREATEST(0::double precision, COALESCE(
            EXTRACT(epoch FROM (m.at - (SELECT min(created_at) FROM ready_jobs)))::double precision, 0)),
        (SELECT count(*) FROM workflow.process_instances
         WHERE state IN ('WAITING_SIGNAL', 'WAITING_MANUAL')),
        (SELECT count(*) FROM pending_outbox),
        GREATEST(0::double precision, COALESCE(
            EXTRACT(epoch FROM (m.at - (SELECT min(created_at) FROM pending_outbox)))::double precision, 0)),
        -- FAILED is terminal: retries/stale attempts do not increment this.
        (SELECT count(*) FROM workflow.process_instances WHERE state = 'FAILED')
    FROM measured AS m;
$$;

REVOKE ALL ON FUNCTION api.observability_metrics()
FROM PUBLIC, course_publication, workflow_worker, outbox_dispatcher,
     inbox_reconciler, autocheck_reader;
GRANT EXECUTE ON FUNCTION api.observability_metrics() TO course_runtime;

RESET ROLE;
