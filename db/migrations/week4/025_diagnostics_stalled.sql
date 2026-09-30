SET ROLE course_owner;

CREATE SCHEMA diagnostics AUTHORIZATION course_owner;
REVOKE ALL ON SCHEMA diagnostics FROM PUBLIC;

-- A single MVCC snapshot, with no locks, domain writes or delivery work.
-- external_requests.operation_id and outbox.external_request_id are UNIQUE:
-- these joins produce at most one item per operation, without pagination.
CREATE FUNCTION diagnostics.stalled_v1(p_context jsonb, p_payload jsonb)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT jsonb_build_object(
        'status', 'ok',
        'outcome', 'FOUND',
        'result', jsonb_build_object(
            'items', COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'operationId', o.operation_id::text,
                        'processId', p.process_id::text,
                        'externalRequestId', x.external_request_id
                    ) ORDER BY o.operation_id::text COLLATE "C"
                ),
                '[]'::jsonb
            )
        )
    )
    FROM payment.operations AS o
    JOIN workflow.process_instances AS p
      ON p.process_id = o.process_id
    JOIN payment.external_requests AS x
      ON x.operation_id = o.operation_id
     AND x.process_id = p.process_id
    JOIN delivery.outbox AS d
      ON d.external_request_id = x.external_request_id
    WHERE o.status = 'PROCESSING'
      AND p.state = 'WAITING_SIGNAL'
      AND d.state = 'DEAD';
$$;

-- Only course_owner, the effective role of generic api.invoke, can call the
-- target. diagnostics:read is a JWT policy, not a new database reader role.
-- No runtime/reader role receives direct domain-table or target privileges.
REVOKE ALL ON FUNCTION diagnostics.stalled_v1(jsonb,jsonb)
FROM PUBLIC, course_runtime, course_publication, workflow_worker,
     outbox_dispatcher, inbox_reconciler, autocheck_reader;

RESET ROLE;
