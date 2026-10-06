SET ROLE course_owner;

-- 30 seconds is the documented diagnostic threshold for the accelerated course profile.
-- It separates newly exhausted delivery from sustained receipt waiting.
-- Production may choose a larger operational threshold in a future policy migration.
CREATE OR REPLACE FUNCTION diagnostics.stalled_v1(p_context jsonb, p_payload jsonb)
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
      AND d.state = 'DEAD'
      AND d.dead_at IS NOT NULL
      AND d.dead_at <= statement_timestamp() - interval '30 seconds';
$$;

RESET ROLE;
