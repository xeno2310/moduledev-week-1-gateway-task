SET ROLE course_owner;

-- An action dispatch has no domain foreign key. Link only persisted identities:
-- the successful idempotency result, a worker execution, a receipt message,
-- or the provider correlation stored with the external request.
CREATE VIEW diagnostics._trace_dispatch_links AS
SELECT DISTINCT dispatch_tid, operation_id, process_id
FROM (
    SELECT d.ctid AS dispatch_tid, o.operation_id, o.process_id
    FROM catalog.action_dispatches AS d
    JOIN catalog.idempotency_records AS r
      ON r.request_id = d.request_id AND r.module = d.module
     AND r.action = d.action AND r.version = d.version
     AND r.scope_key IN ('principal:' || d.principal, 'global')
     AND r.response ->> 'status' = 'ok'
    JOIN payment.operations AS o
      ON o.operation_id::text = r.response #>> '{result,operationId}'
    UNION ALL
    SELECT d.ctid, o.operation_id, p.process_id
    FROM catalog.action_dispatches AS d
    JOIN catalog.idempotency_records AS r
      ON r.request_id = d.request_id AND r.module = d.module
     AND r.action = d.action AND r.version = d.version
     AND r.scope_key IN ('principal:' || d.principal, 'global')
     AND r.response ->> 'status' = 'ok'
    JOIN workflow.process_instances AS p
      ON p.process_id::text = r.response #>> '{result,processId}'
    LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
    UNION ALL
    SELECT d.ctid, x.operation_id, x.process_id
    FROM catalog.action_dispatches AS d
    JOIN catalog.idempotency_records AS r
      ON r.request_id = d.request_id AND r.module = d.module
     AND r.action = d.action AND r.version = d.version
     AND r.scope_key IN ('principal:' || d.principal, 'global')
     AND r.response ->> 'status' = 'ok'
    JOIN payment.external_requests AS x
      ON x.external_request_id = r.response #>> '{result,externalRequestId}'
    UNION ALL
    SELECT d.ctid, o.operation_id, p.process_id
    FROM catalog.action_dispatches AS d
    JOIN catalog.idempotency_records AS r
      ON r.request_id = d.request_id AND r.module = d.module
     AND r.action = d.action AND r.version = d.version
     AND r.scope_key IN ('principal:' || d.principal, 'global')
     AND r.response ->> 'status' = 'ok'
    JOIN payment.decisions AS q
      ON q.decision_id::text = r.response #>> '{result,decisionId}'
    JOIN workflow.process_instances AS p ON p.process_id = q.process_id
    LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
    UNION ALL
    SELECT d.ctid, o.operation_id, p.process_id
    FROM catalog.action_dispatches AS d
    JOIN workflow.workflow_jobs AS j ON j.execution_id::text = d.request_id
    JOIN workflow.process_instances AS p ON p.process_id = j.process_id
    LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
    UNION ALL
    SELECT d.ctid, x.operation_id, x.process_id
    FROM catalog.action_dispatches AS d
    JOIN payment.external_requests AS x ON x.correlation_id = d.correlation_id
    UNION ALL
    SELECT d.ctid, x.operation_id, x.process_id
    FROM catalog.action_dispatches AS d
    JOIN delivery.inbox AS i ON i.message_id = d.request_id
    JOIN payment.external_requests AS x USING (external_request_id)
) AS links;

REVOKE ALL ON diagnostics._trace_dispatch_links FROM PUBLIC, course_runtime,
    course_publication, workflow_worker, outbox_dispatcher, inbox_reconciler,
    autocheck_reader;

CREATE FUNCTION diagnostics._utc(p_value timestamptz)
RETURNS text LANGUAGE sql STABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT to_char(p_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
$$;

-- The resolver emits one row per matching identifier type and canonical chain.
-- A process without an operation remains its own chain.
CREATE FUNCTION diagnostics._trace_matches(p_identifier text)
RETURNS TABLE(operation_id uuid, process_id uuid, matched_by text, sort_order integer)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT DISTINCT m.operation_id, m.process_id, m.matched_by, m.sort_order
    FROM (
        SELECT o.operation_id, o.process_id, 'operationId'::text AS matched_by, 3 AS sort_order
        FROM payment.operations AS o WHERE o.operation_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, o.process_id, 'requestId', 2
        FROM payment.operations AS o WHERE o.request_id = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'processId', 4
        FROM workflow.process_instances AS p
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE p.process_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'stepInstanceId', 5
        FROM workflow.step_instances AS s
        JOIN workflow.process_instances AS p ON p.process_id = s.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE s.step_instance_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'jobId', 6
        FROM workflow.workflow_jobs AS j
        JOIN workflow.process_instances AS p ON p.process_id = j.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE j.job_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'executionId', 7
        FROM workflow.workflow_jobs AS j
        JOIN workflow.process_instances AS p ON p.process_id = j.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE j.execution_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'attemptId', 8
        FROM workflow.task_attempts AS a
        JOIN workflow.workflow_jobs AS j ON j.job_id = a.job_id
        JOIN workflow.process_instances AS p ON p.process_id = j.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE a.attempt_id::text = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'executionId', 7
        FROM workflow.task_attempts AS a
        JOIN workflow.workflow_jobs AS j ON j.job_id = a.job_id
        JOIN workflow.process_instances AS p ON p.process_id = j.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE a.execution_id::text = p_identifier
        UNION ALL
        SELECT x.operation_id, x.process_id, 'externalRequestId', 9
        FROM payment.external_requests AS x WHERE x.external_request_id = p_identifier
        UNION ALL
        SELECT x.operation_id, x.process_id, 'executionId', 7
        FROM payment.external_requests AS x WHERE x.execution_id::text = p_identifier
        UNION ALL
        SELECT x.operation_id, x.process_id, 'correlationId', 1
        FROM payment.external_requests AS x WHERE x.correlation_id::text = p_identifier
        UNION ALL
        SELECT x.operation_id, x.process_id, 'messageId', 10
        FROM delivery.inbox AS i
        JOIN payment.external_requests AS x USING (external_request_id)
        WHERE i.message_id = p_identifier
        UNION ALL
        SELECT x.operation_id, x.process_id, 'messageId', 10
        FROM payment.receipts AS r
        JOIN payment.external_requests AS x USING (external_request_id)
        WHERE r.message_id = p_identifier AND r.signature_valid IS TRUE
        UNION ALL
        SELECT o.operation_id, p.process_id, 'messageId', 10
        FROM workflow.workflow_signals AS s
        JOIN workflow.process_instances AS p ON p.process_id = s.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE s.message_id = p_identifier
        UNION ALL
        SELECT o.operation_id, p.process_id, 'decisionId', 11
        FROM payment.decisions AS q
        JOIN workflow.process_instances AS p ON p.process_id = q.process_id
        LEFT JOIN payment.operations AS o ON o.process_id = p.process_id
        WHERE q.decision_id::text = p_identifier
        UNION ALL
        SELECT l.operation_id, l.process_id, 'correlationId', 1
        FROM diagnostics._trace_dispatch_links AS l
        JOIN catalog.action_dispatches AS d ON d.ctid = l.dispatch_tid
        WHERE d.correlation_id::text = p_identifier
        UNION ALL
        SELECT l.operation_id, l.process_id, 'requestId', 2
        FROM diagnostics._trace_dispatch_links AS l
        JOIN catalog.action_dispatches AS d ON d.ctid = l.dispatch_tid
        WHERE d.request_id = p_identifier
    ) AS m;
$$;

CREATE FUNCTION diagnostics.trace_v1(p_context jsonb, p_payload jsonb)
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    v_identifier text := p_payload ->> 'identifier';
    v_chains integer;
    v_operation_id uuid;
    v_process_id uuid;
    v_matched_by jsonb;
    v_result jsonb;
BEGIN
    IF v_identifier IS NULL OR char_length(v_identifier) NOT BETWEEN 1 AND 200
       OR position(chr(10) IN v_identifier) <> 0
       OR position(chr(13) IN v_identifier) <> 0 THEN
        RETURN jsonb_build_object('status','error','code','payload.invalid',
            'message','payload does not match schema','retryable',false,'details','{}'::jsonb);
    END IF;

    SELECT count(*), min(c.operation_id::text)::uuid, min(c.process_id::text)::uuid
    INTO v_chains, v_operation_id, v_process_id
    FROM (SELECT DISTINCT operation_id, process_id
          FROM diagnostics._trace_matches(v_identifier)) AS c;

    IF v_chains = 0 THEN
        RETURN jsonb_build_object('status','error','code','diagnostics.trace_not_found',
            'message','trace identifier was not found','retryable',false,'details','{}'::jsonb);
    END IF;
    IF v_chains > 1 THEN
        RETURN jsonb_build_object('status','error','code','diagnostics.trace_ambiguous',
            'message','trace identifier is ambiguous','retryable',false,'details','{}'::jsonb);
    END IF;

    SELECT jsonb_agg(m.matched_by ORDER BY m.sort_order)
    INTO v_matched_by
    FROM (SELECT DISTINCT matched_by, sort_order
          FROM diagnostics._trace_matches(v_identifier)) AS m;

    SELECT jsonb_build_object(
        'query', jsonb_build_object('identifier',v_identifier,'matchedBy',v_matched_by),
        'dispatches', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'correlationId',d.correlation_id::text,
                'requestId',COALESCE(d.request_id,''),
                'module',d.module,'action',d.action,'version',d.version,
                'status',d.status,'outcome',d.outcome,
                'occurredAt',diagnostics._utc(d.occurred_at))
                ORDER BY d.occurred_at,d.correlation_id::text COLLATE "C",
                         COALESCE(d.request_id,'') COLLATE "C",d.module,d.action,d.version)
            FROM catalog.action_dispatches AS d
            WHERE EXISTS (
                SELECT 1 FROM diagnostics._trace_dispatch_links AS l
                WHERE l.dispatch_tid = d.ctid
                  AND l.operation_id IS NOT DISTINCT FROM v_operation_id
                  AND l.process_id IS NOT DISTINCT FROM v_process_id
            )), '[]'::jsonb),
        'operation', (
            SELECT jsonb_build_object(
                'operationId',o.operation_id::text,'requestId',o.request_id,
                'operationKind',o.operation_kind,'amount',o.amount::text,
                'currency',o.currency,'status',o.status,
                'processId',o.process_id::text,
                'createdAt',diagnostics._utc(o.created_at),
                'updatedAt',diagnostics._utc(o.updated_at))
            FROM payment.operations AS o WHERE o.operation_id = v_operation_id),
        'operationEvents', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'eventId',e.event_id::text,'eventType',e.event_type,
                'occurredAt',diagnostics._utc(e.occurred_at))
                ORDER BY e.occurred_at,e.event_id::text COLLATE "C")
            FROM payment.operation_events AS e WHERE e.operation_id = v_operation_id), '[]'::jsonb),
        'process', (
            SELECT jsonb_build_object(
                'processId',p.process_id::text,'flowName',p.flow_name,
                'flowVersion',p.flow_version,'state',p.state,
                'currentStepKey',p.current_step_key,
                'createdAt',diagnostics._utc(p.created_at),
                'updatedAt',diagnostics._utc(p.updated_at))
            FROM workflow.process_instances AS p WHERE p.process_id = v_process_id),
        'steps', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'stepInstanceId',s.step_instance_id::text,'stepKey',s.step_key,
                'stepType',s.step_type,'state',s.state,'outcome',s.outcome,
                'enteredAt',diagnostics._utc(s.entered_at),
                'completedAt',diagnostics._utc(s.completed_at))
                ORDER BY s.entered_at,s.step_instance_id::text COLLATE "C")
            FROM workflow.step_instances AS s WHERE s.process_id = v_process_id), '[]'::jsonb),
        'jobs', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'jobId',j.job_id::text,'stepInstanceId',j.step_instance_id::text,
                'executionId',j.execution_id::text,'state',j.state,
                'leaseVersion',j.lease_version,'attemptCount',j.attempt_count,
                'nextAttemptAt',diagnostics._utc(j.next_attempt_at))
                ORDER BY j.created_at,j.job_id::text COLLATE "C")
            FROM workflow.workflow_jobs AS j WHERE j.process_id = v_process_id), '[]'::jsonb),
        'attempts', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'attemptId',a.attempt_id::text,'jobId',a.job_id::text,
                'executionId',a.execution_id::text,'leaseVersion',a.lease_version,
                'attemptNumber',a.attempt_number,'status',a.status,
                'outcome',a.outcome,
                'errorCode',CASE WHEN a.error_code ~ '^[a-z][a-z0-9_.]{0,127}$'
                                 THEN a.error_code ELSE NULL END,
                'startedAt',diagnostics._utc(a.started_at),
                'finishedAt',diagnostics._utc(a.finished_at))
                ORDER BY a.started_at,a.attempt_id::text COLLATE "C")
            FROM workflow.task_attempts AS a
            JOIN workflow.workflow_jobs AS j ON j.job_id = a.job_id
            WHERE j.process_id = v_process_id), '[]'::jsonb),
        'outbox', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'outboxId',d.outbox_id::text,'externalRequestId',d.external_request_id,
                'state',d.state,'attemptCount',d.attempt_count,
                'leaseVersion',d.lease_version,
                'nextAttemptAt',diagnostics._utc(d.next_attempt_at),
                'lastErrorCode',CASE WHEN d.last_error_code ~ '^[a-z][a-z0-9_.]{0,127}$'
                                     THEN d.last_error_code ELSE NULL END,
                'createdAt',diagnostics._utc(d.created_at),
                'deliveredAt',diagnostics._utc(d.delivered_at),
                'deadAt',diagnostics._utc(d.dead_at))
                ORDER BY d.created_at,d.outbox_id::text COLLATE "C")
            FROM delivery.outbox AS d
            JOIN payment.external_requests AS x USING (external_request_id)
            WHERE x.operation_id = v_operation_id AND x.process_id = v_process_id), '[]'::jsonb),
        'inbox', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'messageId',i.message_id,'state',i.state,
                'receivedAt',diagnostics._utc(i.received_at),
                'appliedAt',diagnostics._utc(i.applied_at))
                ORDER BY i.received_at,i.message_id COLLATE "C")
            FROM delivery.inbox AS i
            JOIN payment.external_requests AS x USING (external_request_id)
            WHERE x.operation_id = v_operation_id AND x.process_id = v_process_id), '[]'::jsonb),
        'receipts', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'messageId',r.message_id,'externalRequestId',r.external_request_id,
                'outcome',r.outcome,'signatureValid',true,
                'receivedAt',diagnostics._utc(r.received_at),
                'appliedAt',diagnostics._utc(r.applied_at))
                ORDER BY r.received_at,r.message_id COLLATE "C")
            FROM payment.receipts AS r
            JOIN payment.external_requests AS x USING (external_request_id)
            WHERE x.operation_id = v_operation_id AND x.process_id = v_process_id
              AND r.signature_valid IS TRUE), '[]'::jsonb),
        'decisions', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'decisionId',q.decision_id::text,
                'stepInstanceId',q.step_instance_id::text,
                'source',q.source,'principal',q.principal,'outcome',q.outcome,
                'ruleVersion',q.rule_version,
                'createdAt',diagnostics._utc(q.created_at))
                ORDER BY q.created_at,q.decision_id::text COLLATE "C")
            FROM payment.decisions AS q
            JOIN workflow.step_instances AS s ON s.step_instance_id = q.step_instance_id
            WHERE q.process_id = v_process_id AND s.process_id = v_process_id), '[]'::jsonb)
    ) INTO v_result;

    RETURN jsonb_build_object('status','ok','outcome','FOUND','result',v_result);
END;
$$;

REVOKE ALL ON FUNCTION diagnostics._utc(timestamptz),
    diagnostics._trace_matches(text), diagnostics.trace_v1(jsonb,jsonb)
FROM PUBLIC, course_runtime, course_publication, workflow_worker,
     outbox_dispatcher, inbox_reconciler, autocheck_reader;

RESET ROLE;
