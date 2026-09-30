SET ROLE course_owner;

CREATE OR REPLACE FUNCTION delivery.reconcile_inbox(
    p_limit integer
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery, payment, workflow
AS $$
DECLARE
    v_candidate record;
    v_result jsonb;
    v_signal_status text;

    v_applied integer := 0;
BEGIN
    IF p_limit IS NULL
       OR p_limit < 1
       OR p_limit > 100
    THEN
        RAISE EXCEPTION 'invalid inbox reconcile limit'
            USING ERRCODE = '22023';
    END IF;

    FOR v_candidate IN
        SELECT
            i.message_id,
            i.external_request_id,
            i.body_hash,
            x.process_id,
            d.signal_type,
            r.outcome
        FROM delivery.inbox AS i
        JOIN payment.receipts AS r
          ON r.message_id = i.message_id
        JOIN payment.external_requests AS x
          ON x.external_request_id =
             i.external_request_id
        JOIN workflow.process_instances AS p
          ON p.process_id = x.process_id
        JOIN workflow.step_definitions AS d
          ON d.flow_name = p.flow_name
         AND d.flow_version = p.flow_version
         AND d.step_key = p.current_step_key
        WHERE i.state = 'RECEIVED'
          AND p.state = 'WAITING_SIGNAL'
          AND d.step_type = 'WAIT_SIGNAL'
          AND d.signal_type = 'payment.receipt'
        ORDER BY
            i.received_at,
            i.message_id
        LIMIT p_limit
    LOOP
        v_result :=
            workflow.signal_process(
                v_candidate.process_id,
                v_candidate.signal_type,
                v_candidate.message_id,
                jsonb_build_object(
                    'externalRequestId',
                        v_candidate.external_request_id,
                    'messageId',
                        v_candidate.message_id,
                    'outcome',
                        v_candidate.outcome
                ),
                v_candidate.body_hash
            );

        -- A duplicate can observe another transaction's APPLIED signal.
        -- Only the accepting call owns the application count; the signal
        -- trigger may already have changed Inbox, so rowcount is insufficient.
        IF v_result ->> 'status' = 'ok'
           AND v_result #>> '{result,status}' = 'accepted' THEN
            SELECT s.status
            INTO v_signal_status
            FROM workflow.workflow_signals AS s
            WHERE s.message_id =
                  v_candidate.message_id;

            IF v_signal_status = 'APPLIED' THEN
                UPDATE delivery.inbox AS i
                SET
                    state = 'APPLIED',
                    applied_at =
                        COALESCE(
                            i.applied_at,
                            clock_timestamp()
                        )
                WHERE i.message_id =
                      v_candidate.message_id;

                UPDATE payment.receipts AS r
                SET
                    applied_at =
                        COALESCE(
                            r.applied_at,
                            clock_timestamp()
                        )
                WHERE r.message_id =
                      v_candidate.message_id;

                v_applied :=
                    v_applied + 1;
            END IF;
        END IF;
    END LOOP;

    RETURN v_applied;
END
$$;

RESET ROLE;
