SET ROLE course_owner;


-- ============================================================
-- Outbox claim
-- ============================================================

CREATE OR REPLACE FUNCTION delivery.claim_outbox(
    p_owner text,
    p_limit integer
)
RETURNS TABLE
(
    outbox_id uuid,
    lease_version bigint,
    external_request_id text,
    correlation_id uuid,
    amount text,
    currency text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery, payment
AS $$
BEGIN
    IF p_owner IS NULL
       OR btrim(p_owner) = ''
       OR length(p_owner) > 128
       OR position(chr(10) IN p_owner) <> 0
       OR position(chr(13) IN p_owner) <> 0
    THEN
        RAISE EXCEPTION 'invalid outbox owner'
            USING ERRCODE = '22023';
    END IF;

    IF p_limit IS NULL
       OR p_limit < 1
       OR p_limit > 100
    THEN
        RAISE EXCEPTION 'invalid outbox claim limit'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH candidates AS
    (
        SELECT o.outbox_id
        FROM delivery.outbox AS o
        WHERE
            (
                o.state IN ('PENDING', 'RETRY_WAIT')
                AND o.next_attempt_at <= clock_timestamp()
            )
            OR
            (
                o.state = 'LEASED'
                AND o.lease_until IS NOT NULL
                AND o.lease_until <= clock_timestamp()
            )
        ORDER BY
            CASE
                WHEN o.state = 'LEASED'
                    THEN o.lease_until
                ELSE o.next_attempt_at
            END,
            o.created_at,
            o.outbox_id
        LIMIT p_limit
        FOR UPDATE OF o SKIP LOCKED
    ),
    claimed AS
    (
        UPDATE delivery.outbox AS o
        SET
            state = 'LEASED',
            attempt_count = o.attempt_count + 1,
            lease_owner = p_owner,
            lease_version = o.lease_version + 1,
            lease_until =
                clock_timestamp()
                + interval '2 seconds',
            next_attempt_at = clock_timestamp(),
            updated_at = clock_timestamp()
        FROM candidates AS c
        WHERE o.outbox_id = c.outbox_id
        RETURNING
            o.outbox_id,
            o.lease_version,
            o.external_request_id
    )
    SELECT
        c.outbox_id,
        c.lease_version,
        x.external_request_id,
        x.correlation_id,
        x.amount::text,
        x.currency
    FROM claimed AS c
    JOIN payment.external_requests AS x
      ON x.external_request_id =
         c.external_request_id
    ORDER BY c.outbox_id;
END
$$;


-- ============================================================
-- Successful HTTP attempt
--
-- Lock external_request before Outbox. receipt.accept uses the
-- same external_request -> Outbox order.
-- ============================================================

CREATE OR REPLACE FUNCTION delivery.succeed_outbox(
    p_outbox_id uuid,
    p_owner text,
    p_lease_version bigint,
    p_provider_payment_id text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery, payment
AS $$
DECLARE
    v_external_request_id text;
    v_external payment.external_requests%ROWTYPE;
    v_outbox delivery.outbox%ROWTYPE;
BEGIN
    SELECT o.external_request_id
    INTO v_external_request_id
    FROM delivery.outbox AS o
    WHERE o.outbox_id = p_outbox_id;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.outbox_not_found',
            'retryable', false
        );
    END IF;

    SELECT x.*
    INTO v_external
    FROM payment.external_requests AS x
    WHERE x.external_request_id =
          v_external_request_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'outbox references missing external request';
    END IF;

    SELECT o.*
    INTO v_outbox
    FROM delivery.outbox AS o
    WHERE o.outbox_id = p_outbox_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.outbox_not_found',
            'retryable', false
        );
    END IF;

    IF v_outbox.lease_version <> p_lease_version THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.lease_stale',
            'retryable', false
        );
    END IF;

    -- Callback may confirm the delivery while the provider HTTP
    -- response is still in flight.
    IF v_outbox.state = 'CONFIRMED' THEN
        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'CONFIRMED'
        );
    END IF;

    IF v_outbox.state <> 'LEASED'
       OR v_outbox.lease_owner IS DISTINCT FROM p_owner
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.lease_stale',
            'retryable', false
        );
    END IF;

    IF p_provider_payment_id IS NULL
       OR length(p_provider_payment_id) < 1
       OR length(p_provider_payment_id) > 128
       OR position(chr(10) IN p_provider_payment_id) <> 0
       OR position(chr(13) IN p_provider_payment_id) <> 0
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.provider_payment_id_invalid',
            'retryable', false
        );
    END IF;

    UPDATE delivery.outbox AS o
    SET
        state = 'DELIVERED',
        provider_payment_id =
            p_provider_payment_id,
        lease_owner = NULL,
        lease_until = NULL,
        last_error_code = NULL,
        delivered_at =
            COALESCE(
                o.delivered_at,
                clock_timestamp()
            ),
        updated_at = clock_timestamp()
    WHERE o.outbox_id = p_outbox_id
      AND o.state = 'LEASED'
      AND o.lease_owner IS NOT DISTINCT FROM p_owner
      AND o.lease_version = p_lease_version;

    UPDATE payment.external_requests AS x
    SET
        state = 'SENT',
        provider_payment_id =
            p_provider_payment_id,
        updated_at = clock_timestamp()
    WHERE x.external_request_id =
          v_external_request_id
      AND x.state <> 'CONFIRMED';

    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'DELIVERED'
    );
END
$$;


-- ============================================================
-- Failed HTTP attempt
-- ============================================================

CREATE OR REPLACE FUNCTION delivery.fail_outbox(
    p_outbox_id uuid,
    p_owner text,
    p_lease_version bigint,
    p_error_code text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery
AS $$
DECLARE
    v_outbox delivery.outbox%ROWTYPE;

    v_retryable boolean;
    v_terminal boolean;
    v_retry boolean;
BEGIN
    SELECT o.*
    INTO v_outbox
    FROM delivery.outbox AS o
    WHERE o.outbox_id = p_outbox_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.outbox_not_found',
            'retryable', false
        );
    END IF;

    IF v_outbox.lease_version <> p_lease_version THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.lease_stale',
            'retryable', false
        );
    END IF;

    IF v_outbox.state = 'CONFIRMED' THEN
        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'CONFIRMED'
        );
    END IF;

    IF v_outbox.state <> 'LEASED'
       OR v_outbox.lease_owner IS DISTINCT FROM p_owner
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.lease_stale',
            'retryable', false
        );
    END IF;

    IF p_error_code IS NULL
       OR btrim(p_error_code) = ''
       OR length(p_error_code) > 128
       OR position(chr(10) IN p_error_code) <> 0
       OR position(chr(13) IN p_error_code) <> 0
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.error_code_invalid',
            'retryable', false
        );
    END IF;

    v_retryable :=
        p_error_code = 'transport.error.retryable'
        OR p_error_code ~
            '^http\.(408|429|5[0-9][0-9])\.retryable$';

    v_terminal :=
        p_error_code = 'response.invalid.terminal'
        OR p_error_code ~
            '^http\.[1-5][0-9][0-9]\.terminal$';

    IF NOT v_retryable
       AND NOT v_terminal
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'delivery.error_code_invalid',
            'retryable', false
        );
    END IF;

    -- Small bounded Week-3 retry policy.
    v_retry :=
        v_retryable
        AND v_outbox.attempt_count < 3;

    IF v_retry THEN
        UPDATE delivery.outbox AS o
        SET
            state = 'RETRY_WAIT',
            lease_owner = NULL,
            lease_until = NULL,
            next_attempt_at =
                clock_timestamp()
                + (
                    interval '200 milliseconds'
                    * GREATEST(
                        v_outbox.attempt_count,
                        1
                    )
                ),
            last_error_code = p_error_code,
            updated_at = clock_timestamp()
        WHERE o.outbox_id = p_outbox_id
          AND o.state = 'LEASED'
          AND o.lease_owner IS NOT DISTINCT FROM p_owner
          AND o.lease_version = p_lease_version;

        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'RETRY_WAIT'
        );
    END IF;

    -- Simple terminal marker. Operational dead-letter handling,
    -- jitter and production recovery are outside Week 3.
    UPDATE delivery.outbox AS o
    SET
        state = 'DEAD',
        lease_owner = NULL,
        lease_until = NULL,
        last_error_code = p_error_code,
        updated_at = clock_timestamp()
    WHERE o.outbox_id = p_outbox_id
      AND o.state = 'LEASED'
      AND o.lease_owner IS NOT DISTINCT FROM p_owner
      AND o.lease_version = p_lease_version;

    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'DEAD'
    );
END
$$;


-- ============================================================
-- Synchronize workflow APPLIED -> Inbox/receipt APPLIED
-- ============================================================

CREATE OR REPLACE FUNCTION delivery._sync_applied_receipt()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery, payment
AS $$
DECLARE
    v_applied_at timestamptz;
BEGIN
    IF NEW.signal_type <> 'payment.receipt'
       OR NEW.status <> 'APPLIED'
    THEN
        RETURN NEW;
    END IF;

    v_applied_at :=
        COALESCE(
            NEW.applied_at,
            clock_timestamp()
        );

    UPDATE delivery.inbox AS i
    SET
        state = 'APPLIED',
        applied_at =
            COALESCE(
                i.applied_at,
                v_applied_at
            )
    WHERE i.message_id = NEW.message_id
      AND i.state = 'RECEIVED';

    UPDATE payment.receipts AS r
    SET
        applied_at =
            COALESCE(
                r.applied_at,
                v_applied_at
            )
    WHERE r.message_id = NEW.message_id;

    RETURN NEW;
END
$$;


DROP TRIGGER IF EXISTS
    delivery_sync_applied_receipt
ON workflow.workflow_signals;

CREATE TRIGGER delivery_sync_applied_receipt
AFTER UPDATE OF status
ON workflow.workflow_signals
FOR EACH ROW
WHEN (
    NEW.status = 'APPLIED'
    AND OLD.status IS DISTINCT FROM NEW.status
)
EXECUTE FUNCTION delivery._sync_applied_receipt();


-- ============================================================
-- Early receipt bridge.
--
-- Trigger runs while _enter_step owns the process lock.
-- signal_process therefore stores the durable receipt as an
-- ACCEPTED signal. _enter_step then sees and applies it in the
-- same outer transaction.
-- ============================================================

CREATE OR REPLACE FUNCTION delivery._offer_inbox_on_wait()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, delivery, payment, workflow
AS $$
DECLARE
    v_signal_type text;

    v_message_id text;
    v_external_request_id text;
    v_body_hash text;
    v_outcome text;

    v_signal_result jsonb;
BEGIN
    SELECT d.signal_type
    INTO v_signal_type
    FROM workflow.process_instances AS p
    JOIN workflow.step_definitions AS d
      ON d.flow_name = p.flow_name
     AND d.flow_version = p.flow_version
     AND d.step_key = NEW.step_key
    WHERE p.process_id = NEW.process_id
      AND d.step_type = 'WAIT_SIGNAL';

    IF v_signal_type IS DISTINCT FROM 'payment.receipt' THEN
        RETURN NEW;
    END IF;

    SELECT
        i.message_id,
        i.external_request_id,
        i.body_hash,
        r.outcome
    INTO
        v_message_id,
        v_external_request_id,
        v_body_hash,
        v_outcome
    FROM delivery.inbox AS i
    JOIN payment.external_requests AS x
      ON x.external_request_id =
         i.external_request_id
    JOIN payment.receipts AS r
      ON r.message_id = i.message_id
    WHERE x.process_id = NEW.process_id
      AND i.state = 'RECEIVED'
    ORDER BY
        i.received_at,
        i.message_id
    LIMIT 1
    FOR UPDATE OF i;

    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    v_signal_result :=
        workflow.signal_process(
            NEW.process_id,
            v_signal_type,
            v_message_id,
            jsonb_build_object(
                'externalRequestId',
                    v_external_request_id,
                'messageId',
                    v_message_id,
                'outcome',
                    v_outcome
            ),
            v_body_hash
        );

    RETURN NEW;
END
$$;


DROP TRIGGER IF EXISTS
    delivery_offer_inbox_on_wait
ON workflow.step_instances;

CREATE TRIGGER delivery_offer_inbox_on_wait
AFTER INSERT
ON workflow.step_instances
FOR EACH ROW
WHEN (
    NEW.step_type = 'WAIT_SIGNAL'
    AND NEW.state = 'WAITING'
)
EXECUTE FUNCTION delivery._offer_inbox_on_wait();


-- ============================================================
-- Inbox reconciler.
--
-- Do not lock Inbox before workflow.signal_process: that would
-- invert process -> inbox ordering used by the early bridge.
-- ============================================================

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

        IF v_result ->> 'status' = 'ok' THEN
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


-- ============================================================
-- Least privilege
-- ============================================================

REVOKE ALL
ON FUNCTION delivery.claim_outbox(text, integer)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION delivery.succeed_outbox(uuid, text, bigint, text)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION delivery.fail_outbox(uuid, text, bigint, text)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION delivery.reconcile_inbox(integer)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION delivery._sync_applied_receipt()
FROM PUBLIC;

REVOKE ALL
ON FUNCTION delivery._offer_inbox_on_wait()
FROM PUBLIC;


GRANT USAGE
ON SCHEMA delivery
TO outbox_dispatcher;

GRANT USAGE
ON SCHEMA delivery
TO inbox_reconciler;

REVOKE CREATE
ON SCHEMA delivery
FROM outbox_dispatcher;

REVOKE CREATE
ON SCHEMA delivery
FROM inbox_reconciler;


GRANT EXECUTE
ON FUNCTION delivery.claim_outbox(text, integer)
TO outbox_dispatcher;

GRANT EXECUTE
ON FUNCTION delivery.succeed_outbox(uuid, text, bigint, text)
TO outbox_dispatcher;

GRANT EXECUTE
ON FUNCTION delivery.fail_outbox(uuid, text, bigint, text)
TO outbox_dispatcher;

GRANT EXECUTE
ON FUNCTION delivery.reconcile_inbox(integer)
TO inbox_reconciler;


RESET ROLE;
