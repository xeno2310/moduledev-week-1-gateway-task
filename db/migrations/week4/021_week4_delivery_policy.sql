SET ROLE course_owner;

ALTER TABLE delivery.outbox ADD COLUMN dead_at timestamptz;
-- Only a currently DEAD row has a recoverable terminal timestamp.
UPDATE delivery.outbox SET dead_at = updated_at WHERE state = 'DEAD';

CREATE OR REPLACE VIEW autocheck.outbox AS
SELECT outbox_id, external_request_id, state, attempt_count, next_attempt_at,
       last_error_code, created_at, delivered_at,
       lease_owner, lease_version, lease_until, dead_at
FROM delivery.outbox;

-- Deployment settings are session-settable custom GUCs, not an authorization
-- boundary. Runtime code never SETs them; validate again at the SQL boundary.
CREATE FUNCTION delivery._outbox_policy()
RETURNS TABLE(max_attempts integer, base_ms integer, max_ms integer, jitter_ms integer, lease_ms integer)
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    names text[] := ARRAY['course.outbox_max_attempts','course.outbox_backoff_base_ms',
        'course.outbox_backoff_max_ms','course.outbox_jitter_max_ms',
        'course.outbox_lease_ms','course.job_lease_ms'];
    lows integer[] := ARRAY[1,1,1,0,100,100];
    highs integer[] := ARRAY[100,300000,300000,300000,300000,300000];
    values_ms integer[] := ARRAY[]::integer[];
    raw_value text;
    i integer;
BEGIN
    FOR i IN 1..6 LOOP
        raw_value := current_setting(names[i], true);
        IF raw_value IS NULL OR raw_value !~ '^[0-9]{1,9}$' THEN
            RAISE EXCEPTION 'invalid delivery policy configuration' USING ERRCODE = '22023';
        END IF;
        values_ms[i] := raw_value::integer;
        IF values_ms[i] < lows[i] OR values_ms[i] > highs[i] THEN
            RAISE EXCEPTION 'delivery policy configuration out of range' USING ERRCODE = '22023';
        END IF;
    END LOOP;
    IF values_ms[2] > values_ms[3] THEN
        RAISE EXCEPTION 'delivery policy base exceeds maximum' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY SELECT values_ms[1], values_ms[2], values_ms[3], values_ms[4], values_ms[5];
END
$$;
REVOKE ALL ON FUNCTION delivery._outbox_policy() FROM PUBLIC;

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
DECLARE
    v_policy record;
BEGIN
    SELECT * INTO v_policy FROM delivery._outbox_policy();
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
    changed AS
    (
        -- Both claiming and retiring exhausted leases use the same locked
        -- candidate set. An exhausted row never produces another reservation.
        UPDATE delivery.outbox AS o
        SET
            state = CASE WHEN o.attempt_count < v_policy.max_attempts THEN 'LEASED' ELSE 'DEAD' END,
            attempt_count = o.attempt_count + CASE WHEN o.attempt_count < v_policy.max_attempts THEN 1 ELSE 0 END,
            lease_owner = CASE WHEN o.attempt_count < v_policy.max_attempts THEN p_owner ELSE NULL END,
            lease_version = o.lease_version + CASE WHEN o.attempt_count < v_policy.max_attempts THEN 1 ELSE 0 END,
            lease_until = CASE WHEN o.attempt_count < v_policy.max_attempts
                THEN clock_timestamp() + v_policy.lease_ms * interval '1 millisecond' ELSE NULL END,
            next_attempt_at = clock_timestamp(),
            dead_at = CASE WHEN o.attempt_count >= v_policy.max_attempts
                THEN COALESCE(o.dead_at, clock_timestamp()) ELSE o.dead_at END,
            last_error_code = CASE WHEN o.attempt_count >= v_policy.max_attempts
                THEN CASE WHEN o.state = 'LEASED' THEN 'delivery.lease_expired'
                          ELSE COALESCE(NULLIF(o.last_error_code, ''), 'delivery.attempts_exhausted') END
                ELSE o.last_error_code END,
            updated_at = clock_timestamp()
        FROM candidates AS c
        WHERE o.outbox_id = c.outbox_id
        RETURNING o.outbox_id, o.lease_version, o.external_request_id, o.state
    )
    SELECT
        c.outbox_id,
        c.lease_version,
        x.external_request_id,
        x.correlation_id,
        x.amount::text,
        x.currency
    FROM changed AS c
    JOIN payment.external_requests AS x
      ON x.external_request_id =
         c.external_request_id
    WHERE c.state = 'LEASED'
    ORDER BY c.outbox_id;
END
$$;

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
    v_policy record;
    v_delay_ms integer;
    v_failed_at timestamptz;
    i integer;
BEGIN
    SELECT * INTO v_policy FROM delivery._outbox_policy();

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

    -- SQL owns the budget across all dispatcher replicas.
    v_retry :=
        v_retryable
        AND v_outbox.attempt_count < v_policy.max_attempts;

    IF v_retry THEN
        -- Values are bounded to 300000. Saturating doubling reaches the cap
        -- within 19 iterations even for legacy large attempt counts.
        v_delay_ms := v_policy.base_ms;
        FOR i IN 1..GREATEST(v_outbox.attempt_count - 1, 0) LOOP
            EXIT WHEN v_delay_ms >= v_policy.max_ms;
            v_delay_ms := LEAST(v_policy.max_ms, v_delay_ms * 2);
        END LOOP;
        v_delay_ms := v_delay_ms + floor(random() * (v_policy.jitter_ms + 1))::integer;
        v_failed_at := clock_timestamp();
        UPDATE delivery.outbox AS o
        SET
            state = 'RETRY_WAIT',
            lease_owner = NULL,
            lease_until = NULL,
            next_attempt_at = v_failed_at + v_delay_ms * interval '1 millisecond',
            last_error_code = p_error_code,
            updated_at = v_failed_at
        WHERE o.outbox_id = p_outbox_id
          AND o.state = 'LEASED'
          AND o.lease_owner IS NOT DISTINCT FROM p_owner
          AND o.lease_version = p_lease_version;

        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'RETRY_WAIT'
        );
    END IF;

    -- DEAD ends automatic delivery, not the payment/workflow. Late receipt
    -- confirmation preserves this evidence using the existing receipt target.
    UPDATE delivery.outbox AS o
    SET
        state = 'DEAD',
        dead_at = COALESCE(o.dead_at, clock_timestamp()),
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

-- These replace existing functions without changing signatures, owners or ACL.
-- Reader receives views only; never grant SELECT on all (future) tables.
GRANT USAGE ON SCHEMA autocheck TO autocheck_reader;
GRANT SELECT ON
    autocheck.contract_info, autocheck.action_definitions, autocheck.action_dispatches,
    autocheck.operations, autocheck.operation_events, autocheck.flow_versions,
    autocheck.processes, autocheck.steps, autocheck.jobs, autocheck.attempts,
    autocheck.signals, autocheck.workflow_events, autocheck.external_requests,
    autocheck.receipts, autocheck.outbox, autocheck.inbox, autocheck.decisions
TO autocheck_reader;

RESET ROLE;
