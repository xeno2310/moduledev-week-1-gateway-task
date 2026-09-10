SET ROLE course_owner;


-- ============================================================
-- payment.submit v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.submit_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, workflow, api
AS $$
DECLARE
    v_operation     payment.operations%ROWTYPE;
    v_process       workflow.process_instances%ROWTYPE;

    v_operation_id  uuid;
    v_process_id    uuid;
    v_flow_name     text;

    v_data           jsonb;
    v_data_hash      text;
    v_payload_hash   text;

    v_started        jsonb;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    -- A later submit command for the same operation never creates
    -- another process and never replaces the original pinned result.

    IF v_operation.process_id IS NOT NULL THEN

        SELECT *
        INTO v_process
        FROM workflow.process_instances
        WHERE process_id = v_operation.process_id;

        IF NOT FOUND THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'workflow.process_not_found',
                'message', 'pinned process does not exist',
                'retryable', false,
                'details', '{}'::jsonb
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'PROCESSING',
            'result', jsonb_build_object(
                'operationId', v_operation.operation_id::text,
                'processId', v_process.process_id::text,
                'flowName', v_process.flow_name,
                'flowVersion', v_process.flow_version,
                'status', 'PROCESSING'
            )
        );
    END IF;


    IF v_operation.status <> 'CREATED' THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation cannot be submitted from current state',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT flow_name
    INTO v_flow_name
    FROM payment.operation_flow_bindings
    WHERE operation_kind = v_operation.operation_kind;

    IF v_flow_name IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.binding_not_found',
            'message', 'server-side workflow binding does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    v_data :=
        jsonb_build_object(
            'operationId',
            v_operation.operation_id::text
        );

    v_data_hash :=
        encode(
            api.digest(
                convert_to(v_data::text, 'UTF8'),
                'sha256'
            ),
            'hex'
        );


    v_started :=
        workflow.start_process(
            v_flow_name,
            v_operation.operation_id::text,
            v_data,
            v_data_hash
        );

    IF v_started ->> 'status' <> 'ok' THEN
        RETURN v_started;
    END IF;


    v_process_id :=
        (v_started #>> '{result,processId}')::uuid;

    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = v_process_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'workflow.start_process returned missing process';
    END IF;


    UPDATE payment.operations
    SET
        status = 'PROCESSING',
        process_id = v_process_id,
        updated_at = clock_timestamp()
    WHERE operation_id = v_operation.operation_id;


    v_payload_hash :=
        encode(
            api.digest(
                convert_to(p_payload::text, 'UTF8'),
                'sha256'
            ),
            'hex'
        );

    INSERT INTO payment.operation_events
    (
        event_id,
        operation_id,
        event_type,
        payload_hash
    )
    VALUES
    (
        gen_random_uuid(),
        v_operation.operation_id,
        'OPERATION_SUBMITTED',
        v_payload_hash
    );


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'PROCESSING',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text,
            'processId', v_process.process_id::text,
            'flowName', v_process.flow_name,
            'flowVersion', v_process.flow_version,
            'status', 'PROCESSING'
        )
    );
END
$$;


-- ============================================================
-- operation.events v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.operation_events_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, payment
AS $$
DECLARE
    v_operation_id uuid;
    v_items jsonb;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    IF NOT EXISTS (
        SELECT 1
        FROM payment.operations
        WHERE operation_id = v_operation_id
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'eventId', e.event_id::text,
                'eventType', e.event_type,
                'payloadHash', e.payload_hash,
                'occurredAt', e.occurred_at
            )
            ORDER BY e.occurred_at, e.event_id
        ),
        '[]'::jsonb
    )
    INTO v_items
    FROM payment.operation_events AS e
    WHERE e.operation_id = v_operation_id;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'FOUND',
        'result', jsonb_build_object(
            'operationId', v_operation_id::text,
            'items', v_items
        )
    );
END
$$;


-- ============================================================
-- payment.validate v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.validate_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;
    v_operation_id uuid;
    v_process_id uuid;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;

    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;

    IF v_operation.status <> 'PROCESSING'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation is not processing in this workflow',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;

    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'VALID',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text
        )
    );
END
$$;


-- ============================================================
-- payment.prepare_external v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.prepare_external_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, delivery, api
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;
    v_existing payment.external_requests%ROWTYPE;

    v_operation_id uuid;
    v_process_id uuid;
    v_execution_id uuid;

    v_external_request_id text;
    v_correlation_id uuid;

    v_provider_body text;
    v_payload_hash text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;

    -- Worker requestId is the stable executionId.
    v_execution_id :=
        NULLIF(p_context ->> 'requestId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;

    IF v_operation.operation_kind <> 'PAYMENT_EXECUTION'
       OR v_operation.status <> 'PROCESSING'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'external request is not valid for this operation',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT *
    INTO v_existing
    FROM payment.external_requests
    WHERE execution_id = v_execution_id
    FOR UPDATE;

    IF FOUND THEN

        IF v_existing.operation_id IS DISTINCT FROM v_operation_id
           OR v_existing.process_id IS DISTINCT FROM v_process_id
        THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'payment.execution_conflict',
                'message', 'execution id belongs to another operation',
                'retryable', false,
                'details', '{}'::jsonb
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'PREPARED',
            'result', jsonb_build_object(
                'operationId', v_operation_id::text,
                'externalRequestId',
                    v_existing.external_request_id
            )
        );
    END IF;


    v_external_request_id :=
        'external-' || v_execution_id::text;

    v_correlation_id :=
        gen_random_uuid();


    -- Canonical provider request bytes used later by Python:
    -- {"amount":"...","currency":"RUB","operationId":"..."}

    v_provider_body :=
        '{"amount":'
        || to_jsonb(v_operation.amount::text)::text
        || ',"currency":'
        || to_jsonb(v_operation.currency)::text
        || ',"operationId":'
        || to_jsonb(v_external_request_id)::text
        || '}';


    v_payload_hash :=
        encode(
            api.digest(
                convert_to(v_provider_body, 'UTF8'),
                'sha256'
            ),
            'hex'
        );


    INSERT INTO payment.external_requests
    (
        external_request_id,
        operation_id,
        process_id,
        execution_id,
        correlation_id,
        amount,
        currency,
        state,
        payload_hash
    )
    VALUES
    (
        v_external_request_id,
        v_operation.operation_id,
        v_process_id,
        v_execution_id,
        v_correlation_id,
        v_operation.amount,
        v_operation.currency,
        'CREATED',
        v_payload_hash
    );


    INSERT INTO delivery.outbox
    (
        outbox_id,
        external_request_id,
        state
    )
    VALUES
    (
        gen_random_uuid(),
        v_external_request_id,
        'PENDING'
    );


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'PREPARED',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text,
            'externalRequestId', v_external_request_id
        )
    );
END
$$;


-- ============================================================
-- payment.apply_receipt v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.apply_receipt_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, delivery
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;

    v_operation_id uuid;
    v_process_id uuid;

    v_message_id text;
    v_external_request_id text;
    v_outcome text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.operation_kind <> 'PAYMENT_EXECUTION'
       OR v_operation.status <> 'PROCESSING'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'receipt cannot be applied to this operation',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT
        r.message_id,
        r.external_request_id,
        r.outcome
    INTO
        v_message_id,
        v_external_request_id,
        v_outcome
    FROM payment.receipts AS r
    JOIN payment.external_requests AS x
      ON x.external_request_id = r.external_request_id
    JOIN delivery.inbox AS i
      ON i.message_id = r.message_id
    WHERE x.operation_id = v_operation.operation_id
      AND x.process_id = v_process_id
      AND i.state = 'APPLIED'
      AND r.applied_at IS NOT NULL
    ORDER BY r.received_at
    LIMIT 1;


    IF v_message_id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'receipt.not_applied',
            'message', 'applied receipt does not exist',
            'retryable', true,
            'details', '{}'::jsonb
        );
    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', v_outcome,
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text,
            'externalRequestId', v_external_request_id,
            'messageId', v_message_id
        )
    );
END
$$;


-- ============================================================
-- payment.complete v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.complete_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;
    v_operation_id uuid;
    v_process_id uuid;
    v_evidence_hash text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT r.body_hash
    INTO v_evidence_hash
    FROM payment.receipts AS r
    JOIN payment.external_requests AS x
      ON x.external_request_id = r.external_request_id
    WHERE x.operation_id = v_operation.operation_id
      AND x.process_id = v_process_id
      AND r.outcome = 'COMPLETED'
      AND r.applied_at IS NOT NULL
    ORDER BY r.received_at
    LIMIT 1;


    IF v_operation.operation_kind <> 'PAYMENT_EXECUTION'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
       OR v_evidence_hash IS NULL
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.evidence_missing',
            'message', 'completed receipt evidence is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.status = 'PROCESSING' THEN

        UPDATE payment.operations
        SET
            status = 'COMPLETED',
            updated_at = clock_timestamp()
        WHERE operation_id = v_operation.operation_id;

        INSERT INTO payment.operation_events
        (
            event_id,
            operation_id,
            event_type,
            payload_hash
        )
        VALUES
        (
            gen_random_uuid(),
            v_operation.operation_id,
            'OPERATION_COMPLETED',
            v_evidence_hash
        );

    ELSIF v_operation.status <> 'COMPLETED' THEN

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation cannot be completed',
            'retryable', false,
            'details', '{}'::jsonb
        );

    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'COMPLETED',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text
        )
    );
END
$$;


-- ============================================================
-- payment.reject v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.reject_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, api
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;

    v_operation_id uuid;
    v_process_id uuid;

    v_evidence_hash text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.process_id IS DISTINCT FROM v_process_id THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation belongs to another process',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.operation_kind = 'PAYMENT_EXECUTION' THEN

        SELECT r.body_hash
        INTO v_evidence_hash
        FROM payment.receipts AS r
        JOIN payment.external_requests AS x
          ON x.external_request_id = r.external_request_id
        WHERE x.operation_id = v_operation.operation_id
          AND x.process_id = v_process_id
          AND r.outcome = 'REJECTED'
          AND r.applied_at IS NOT NULL
        ORDER BY r.received_at
        LIMIT 1;

    ELSIF v_operation.operation_kind = 'PAYMENT_APPROVAL' THEN

        SELECT COALESCE(
            d.reason_hash,
            encode(
                api.digest(
                    convert_to(
                        COALESCE(d.rule_version, d.source),
                        'UTF8'
                    ),
                    'sha256'
                ),
                'hex'
            )
        )
        INTO v_evidence_hash
        FROM payment.decisions AS d
        WHERE d.process_id = v_process_id
          AND d.source = 'MANUAL'
          AND d.outcome = 'REJECTED'
        ORDER BY d.created_at
        LIMIT 1;

    END IF;


    IF v_evidence_hash IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.evidence_missing',
            'message', 'rejection evidence is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.status = 'PROCESSING' THEN

        UPDATE payment.operations
        SET
            status = 'REJECTED',
            updated_at = clock_timestamp()
        WHERE operation_id = v_operation.operation_id;

        INSERT INTO payment.operation_events
        (
            event_id,
            operation_id,
            event_type,
            payload_hash
        )
        VALUES
        (
            gen_random_uuid(),
            v_operation.operation_id,
            'OPERATION_REJECTED',
            v_evidence_hash
        );

    ELSIF v_operation.status <> 'REJECTED' THEN

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation cannot be rejected',
            'retryable', false,
            'details', '{}'::jsonb
        );

    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'REJECTED',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text
        )
    );
END
$$;


-- ============================================================
-- payment.check_limit v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.check_limit_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, workflow
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;

    v_operation_id uuid;
    v_process_id uuid;
    v_job_id uuid;

    v_step_instance_id uuid;
    v_decision_id uuid;

    v_outcome text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;

    v_job_id :=
        NULLIF(p_context ->> 'jobId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.operation_kind <> 'PAYMENT_APPROVAL'
       OR v_operation.status <> 'PROCESSING'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'limit rule cannot be applied',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.amount <= 100000.00 THEN
        v_outcome := 'WITHIN_LIMIT';
    ELSE
        v_outcome := 'REVIEW_REQUIRED';
    END IF;


    -- Only an actual automatic approval is a final audited decision.
    -- REVIEW_REQUIRED itself is not a final decision.

    IF v_outcome = 'WITHIN_LIMIT' THEN

        SELECT step_instance_id
        INTO v_step_instance_id
        FROM workflow.workflow_jobs
        WHERE job_id = v_job_id
          AND process_id = v_process_id;

        IF v_step_instance_id IS NULL THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'workflow.job_not_found',
                'message', 'current workflow job does not exist',
                'retryable', false,
                'details', '{}'::jsonb
            );
        END IF;


        SELECT decision_id
        INTO v_decision_id
        FROM payment.decisions
        WHERE step_instance_id = v_step_instance_id
        FOR UPDATE;

        IF v_decision_id IS NULL THEN

            v_decision_id := gen_random_uuid();

            INSERT INTO payment.decisions
            (
                decision_id,
                process_id,
                step_instance_id,
                source,
                principal,
                reason_hash,
                outcome,
                rule_version
            )
            VALUES
            (
                v_decision_id,
                v_process_id,
                v_step_instance_id,
                'LIMIT_RULE',
                'course-limit-v1',
                NULL,
                'APPROVED',
                'course-limit-v1'
            );

        END IF;
    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', v_outcome,
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text,
            'ruleVersion', 'course-limit-v1'
        )
    );
END
$$;


-- ============================================================
-- payment.approve v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.approve_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, api
AS $$
DECLARE
    v_operation payment.operations%ROWTYPE;

    v_operation_id uuid;
    v_process_id uuid;

    v_evidence_hash text;
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;

    v_process_id :=
        NULLIF(p_context ->> 'processId', '')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT COALESCE(
        d.reason_hash,
        encode(
            api.digest(
                convert_to(
                    COALESCE(d.rule_version, d.source),
                    'UTF8'
                ),
                'sha256'
            ),
            'hex'
        )
    )
    INTO v_evidence_hash
    FROM payment.decisions AS d
    WHERE d.process_id = v_process_id
      AND d.outcome = 'APPROVED'
      AND d.source IN ('LIMIT_RULE', 'MANUAL')
    ORDER BY d.created_at
    LIMIT 1;


    IF v_operation.operation_kind <> 'PAYMENT_APPROVAL'
       OR v_operation.process_id IS DISTINCT FROM v_process_id
       OR v_evidence_hash IS NULL
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.evidence_missing',
            'message', 'audited approval decision is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_operation.status = 'PROCESSING' THEN

        UPDATE payment.operations
        SET
            status = 'COMPLETED',
            updated_at = clock_timestamp()
        WHERE operation_id = v_operation.operation_id;

        INSERT INTO payment.operation_events
        (
            event_id,
            operation_id,
            event_type,
            payload_hash
        )
        VALUES
        (
            gen_random_uuid(),
            v_operation.operation_id,
            'OPERATION_COMPLETED',
            v_evidence_hash
        );

    ELSIF v_operation.status <> 'COMPLETED' THEN

        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.invalid_state',
            'message', 'operation cannot be approved',
            'retryable', false,
            'details', '{}'::jsonb
        );

    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'APPROVED',
        'result', jsonb_build_object(
            'operationId', v_operation.operation_id::text
        )
    );
END
$$;


-- ============================================================
-- receipt.accept v1
-- ============================================================

CREATE OR REPLACE FUNCTION receipt.accept_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, receipt, payment, delivery
AS $$
DECLARE
    v_external payment.external_requests%ROWTYPE;
    v_existing delivery.inbox%ROWTYPE;

    v_external_request_id text;
    v_message_id text;
    v_provider_payment_id text;
    v_occurred_at text;
    v_outcome text;

    v_body_hash text;
    v_existing_message text;
BEGIN
    -- Signature-specific trusted context contains no secret or
    -- signature bytes, only verification markers.

    IF COALESCE(
        (p_context #>> '{transport,signatureVerified}')::boolean,
        false
    ) IS NOT TRUE
       OR COALESCE(
            (p_context #>> '{transport,signatureVersion}')::integer,
            0
       ) <> 1
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'receipt.signature_required',
            'message', 'verified provider signature is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    -- Generic HTTP boundary exposes SHA-256 exact raw bytes only through a transaction-local database setting.
    v_body_hash :=
        NULLIF(
            current_setting(
                'course.raw_body_sha256',
                true
            ),
            ''
        );

    IF v_body_hash IS NULL
       OR v_body_hash !~ '^[0-9a-f]{64}$'
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'action.contract_violation',
            'message', 'trusted exact body hash is missing',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF (p_payload ->> 'version')::integer <> 1 THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'payload.invalid',
            'message', 'unsupported receipt version',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    v_external_request_id :=
        p_payload ->> 'externalRequestId';

    v_message_id :=
        p_payload ->> 'messageId';

    v_provider_payment_id :=
        p_payload ->> 'providerPaymentId';

    v_occurred_at :=
        p_payload ->> 'occurredAt';

    v_outcome :=
        p_payload ->> 'outcome';


    SELECT *
    INTO v_external
    FROM payment.external_requests
    WHERE external_request_id = v_external_request_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'receipt.external_request_not_found',
            'message', 'external request does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT *
    INTO v_existing
    FROM delivery.inbox
    WHERE message_id = v_message_id
    FOR UPDATE;

    IF FOUND THEN

        IF v_existing.body_hash <> v_body_hash THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'idempotency.conflict',
                'message', 'message id already has another body',
                'retryable', false,
                'details', '{}'::jsonb
            );
        END IF;

        RETURN jsonb_build_object(
            'status', 'ok',
            'outcome', 'DUPLICATE',
            'result', jsonb_build_object(
                'messageId', v_message_id,
                'externalRequestId',
                    v_existing.external_request_id,
                'state', v_existing.state
            )
        );
    END IF;


    -- A different message cannot replace already accepted evidence
    -- for the same external payment.

    SELECT r.message_id
    INTO v_existing_message
    FROM payment.receipts AS r
    WHERE r.external_request_id = v_external_request_id
    ORDER BY r.received_at
    LIMIT 1;

    IF v_existing_message IS NOT NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'idempotency.conflict',
            'message', 'external request already has receipt evidence',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    INSERT INTO delivery.inbox
    (
        message_id,
        external_request_id,
        body_hash,
        state
    )
    VALUES
    (
        v_message_id,
        v_external_request_id,
        v_body_hash,
        'RECEIVED'
    );


    INSERT INTO payment.receipts
    (
        message_id,
        external_request_id,
        message_version,
        outcome,
        provider_payment_id,
        occurred_at_text,
        signature_valid,
        body_hash
    )
    VALUES
    (
        v_message_id,
        v_external_request_id,
        1,
        v_outcome,
        v_provider_payment_id,
        v_occurred_at,
        true,
        v_body_hash
    );


    UPDATE delivery.outbox
    SET
        state = 'CONFIRMED',
        lease_owner = NULL,
        lease_until = NULL,
        updated_at = clock_timestamp()
    WHERE external_request_id = v_external_request_id
      AND state <> 'CONFIRMED';


    UPDATE payment.external_requests
    SET
        state = 'CONFIRMED',
        updated_at = clock_timestamp()
    WHERE external_request_id = v_external_request_id
      AND state <> 'CONFIRMED';


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'RECEIVED',
        'result', jsonb_build_object(
            'messageId', v_message_id,
            'externalRequestId', v_external_request_id,
            'state', 'RECEIVED'
        )
    );
END
$$;


-- ============================================================
-- workflow.manual v1
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.manual_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow, payment, api
AS $$
DECLARE
    v_process workflow.process_instances%ROWTYPE;
    v_step_instance workflow.step_instances%ROWTYPE;

    v_process_id uuid;
    v_step_instance_id uuid;

    v_principal text;
    v_decision text;
    v_reason text;

    v_reason_hash text;
    v_decision_id uuid;

    v_next_step text;
    v_allowed jsonb;
BEGIN
    v_process_id :=
        (p_payload ->> 'processId')::uuid;

    v_step_instance_id :=
        (p_payload ->> 'stepInstanceId')::uuid;

    v_decision :=
        p_payload ->> 'decision';

    v_reason :=
        p_payload ->> 'reason';

    v_principal :=
        NULLIF(p_context ->> 'principal', '');


    IF v_principal IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'access.denied',
            'message', 'trusted principal is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT *
    INTO v_process
    FROM workflow.process_instances
    WHERE process_id = v_process_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'manual process is not current',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF v_process.state <> 'WAITING_MANUAL' THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'process is not waiting for manual decision',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT *
    INTO v_step_instance
    FROM workflow.step_instances
    WHERE step_instance_id = v_step_instance_id
      AND process_id = v_process_id
    FOR UPDATE;

    IF NOT FOUND
       OR v_step_instance.step_type <> 'MANUAL'
       OR v_step_instance.state <> 'WAITING'
       OR v_step_instance.step_key
            IS DISTINCT FROM v_process.current_step_key
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'manual step is no longer current',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    IF EXISTS (
        SELECT 1
        FROM payment.decisions
        WHERE step_instance_id = v_step_instance_id
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'manual step already has a decision',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT allowed_outcomes
    INTO v_allowed
    FROM workflow.step_definitions
    WHERE flow_name = v_process.flow_name
      AND flow_version = v_process.flow_version
      AND step_key = v_process.current_step_key;


    IF v_allowed IS NULL
       OR NOT (v_allowed ? v_decision)
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'decision is not allowed by pinned workflow',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    SELECT to_step
    INTO v_next_step
    FROM workflow.transition_definitions
    WHERE flow_name = v_process.flow_name
      AND flow_version = v_process.flow_version
      AND from_step = v_process.current_step_key
      AND outcome = v_decision;

    IF v_next_step IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'workflow.decision_conflict',
            'message', 'manual transition does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    v_reason_hash :=
        encode(
            api.digest(
                convert_to(v_reason, 'UTF8'),
                'sha256'
            ),
            'hex'
        );

    v_decision_id :=
        gen_random_uuid();


    INSERT INTO payment.decisions
    (
        decision_id,
        process_id,
        step_instance_id,
        source,
        principal,
        reason_hash,
        outcome,
        rule_version
    )
    VALUES
    (
        v_decision_id,
        v_process_id,
        v_step_instance_id,
        'MANUAL',
        v_principal,
        v_reason_hash,
        v_decision,
        NULL
    );


    UPDATE workflow.step_instances
    SET
        state = 'COMPLETED',
        outcome = v_decision,
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
        v_process_id,
        v_step_instance_id,
        'ManualDecided',
        jsonb_build_object(
            'decisionId', v_decision_id::text,
            'outcome', v_decision,
            'principal', v_principal,
            'reasonHash', v_reason_hash
        )
    );


    PERFORM workflow._enter_step(
        v_process_id,
        v_next_step
    );


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', v_decision,
        'result', jsonb_build_object(
            'decisionId', v_decision_id::text,
            'processId', v_process_id::text,
            'stepInstanceId', v_step_instance_id::text,
            'decision', v_decision,
            'source', 'MANUAL',
            'principal', v_principal
        )
    );
END
$$;


-- ============================================================
-- Target permissions
-- ============================================================

REVOKE ALL
ON FUNCTION payment.submit_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.operation_events_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.validate_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.prepare_external_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.apply_receipt_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.complete_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.reject_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.check_limit_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.approve_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION receipt.accept_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION workflow.manual_v1(jsonb, jsonb)
FROM PUBLIC;


RESET ROLE;
