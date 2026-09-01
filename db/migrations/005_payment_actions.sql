SET ROLE course_owner;


-- ============================================================
-- payment.request v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.request_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, payment, api
AS $$
DECLARE
    v_operation_id uuid;
    v_event_id uuid;

    v_request_id text;
    v_operation_kind text;
    v_amount_text text;
    v_amount numeric(18,2);
    v_currency text;

    v_payload_hash text;
BEGIN
    v_request_id :=
        NULLIF(p_context ->> 'requestId', '');

    IF v_request_id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'idempotency.required',
            'message', 'idempotency key is required',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;

    v_operation_kind :=
        p_payload ->> 'operationKind';

    v_amount_text :=
        p_payload ->> 'amount';

    v_amount :=
        v_amount_text::numeric(18,2);

    v_currency :=
        p_payload ->> 'currency';

    v_operation_id :=
        gen_random_uuid();

    v_event_id :=
        gen_random_uuid();

    v_payload_hash :=
        encode(
            api.digest(
                convert_to(
                    p_payload::text,
                    'UTF8'
                ),
                'sha256'
            ),
            'hex'
        );


    INSERT INTO payment.operations
    (
        operation_id,
        request_id,
        operation_kind,
        amount,
        currency,
        status,
        process_id
    )
    VALUES
    (
        v_operation_id,
        v_request_id,
        v_operation_kind,
        v_amount,
        v_currency,
        'CREATED',
        NULL
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
        v_event_id,
        v_operation_id,
        'OPERATION_CREATED',
        v_payload_hash
    );


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'CREATED',

        'result', jsonb_build_object(
            'operationId',
                v_operation_id::text,

            'requestId',
                v_request_id,

            'operationKind',
                v_operation_kind,

            'amount',
                v_amount_text,

            'currency',
                v_currency,

            'status',
                'CREATED'
        )
    );
END;
$$;


-- ============================================================
-- operation.get v1
-- ============================================================

CREATE OR REPLACE FUNCTION payment.operation_get_v1(
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
BEGIN
    v_operation_id :=
        (p_payload ->> 'operationId')::uuid;


    SELECT *
    INTO v_operation
    FROM payment.operations
    WHERE operation_id = v_operation_id;


    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'operation.not_found',
            'message', 'operation does not exist',
            'retryable', false,
            'details', '{}'::jsonb
        );
    END IF;


    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'FOUND',

        'result', jsonb_build_object(
            'operationId',
                v_operation.operation_id::text,

            'requestId',
                v_operation.request_id,

            'operationKind',
                v_operation.operation_kind,

            'amount',
                v_operation.amount::text,

            'currency',
                v_operation.currency,

            'status',
                v_operation.status
        )
    );
END;
$$;


-- Targets must not be callable by PUBLIC/runtime directly.

REVOKE ALL
ON FUNCTION payment.request_v1(jsonb, jsonb)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION payment.operation_get_v1(jsonb, jsonb)
FROM PUBLIC;


RESET ROLE;