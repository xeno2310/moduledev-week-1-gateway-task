SET ROLE course_owner;

-- Новые функции, создаваемые course_owner, не должны
-- автоматически быть исполняемыми через PUBLIC.
ALTER DEFAULT PRIVILEGES
FOR ROLE course_owner
REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;


-- ============================================================
-- Resolve manifest for HTTP runtime
-- ============================================================

CREATE OR REPLACE FUNCTION api.resolve_action(
    p_module text,
    p_action text,
    p_version integer DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, api, catalog
AS $$
DECLARE
    v_definition catalog.action_definitions%ROWTYPE;
BEGIN
    IF p_version IS NULL THEN
        SELECT *
        INTO v_definition
        FROM catalog.action_definitions
        WHERE module = p_module
          AND action = p_action
          AND enabled = true
          AND is_default = true
        LIMIT 1;
    ELSE
        SELECT *
        INTO v_definition
        FROM catalog.action_definitions
        WHERE module = p_module
          AND action = p_action
          AND version = p_version
          AND enabled = true
        LIMIT 1;
    END IF;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    RETURN jsonb_build_object(
        'contract_version', v_definition.contract_version,
        'module', v_definition.module,
        'action', v_definition.action,
        'version', v_definition.version,
        'http_method', v_definition.http_method,
        'target_schema', v_definition.target_schema,
        'target_function', v_definition.target_function,
        'request_schema', v_definition.request_schema,
        'response_schema', v_definition.response_schema,
        'outcomes', v_definition.outcomes,
        'required_policy', v_definition.required_policy,
        'idempotency_mode', v_definition.idempotency_mode,
        'idempotency_scope', v_definition.idempotency_scope,
        'timeout_ms', v_definition.timeout_ms
    );
END;
$$;


-- ============================================================
-- Generic database-first dispatcher
-- ============================================================

CREATE OR REPLACE FUNCTION api.invoke(
    p_module text,
    p_action text,
    p_version integer,
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, api, catalog
AS $$
DECLARE
    v_definition catalog.action_definitions%ROWTYPE;

    v_target_count integer;

    v_result jsonb;

    v_request_id text;
    v_scope_key text;

    v_payload_hash text;
    v_existing_hash text;
    v_existing_response jsonb;

    v_correlation_id uuid;
BEGIN
    -- --------------------------------------------------------
    -- Resolve exact/default enabled version from trusted catalog
    -- --------------------------------------------------------

    IF p_version IS NULL THEN
        SELECT *
        INTO v_definition
        FROM catalog.action_definitions
        WHERE module = p_module
          AND action = p_action
          AND enabled = true
          AND is_default = true
        LIMIT 1;
    ELSE
        SELECT *
        INTO v_definition
        FROM catalog.action_definitions
        WHERE module = p_module
          AND action = p_action
          AND version = p_version
          AND enabled = true
        LIMIT 1;
    END IF;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'action.not_found',
            'message', 'action or version not found',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'correlationId', p_context ->> 'correlationId',
                'actionVersion', NULL
            )
        );
    END IF;


    -- --------------------------------------------------------
    -- Trusted context
    -- --------------------------------------------------------

    BEGIN
        v_correlation_id :=
            (p_context ->> 'correlationId')::uuid;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'access.denied',
                'message', 'trusted context is invalid',
                'retryable', false,
                'details', '{}'::jsonb,
                'meta', jsonb_build_object(
                    'correlationId',
                    p_context ->> 'correlationId',
                    'actionVersion',
                    v_definition.version
                )
            );
    END;


    -- --------------------------------------------------------
    -- Defense-in-depth policy check
    -- --------------------------------------------------------

    IF jsonb_typeof(p_context -> 'scopes')
        IS DISTINCT FROM 'array'
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'access.denied',
            'message', 'required policy is not satisfied',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'correlationId', p_context ->> 'correlationId',
                'actionVersion', v_definition.version
            )
        );
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
            v_definition.required_policy
        ) AS req(scope)
        WHERE NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(
                p_context -> 'scopes'
            ) AS actual(scope)
            WHERE actual.scope = req.scope
        )
    )
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'access.denied',
            'message', 'required policy is not satisfied',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'correlationId', p_context ->> 'correlationId',
                'actionVersion', v_definition.version
            )
        );
    END IF;


    -- --------------------------------------------------------
    -- Verify registered target and exact signature
    -- --------------------------------------------------------

    SELECT count(*)
    INTO v_target_count
    FROM pg_proc p
    JOIN pg_namespace n
      ON n.oid = p.pronamespace
    WHERE n.nspname = v_definition.target_schema
      AND p.proname = v_definition.target_function
      AND p.prokind = 'f'
      AND p.pronargs = 2
      AND p.prorettype = 'jsonb'::regtype
      AND p.proargtypes[0] = 'jsonb'::regtype
      AND p.proargtypes[1] = 'jsonb'::regtype;

    IF v_target_count <> 1 THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'action.contract_violation',
            'message', 'registered target has invalid signature',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'correlationId', p_context ->> 'correlationId',
                'actionVersion', v_definition.version
            )
        );
    END IF;


    -- --------------------------------------------------------
    -- Idempotency
    -- --------------------------------------------------------

    v_request_id :=
        NULLIF(p_context ->> 'requestId', '');

    IF v_definition.idempotency_mode = 'required'
       AND v_request_id IS NULL
    THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'code', 'idempotency.required',
            'message', 'idempotency key is required',
            'retryable', false,
            'details', '{}'::jsonb,
            'meta', jsonb_build_object(
                'correlationId', p_context ->> 'correlationId',
                'actionVersion', v_definition.version
            )
        );
    END IF;

    v_payload_hash :=
        encode(
            digest(
                convert_to(p_payload::text, 'UTF8'),
                'sha256'
            ),
            'hex'
        );

    IF v_definition.idempotency_mode <> 'none'
       AND v_request_id IS NOT NULL
    THEN

        v_scope_key :=
            CASE v_definition.idempotency_scope

                WHEN 'principal_action'
                    THEN 'principal:' ||
                         COALESCE(
                             p_context ->> 'principal',
                             ''
                         )

                WHEN 'consumer_action'
                    THEN 'consumer:' ||
                         COALESCE(
                             p_context ->> 'consumer',
                             ''
                         )

                WHEN 'global_action'
                    THEN 'global'

                ELSE 'none'
            END;

        -- Transaction-scoped DB lock.
        -- Concurrent calls with the same idempotency identity
        -- cannot both execute the target.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(
                v_scope_key || '|' ||
                p_module || '|' ||
                p_action || '|' ||
                v_definition.version::text || '|' ||
                v_request_id,
                0
            )
        );

        SELECT
            payload_hash,
            response
        INTO
            v_existing_hash,
            v_existing_response
        FROM catalog.idempotency_records
        WHERE scope_key = v_scope_key
          AND module = p_module
          AND action = p_action
          AND version = v_definition.version
          AND request_id = v_request_id;

        IF FOUND THEN

            IF v_existing_hash <> v_payload_hash THEN
                RETURN jsonb_build_object(
                    'status', 'error',
                    'code', 'idempotency.conflict',
                    'message',
                        'idempotency key was used with another payload',
                    'retryable', false,
                    'details', '{}'::jsonb,
                    'meta', jsonb_build_object(
                        'correlationId',
                        p_context ->> 'correlationId',
                        'actionVersion',
                        v_definition.version
                    )
                );
            END IF;

            INSERT INTO catalog.action_dispatches
            (
                correlation_id,
                request_id,
                module,
                action,
                version,
                principal,
                payload_hash,
                status,
                outcome
            )
            VALUES
            (
                v_correlation_id,
                v_request_id,
                p_module,
                p_action,
                v_definition.version,
                p_context ->> 'principal',
                v_payload_hash,
                'OK',
                v_existing_response ->> 'outcome'
            );

            RETURN v_existing_response;
        END IF;
    END IF;


    -- --------------------------------------------------------
    -- The only dynamic execution.
    --
    -- Identifiers come from catalog, never payload.
    -- format(%I) quotes identifiers safely.
    -- --------------------------------------------------------

    EXECUTE format(
        'SELECT %I.%I($1, $2)',
        v_definition.target_schema,
        v_definition.target_function
    )
    INTO v_result
    USING p_context, p_payload;


    -- --------------------------------------------------------
    -- Dispatch evidence
    -- --------------------------------------------------------

    INSERT INTO catalog.action_dispatches
    (
        correlation_id,
        request_id,
        module,
        action,
        version,
        principal,
        payload_hash,
        status,
        outcome
    )
    VALUES
    (
        v_correlation_id,
        v_request_id,
        p_module,
        p_action,
        v_definition.version,
        p_context ->> 'principal',
        v_payload_hash,

        CASE
            WHEN v_result ->> 'status' = 'ok'
                THEN 'OK'
            ELSE 'ERROR'
        END,

        v_result ->> 'outcome'
    );


    -- Save only a successful envelope.
    -- If C# later detects invalid outcome/result and rolls
    -- back its transaction, this INSERT rolls back as well.
    IF v_definition.idempotency_mode <> 'none'
       AND v_request_id IS NOT NULL
       AND v_result ->> 'status' = 'ok'
    THEN
        INSERT INTO catalog.idempotency_records
        (
            scope_key,
            module,
            action,
            version,
            request_id,
            payload_hash,
            response
        )
        VALUES
        (
            v_scope_key,
            p_module,
            p_action,
            v_definition.version,
            v_request_id,
            v_payload_hash,
            v_result
        );
    END IF;

    RETURN v_result;
END;
$$;


-- ============================================================
-- Runtime permissions
-- ============================================================

REVOKE ALL
ON FUNCTION api.resolve_action(text, text, integer)
FROM PUBLIC;

REVOKE ALL
ON FUNCTION api.invoke(text, text, integer, jsonb, jsonb)
FROM PUBLIC;

GRANT EXECUTE
ON FUNCTION api.resolve_action(text, text, integer)
TO course_runtime;

GRANT EXECUTE
ON FUNCTION api.invoke(text, text, integer, jsonb, jsonb)
TO course_runtime;


-- Existing target functions may have been created before the
-- new default privilege rule. Remove PUBLIC execute from every
-- schema owned by course_owner.
DO $$
DECLARE
    v_schema text;
BEGIN
    FOR v_schema IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspowner = 'course_owner'::regrole
    LOOP
        EXECUTE format(
            'REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC',
            v_schema
        );
    END LOOP;
END
$$;


-- The previous block also revoked api functions,
-- so explicitly restore only the two runtime entrypoints.

GRANT EXECUTE
ON FUNCTION api.resolve_action(text, text, integer)
TO course_runtime;

GRANT EXECUTE
ON FUNCTION api.invoke(text, text, integer, jsonb, jsonb)
TO course_runtime;

RESET ROLE;