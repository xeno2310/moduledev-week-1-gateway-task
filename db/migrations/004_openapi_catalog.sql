SET ROLE course_owner;


-- ============================================================
-- Definitions exposed to the HTTP/OpenAPI runtime
-- ============================================================

CREATE OR REPLACE FUNCTION api.openapi_default_actions()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, catalog
AS $$
    SELECT coalesce(
        jsonb_agg(
            jsonb_build_object(
                'module', module,
                'action', action,
                'version', version,
                'request_schema', request_schema,
                'response_schema', response_schema,
                'outcomes', outcomes,
                'idempotency_mode', idempotency_mode
            )
            ORDER BY module, action
        ),
        '[]'::jsonb
    )
    FROM catalog.action_definitions
    WHERE enabled = true
      AND is_default = true;
$$;


CREATE OR REPLACE FUNCTION api.openapi_action(
    p_module text,
    p_action text,
    p_version integer
)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, catalog
AS $$
    SELECT jsonb_build_object(
        'module', module,
        'action', action,
        'version', version,
        'request_schema', request_schema,
        'response_schema', response_schema,
        'outcomes', outcomes,
        'idempotency_mode', idempotency_mode
    )
    FROM catalog.action_definitions
    WHERE module = p_module
      AND action = p_action
      AND version = p_version
      AND enabled = true;
$$;


REVOKE ALL
ON FUNCTION api.openapi_default_actions()
FROM PUBLIC;

REVOKE ALL
ON FUNCTION api.openapi_action(text, text, integer)
FROM PUBLIC;

GRANT EXECUTE
ON FUNCTION api.openapi_default_actions()
TO course_runtime;

GRANT EXECUTE
ON FUNCTION api.openapi_action(text, text, integer)
TO course_runtime;


RESET ROLE;