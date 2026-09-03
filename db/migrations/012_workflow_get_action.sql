SET ROLE course_owner;


-- ============================================================
-- Built-in action target: workflow.get v1
-- ============================================================

CREATE OR REPLACE FUNCTION workflow.get_v1(
    p_context jsonb,
    p_payload jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, workflow
AS $$
DECLARE
    v_process_id uuid;
    v_response jsonb;
BEGIN
    BEGIN
        v_process_id :=
            (p_payload ->> 'processId')::uuid;
    EXCEPTION
        WHEN invalid_text_representation THEN
            RETURN jsonb_build_object(
                'status', 'error',
                'code', 'workflow.process_not_found',
                'message', 'process does not exist',
                'retryable', false,
                'details', '{}'::jsonb
            );
    END;

    v_response :=
        workflow.get_process(
            v_process_id
        );

    IF v_response ->> 'status' <> 'ok' THEN
        RETURN v_response;
    END IF;

    RETURN jsonb_build_object(
        'status', 'ok',
        'outcome', 'FOUND',
        'result', v_response -> 'result'
    );
END;
$$;


REVOKE ALL
ON FUNCTION workflow.get_v1(jsonb, jsonb)
FROM PUBLIC;


-- ============================================================
-- Built-in mandatory action: workflow.get v1
-- ============================================================

INSERT INTO catalog.action_definitions
(
    module,
    action,
    version,
    contract_version,
    http_method,
    target_schema,
    target_function,
    request_schema,
    response_schema,
    outcomes,
    required_policy,
    idempotency_mode,
    idempotency_scope,
    timeout_ms,
    enabled,
    is_default,
    manifest_hash
)
VALUES
(
    'workflow',
    'get',
    1,
    'course-1',
    'POST',
    'workflow',
    'get_v1',

    $json$
    {
      "$id": "urn:course:course-1:workflow-get-payload",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "processId"
      ],
      "properties": {
        "processId": {
          "type": "string",
          "format": "uuid"
        }
      }
    }
    $json$::jsonb,

    $json$
    {
      "$id": "urn:course:course-1:workflow-get-result",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "required": [
        "process",
        "steps",
        "jobs",
        "attempts"
      ],
      "properties": {
        "process": {
          "type": "object"
        },
        "steps": {
          "type": "array"
        },
        "jobs": {
          "type": "array"
        },
        "attempts": {
          "type": "array"
        }
      }
    }
    $json$::jsonb,

    '["FOUND"]'::jsonb,
    '["workflow:read"]'::jsonb,
    'none',
    'none',
    2000,
    true,
    true,
    '8c64923ff62967cdd78760c42cc9240c05bf2106495191504dfcc6f00e88b878'
)
ON CONFLICT (module, action, version)
DO NOTHING;


RESET ROLE;