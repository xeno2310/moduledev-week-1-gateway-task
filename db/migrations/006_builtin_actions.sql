SET ROLE course_owner;


-- ============================================================
-- Built-in mandatory action: payment.request v1
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
    'payment',
    'request',
    1,
    'course-1',
    'POST',
    'payment',
    'request_v1',

    $json$
    {
      "$id": "urn:course:course-1:payment-request-payload",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "operationKind",
        "amount",
        "currency"
      ],
      "properties": {
        "operationKind": {
          "enum": [
            "PAYMENT_EXECUTION",
            "PAYMENT_APPROVAL"
          ]
        },
        "amount": {
          "type": "string",
          "pattern": "^(?:0\\.0[1-9]|0\\.[1-9][0-9]?|[1-9][0-9]{0,15}(?:\\.[0-9]{1,2})?)$"
        },
        "currency": {
          "const": "RUB"
        }
      }
    }
    $json$::jsonb,

    $json$
    {
      "$id": "urn:course:course-1:operation-result",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "operationId",
        "requestId",
        "operationKind",
        "amount",
        "currency",
        "status"
      ],
      "properties": {
        "operationId": {
          "type": "string",
          "format": "uuid"
        },
        "requestId": {
          "type": "string",
          "minLength": 1,
          "maxLength": 128
        },
        "operationKind": {
          "enum": [
            "PAYMENT_EXECUTION",
            "PAYMENT_APPROVAL"
          ]
        },
        "amount": {
          "type": "string",
          "pattern": "^(?:0\\.0[1-9]|0\\.[1-9][0-9]?|[1-9][0-9]{0,15}(?:\\.[0-9]{1,2})?)$"
        },
        "currency": {
          "const": "RUB"
        },
        "status": {
          "enum": [
            "CREATED",
            "PROCESSING",
            "COMPLETED",
            "REJECTED"
          ]
        }
      }
    }
    $json$::jsonb,

    '["CREATED"]'::jsonb,
    '["payment:write"]'::jsonb,
    'required',
    'principal_action',
    2000,
    true,
    true,
    '5c09ccf3485cb852deb26c38e97e4b2adbd538de85cef355565d1bfc698dde0d'
)
ON CONFLICT (module, action, version)
DO NOTHING;


-- ============================================================
-- Built-in mandatory action: operation.get v1
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
    'operation',
    'get',
    1,
    'course-1',
    'POST',
    'payment',
    'operation_get_v1',

    $json$
    {
      "$id": "urn:course:course-1:operation-get-payload",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "operationId"
      ],
      "properties": {
        "operationId": {
          "type": "string",
          "format": "uuid"
        }
      }
    }
    $json$::jsonb,

    $json$
    {
      "$id": "urn:course:course-1:operation-result",
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": [
        "operationId",
        "requestId",
        "operationKind",
        "amount",
        "currency",
        "status"
      ],
      "properties": {
        "operationId": {
          "type": "string",
          "format": "uuid"
        },
        "requestId": {
          "type": "string",
          "minLength": 1,
          "maxLength": 128
        },
        "operationKind": {
          "enum": [
            "PAYMENT_EXECUTION",
            "PAYMENT_APPROVAL"
          ]
        },
        "amount": {
          "type": "string",
          "pattern": "^(?:0\\.0[1-9]|0\\.[1-9][0-9]?|[1-9][0-9]{0,15}(?:\\.[0-9]{1,2})?)$"
        },
        "currency": {
          "const": "RUB"
        },
        "status": {
          "enum": [
            "CREATED",
            "PROCESSING",
            "COMPLETED",
            "REJECTED"
          ]
        }
      }
    }
    $json$::jsonb,

    '["FOUND"]'::jsonb,
    '["payment:read"]'::jsonb,
    'none',
    'none',
    2000,
    true,
    true,
    'ff6116e05dc25cbb0e139e456670d6970198e0c5f122dfdd66376423d09616e6'
)
ON CONFLICT (module, action, version)
DO NOTHING;


RESET ROLE;