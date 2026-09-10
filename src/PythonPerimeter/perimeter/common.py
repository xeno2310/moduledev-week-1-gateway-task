from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import datetime
from typing import Any


MAX_CALLBACK_BODY = 64 * 1024
MAX_PROVIDER_RESPONSE = 64 * 1024

_CALLBACK_FIELDS = {
    "providerPaymentId",
    "operationId",
    "result",
    "message",
    "occurredAt",
}

_RECEIPT_FIELDS = {
    "externalRequestId",
    "messageId",
    "occurredAt",
    "outcome",
    "providerPaymentId",
    "version",
}

_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"
    r"T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?Z$"
)


class ContractError(ValueError):
    pass


def _reject_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for key, value in pairs:
        if key in result:
            raise ContractError(
                "duplicate JSON member"
            )

        result[key] = value

    return result


def parse_json_object(
    raw: bytes,
) -> dict[str, Any]:
    try:
        text = raw.decode(
            "utf-8",
            errors="strict",
        )
    except UnicodeDecodeError as exc:
        raise ContractError(
            "invalid UTF-8"
        ) from exc

    try:
        value = json.loads(
            text,
            object_pairs_hook=
                _reject_duplicate_keys,
        )
    except (
        json.JSONDecodeError,
        ContractError,
    ) as exc:
        raise ContractError(
            "invalid JSON"
        ) from exc

    if not isinstance(value, dict):
        raise ContractError(
            "JSON root must be object"
        )

    return value


def compact_json_bytes(
    value: dict[str, Any],
) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_hex(
    body: bytes,
) -> str:
    return hashlib.sha256(
        body
    ).hexdigest()


def hmac_signature(
    secret: str,
    body: bytes,
) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()

    return "v1=" + digest


def _valid_transport_text(
    value: Any,
    *,
    minimum: int,
    maximum: int,
) -> bool:
    return (
        isinstance(value, str)
        and minimum <= len(value) <= maximum
        and "\r" not in value
        and "\n" not in value
    )


def _valid_timestamp(
    value: Any,
) -> bool:
    if not _valid_transport_text(
        value,
        minimum=1,
        maximum=64,
    ):
        return False

    if _TIMESTAMP_RE.fullmatch(
        value
    ) is None:
        return False

    try:
        datetime.fromisoformat(
            value[:-1] + "+00:00"
        )
    except ValueError:
        return False

    return True


def receipt_from_legacy(
    callback: dict[str, Any],
) -> dict[str, Any]:
    if set(callback) != _CALLBACK_FIELDS:
        raise ContractError(
            "callback fields do not match schema"
        )

    provider_payment_id = callback[
        "providerPaymentId"
    ]

    operation_id = callback[
        "operationId"
    ]

    result = callback[
        "result"
    ]

    message = callback[
        "message"
    ]

    occurred_at = callback[
        "occurredAt"
    ]

    if not _valid_transport_text(
        provider_payment_id,
        minimum=1,
        maximum=128,
    ):
        raise ContractError(
            "invalid providerPaymentId"
        )

    if not _valid_transport_text(
        operation_id,
        minimum=1,
        maximum=128,
    ):
        raise ContractError(
            "invalid operationId"
        )

    if result not in {
        "COMPLETED",
        "REJECTED",
    }:
        raise ContractError(
            "invalid result"
        )

    if not _valid_transport_text(
        message,
        minimum=0,
        maximum=500,
    ):
        raise ContractError(
            "invalid message"
        )

    if not _valid_timestamp(
        occurred_at
    ):
        raise ContractError(
            "invalid occurredAt"
        )

    receipt = {
        "externalRequestId":
            operation_id,
        "messageId":
            provider_payment_id,
        "occurredAt":
            occurred_at,
        "outcome":
            result,
        "providerPaymentId":
            provider_payment_id,
        "version":
            1,
    }

    assert set(receipt) == _RECEIPT_FIELDS

    return receipt


def classify_http_status(
    status: int,
) -> str:
    if (
        status == 408
        or status == 429
        or 500 <= status <= 599
    ):
        return (
            f"http.{status}.retryable"
        )

    return f"http.{status}.terminal"


def parse_provider_success(
    status: int,
    body: bytes,
) -> tuple[bool, str]:
    if status != 202:
        return (
            False,
            classify_http_status(
                status
            ),
        )

    if len(body) > MAX_PROVIDER_RESPONSE:
        return (
            False,
            "response.invalid.terminal",
        )

    try:
        value = parse_json_object(
            body
        )
    except ContractError:
        return (
            False,
            "response.invalid.terminal",
        )

    if set(value) != {
        "providerPaymentId",
        "status",
    }:
        return (
            False,
            "response.invalid.terminal",
        )

    provider_payment_id = value.get(
        "providerPaymentId"
    )

    provider_status = value.get(
        "status"
    )

    if provider_status != "ACCEPTED":
        return (
            False,
            "response.invalid.terminal",
        )

    if not _valid_transport_text(
        provider_payment_id,
        minimum=1,
        maximum=128,
    ):
        return (
            False,
            "response.invalid.terminal",
        )

    return (
        True,
        provider_payment_id,
    )
