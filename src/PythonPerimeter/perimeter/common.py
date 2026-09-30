from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from dataclasses import dataclass
from datetime import datetime
from typing import Any


MAX_CALLBACK_BODY = 64 * 1024
MAX_PROVIDER_RESPONSE = 64 * 1024

OBSERVABILITY_PORT = 8080
ADAPTER_PORT = 8082
OPENMETRICS_CONTENT_TYPE = "application/openmetrics-text; version=1.0.0; charset=utf-8"
PROCESS_METRICS = b"# TYPE process_up gauge\nprocess_up 1\n# EOF\n"
LIVE_BODY = b'{"status":"live"}'
READY_BODY = b'{"status":"ready"}'
NOT_READY_BODY = b'{"status":"not_ready","code":"dependency.unavailable"}'


def reach_failpoint(name: str, instance_id: str, **identifiers: str) -> None:
    if (os.environ.get("COURSE_TEST_PROFILE") != "1"
            or os.environ.get("COURSE_FAILPOINT") != name):
        return
    event = {"event": "failpoint.reached", "name": name,
             "instanceId": instance_id, **identifiers}
    print(json.dumps(event, separators=(",", ":")), flush=True)
    # The harness kills this process after observing the flushed line.
    while True:
        time.sleep(3600)


def postgres_ready() -> bool:
    # Lazy import keeps pure contract tests independent of the DB driver.
    # This connection is independent of the processing loop and its settings.
    try:
        import psycopg
        with psycopg.connect("", autocommit=True, connect_timeout=2,
                             options="-c statement_timeout=1500 -c lock_timeout=1000") as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone() == (1,)
    except Exception:
        return False


def observability_response(path: str, readiness) -> tuple[int, str, bytes]:
    if path == "/health/live":
        return 200, "application/json", LIVE_BODY
    if path == "/metrics":
        return 200, OPENMETRICS_CONTENT_TYPE, PROCESS_METRICS
    if path == "/health/ready":
        try:
            ready = readiness() is True
        except Exception:
            ready = False
        return (200, "application/json", READY_BODY) if ready else (503, "application/json", NOT_READY_BODY)
    return 404, "application/json", b""


class ObservabilityHandler(BaseHTTPRequestHandler):
    readiness = staticmethod(lambda: False)

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(3)

    def log_message(self, format: str, *args: object) -> None:
        pass

    def _write_observability(self, response: tuple[int, str, bytes]) -> None:
        status, content_type, body = response
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self._write_observability(observability_response(self.path, self.readiness))

    def do_POST(self) -> None:
        self._write_observability((404, "application/json", b""))

    do_PUT = do_PATCH = do_DELETE = do_HEAD = do_POST


class ObservabilityServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address) -> None:
        # Socket/probe failures must not dump exception details or requests.
        pass


@contextmanager
def observability_server(readiness, port: int = OBSERVABILITY_PORT):
    handler = type("ServiceObservabilityHandler", (ObservabilityHandler,),
                   {"readiness": staticmethod(readiness)})
    # Bind synchronously: failure prevents the processing loop from starting.
    server = ObservabilityServer(("0.0.0.0", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    started = False
    try:
        thread.start()
        started = True
        yield server
    finally:
        if started:
            server.shutdown()
        server.server_close()
        if started:
            thread.join(timeout=3)

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


def integer_setting(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    if re.fullmatch(r"[0-9]{1,9}", raw) is None:
        raise ValueError(name + ": invalid integer configuration")
    value = int(raw, 10)
    if not minimum <= value <= maximum:
        raise ValueError(name + ": integer configuration out of range")
    return value


@dataclass(frozen=True)
class DispatcherSettings:
    timeout_ms: int
    lease_ms: int
    poll_ms: int
    max_attempts: int


def dispatcher_settings() -> DispatcherSettings:
    test = os.environ.get("COURSE_TEST_PROFILE") == "1"
    settings = DispatcherSettings(
        integer_setting("COURSE_PROVIDER_TIMEOUT_MS", 500 if test else 10000, 1, 300000),
        integer_setting("COURSE_OUTBOX_LEASE_MS", 2000 if test else 20000, 100, 300000),
        integer_setting("COURSE_OUTBOX_POLL_MS", 100 if test else 400, 1, 60000),
        integer_setting("COURSE_OUTBOX_MAX_ATTEMPTS", 4, 1, 100),
    )
    if settings.timeout_ms >= settings.lease_ms:
        raise ValueError("COURSE_PROVIDER_TIMEOUT_MS must be less than COURSE_OUTBOX_LEASE_MS")
    # SQL owns reservations/retries. These values validate deployment consistency;
    # there is no independent Python attempt counter or inner HTTP retry loop.
    return settings


def inbox_poll_ms() -> int:
    return integer_setting("COURSE_INBOX_POLL_MS",
                           100 if os.environ.get("COURSE_TEST_PROFILE") == "1" else 400,
                           1, 60000)


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
