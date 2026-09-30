from __future__ import annotations

import http.client
import json
import os
import socket
import time
import urllib.parse

import psycopg

from .common import (
    compact_json_bytes,
    dispatcher_settings,
    parse_provider_success,
    observability_server,
    postgres_ready,
    reach_failpoint,
)


def _sleep_interval() -> float:
    return dispatcher_settings().poll_ms / 1000.0


def _http_timeout() -> float:
    return dispatcher_settings().timeout_ms / 1000.0


def _provider_endpoint() -> tuple[
    str,
    int,
    str,
]:
    raw = os.environ.get(
        "PROVIDER_URL",
        "http://provider-simulator:8081",
    )

    parsed = urllib.parse.urlsplit(
        raw
    )

    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(
            "PROVIDER_URL is invalid"
        )

    port = parsed.port or 80

    base_path = parsed.path.rstrip("/")

    path = (
        base_path + "/payments"
        if base_path
        else "/payments"
    )

    return (
        parsed.hostname,
        port,
        path,
    )


def _provider_attempt(
    body: bytes,
    external_request_id: str,
    correlation_id: str,
) -> tuple[int, bytes]:
    host, port, path = (
        _provider_endpoint()
    )

    connection = (
        http.client.HTTPConnection(
            host,
            port,
            timeout=_http_timeout(),
        )
    )

    try:
        connection.putrequest(
            "POST",
            path,
            skip_accept_encoding=True,
        )

        connection.putheader(
            "Content-Type",
            "application/json",
        )

        connection.putheader(
            "Content-Length",
            str(len(body)),
        )

        connection.putheader(
            "Idempotency-Key",
            external_request_id,
        )

        connection.putheader(
            "X-Correlation-ID",
            correlation_id,
        )

        connection.endheaders(
            body
        )

        response = (
            connection.getresponse()
        )

        response_body = response.read(
            64 * 1024 + 1
        )

        return (
            response.status,
            response_body,
        )
    finally:
        connection.close()


def _process_claim(
    connection: psycopg.Connection,
    row: tuple,
    owner: str,
) -> None:
    (
        outbox_id,
        lease_version,
        external_request_id,
        correlation_id,
        amount,
        currency,
    ) = row

    provider_request = {
        "operationId":
            external_request_id,
        "amount":
            amount,
        "currency":
            currency,
    }

    body = compact_json_bytes(
        provider_request
    )

    reach_failpoint("after_outbox_claim", owner, outboxId=str(outbox_id),
                    externalRequestId=external_request_id)

    try:
        status, response_body = (
            _provider_attempt(
                body,
                external_request_id,
                str(correlation_id),
            )
        )
    except (
        TimeoutError,
        socket.timeout,
        OSError,
        http.client.HTTPException,
    ):
        error_code = (
            "transport.error.retryable"
        )

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT delivery.fail_outbox(
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    outbox_id,
                    owner,
                    lease_version,
                    error_code,
                ),
            )

        return

    reach_failpoint("after_provider_response", owner, outboxId=str(outbox_id),
                    externalRequestId=external_request_id)

    success, value = (
        parse_provider_success(
            status,
            response_body,
        )
    )

    with connection.cursor() as cur:
        if success:
            cur.execute(
                """
                SELECT delivery.succeed_outbox(
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    outbox_id,
                    owner,
                    lease_version,
                    value,
                ),
            )
        else:
            cur.execute(
                """
                SELECT delivery.fail_outbox(
                    %s,
                    %s,
                    %s,
                    %s
                );
                """,
                (
                    outbox_id,
                    owner,
                    lease_version,
                    value,
                ),
            )


def _run_connection(
    owner: str,
) -> None:
    interval = _sleep_interval()

    with psycopg.connect(
        "",
        autocommit=True,
    ) as connection:

        while True:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM delivery.claim_outbox(
                        %s,
                        %s
                    );
                    """,
                    (
                        owner,
                        1,
                    ),
                )

                row = cur.fetchone()

            if row is None:
                time.sleep(
                    interval
                )
                continue

            _process_claim(
                connection,
                row,
                owner,
            )


def main() -> None:
    owner = os.environ.get(
        "OUTBOX_OWNER",
        "outbox-dispatcher",
    )

    interval = _sleep_interval()

    with observability_server(postgres_ready):
        while True:
            try:
                _run_connection(
                    owner
                )
            except (
                psycopg.Error,
                OSError,
            ):
                time.sleep(
                    interval
                )


if __name__ == "__main__":
    main()
