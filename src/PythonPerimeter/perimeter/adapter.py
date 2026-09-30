from __future__ import annotations

import http.client
import os
import socket
import urllib.parse

from .common import (
    ContractError,
    MAX_CALLBACK_BODY,
    compact_json_bytes,
    hmac_signature,
    parse_json_object,
    receipt_from_legacy,
    ADAPTER_PORT,
    ObservabilityHandler,
    ObservabilityServer,
    integer_setting,
)


_DEPENDENCY_ERROR = (
    b'{"status":"error",'
    b'"code":"dependency.unavailable"}'
)


def _api_timeout() -> float:
    return (
        2.0
        if os.getenv(
            "COURSE_TEST_PROFILE",
            "0",
        ) == "1"
        else 10.0
    )


def _gateway_endpoint() -> tuple[str, int, str]:
    try:
        parsed = urllib.parse.urlsplit(os.environ.get(
            "RECEIPT_API_URL", "http://gateway:8080/api/receipt/accept"))
        port = parsed.port if parsed.port is not None else 80
        if (parsed.scheme != "http" or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or not 1 <= port <= 65535):
            raise ValueError()
        return parsed.hostname, port, parsed.path or "/"
    except ValueError:
        raise OSError("invalid API URL") from None


def gateway_ready() -> bool:
    connection = None
    try:
        host, port, _ = _gateway_endpoint()
        connection = http.client.HTTPConnection(host, port, timeout=2)
        # This request carries no callback body, credentials, or HMAC headers.
        connection.request("GET", "/health/ready")
        response = connection.getresponse()
        body = response.read(2049)
        return (response.status == 200 and len(body) <= 2048
                and (response.getheader("Content-Type") or "").split(";", 1)[0].strip().lower() == "application/json"
                and parse_json_object(body) == {"status": "ready"})
    except Exception:
        return False
    finally:
        if connection is not None:
            connection.close()


def _post_receipt(
    body: bytes,
    message_id: str,
) -> tuple[
    int,
    bytes,
    str | None,
]:
    host, port, path = _gateway_endpoint()

    token = os.environ[
        "PROVIDER_CALLBACK_TOKEN"
    ]

    secret = os.environ[
        "PROVIDER_HMAC_SECRET"
    ]

    signature = hmac_signature(
        secret,
        body,
    )

    connection = (
        http.client.HTTPConnection(
            host,
            port,
            timeout=_api_timeout(),
        )
    )

    try:
        connection.putrequest(
            "POST",
            path,
            skip_accept_encoding=True,
        )

        connection.putheader(
            "Authorization",
            "Bearer " + token,
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
            message_id,
        )

        connection.putheader(
            "X-Action-Version",
            "1",
        )

        connection.putheader(
            "X-Provider-Signature",
            signature,
        )

        connection.endheaders(
            body
        )

        response = (
            connection.getresponse()
        )

        response_body = response.read(
            1024 * 1024
        )

        media_type = (
            response.getheader(
                "Content-Type"
            )
        )

        return (
            response.status,
            response_body,
            media_type,
        )
    finally:
        connection.close()


class CallbackHandler(
    ObservabilityHandler
):
    protocol_version = "HTTP/1.1"
    readiness = staticmethod(gateway_ready)

    def log_message(
        self,
        format: str,
        *args: object,
    ) -> None:
        return

    def _empty(
        self,
        status: int,
    ) -> None:
        self.send_response(
            status
        )

        self.send_header(
            "Content-Length",
            "0",
        )

        self.end_headers()

    def _dependency_error(
        self,
    ) -> None:
        self.send_response(
            503
        )

        self.send_header(
            "Content-Type",
            "application/json",
        )

        self.send_header(
            "Content-Length",
            str(
                len(
                    _DEPENDENCY_ERROR
                )
            ),
        )

        self.end_headers()

        self.wfile.write(
            _DEPENDENCY_ERROR
        )

    def do_GET(self) -> None:
        super().do_GET()

    def do_PUT(self) -> None:
        self._empty(
            404
        )

    def do_PATCH(self) -> None:
        self._empty(
            404
        )

    def do_DELETE(self) -> None:
        self._empty(
            404
        )

    def do_POST(self) -> None:
        capability = os.environ.get(
            "PROVIDER_CALLBACK_CAPABILITY",
            "",
        )

        expected_path = (
            "/callbacks/provider-v02/"
            + capability
        )

        if self.path != expected_path:
            self._empty(
                404
            )
            return

        content_types = (
            self.headers.get_all(
                "Content-Type"
            )
        )

        if (
            content_types is None
            or len(content_types) != 1
            or (
                content_types[0]
                .split(";", 1)[0]
                .strip()
                .lower()
                != "application/json"
            )
        ):
            self._empty(
                400
            )
            return

        lengths = self.headers.get_all(
            "Content-Length"
        )

        if (
            lengths is None
            or len(lengths) != 1
        ):
            self._empty(
                400
            )
            return

        length_text = lengths[0]

        if (
            not length_text.isascii()
            or not length_text.isdigit()
        ):
            self._empty(
                400
            )
            return

        length = int(
            length_text
        )

        if (
            length < 1
            or length > MAX_CALLBACK_BODY
        ):
            self.close_connection = True
            self._empty(
                400
            )
            return

        try:
            raw = self.rfile.read(
                length
            )

            if len(raw) != length:
                raise ContractError(
                    "truncated request"
                )

            callback = (
                parse_json_object(
                    raw
                )
            )

            receipt = (
                receipt_from_legacy(
                    callback
                )
            )

            body = (
                compact_json_bytes(
                    receipt
                )
            )

        except (
            ContractError,
            ValueError,
        ):
            self._empty(
                400
            )
            return

        try:
            (
                status,
                response_body,
                media_type,
            ) = _post_receipt(
                body,
                receipt["messageId"],
            )

        except (
            TimeoutError,
            socket.timeout,
            OSError,
            http.client.HTTPException,
        ):
            self._dependency_error()
            return

        self.send_response(
            status
        )

        if media_type:
            self.send_header(
                "Content-Type",
                media_type,
            )

        self.send_header(
            "Content-Length",
            str(
                len(
                    response_body
                )
            ),
        )

        self.end_headers()

        if response_body:
            self.wfile.write(
                response_body
            )


def main() -> None:
    for name in (
        "PROVIDER_CALLBACK_CAPABILITY",
        "PROVIDER_CALLBACK_TOKEN",
        "PROVIDER_HMAC_SECRET",
    ):
        if name not in os.environ:
            raise RuntimeError(
                f"{name} is required"
            )

    _gateway_endpoint()
    port = integer_setting("PORT", ADAPTER_PORT, 1, 65535)

    server = ObservabilityServer(
        (
            "0.0.0.0",
            port,
        ),
        CallbackHandler,
    )

    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
