from __future__ import annotations

import hashlib
import hmac
import os
import io
import json
from contextlib import redirect_stdout
import importlib
from types import SimpleNamespace
from email.message import Message
import unittest
from unittest.mock import patch, MagicMock, Mock
from . import common, adapter

from .common import (
    ContractError,
    integer_setting,
    dispatcher_settings,
    inbox_poll_ms,
    classify_http_status,
    compact_json_bytes,
    hmac_signature,
    parse_provider_success,
    receipt_from_legacy,
)


class CommonContractTests(
    unittest.TestCase
):
    def test_provider_request_bytes(
        self,
    ) -> None:
        value = {
            "operationId":
                "external-123",
            "amount":
                "1000.00",
            "currency":
                "RUB",
        }

        self.assertEqual(
            compact_json_bytes(value),
            (
                b'{"amount":"1000.00",'
                b'"currency":"RUB",'
                b'"operationId":"external-123"}'
            ),
        )

    def test_receipt_mapping_and_bytes(
        self,
    ) -> None:
        legacy = {
            "providerPaymentId":
                "provider-123",
            "operationId":
                "external-123",
            "result":
                "COMPLETED",
            "message":
                "discard me",
            "occurredAt":
                "2026-09-04T12:00:00.123456789Z",
        }

        receipt = receipt_from_legacy(
            legacy
        )

        self.assertEqual(
            receipt["messageId"],
            "provider-123",
        )

        self.assertEqual(
            receipt["providerPaymentId"],
            "provider-123",
        )

        self.assertEqual(
            receipt["externalRequestId"],
            "external-123",
        )

        self.assertEqual(
            receipt["occurredAt"],
            legacy["occurredAt"],
        )

        self.assertNotIn(
            "message",
            receipt,
        )

        self.assertEqual(
            compact_json_bytes(
                receipt
            ),
            (
                b'{"externalRequestId":"external-123",'
                b'"messageId":"provider-123",'
                b'"occurredAt":"2026-09-04T12:00:00.123456789Z",'
                b'"outcome":"COMPLETED",'
                b'"providerPaymentId":"provider-123",'
                b'"version":1}'
            ),
        )

    def test_hmac_exact_utf8_secret(
        self,
    ) -> None:
        secret = "  key ü  "
        body = b'{"x":1}'

        expected = hmac.new(
            secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()

        self.assertEqual(
            hmac_signature(
                secret,
                body,
            ),
            "v1=" + expected,
        )

    def test_unknown_callback_field_rejected(
        self,
    ) -> None:
        legacy = {
            "providerPaymentId": "p",
            "operationId": "o",
            "result": "COMPLETED",
            "message": "",
            "occurredAt":
                "2026-09-04T12:00:00Z",
            "extra": True,
        }

        with self.assertRaises(
            ContractError
        ):
            receipt_from_legacy(
                legacy
            )

    def test_crlf_rejected(
        self,
    ) -> None:
        legacy = {
            "providerPaymentId":
                "p\r\nx",
            "operationId":
                "o",
            "result":
                "COMPLETED",
            "message":
                "",
            "occurredAt":
                "2026-09-04T12:00:00Z",
        }

        with self.assertRaises(
            ContractError
        ):
            receipt_from_legacy(
                legacy
            )

    def test_http_classification(
        self,
    ) -> None:
        self.assertEqual(
            classify_http_status(408),
            "http.408.retryable",
        )

        self.assertEqual(
            classify_http_status(429),
            "http.429.retryable",
        )

        self.assertEqual(
            classify_http_status(503),
            "http.503.retryable",
        )

        self.assertEqual(
            classify_http_status(400),
            "http.400.terminal",
        )

        self.assertEqual(
            classify_http_status(302),
            "http.302.terminal",
        )

    def test_strict_provider_success(
        self,
    ) -> None:
        ok, value = (
            parse_provider_success(
                202,
                (
                    b'{"providerPaymentId":"p-1",'
                    b'"status":"ACCEPTED"}'
                ),
            )
        )

        self.assertTrue(ok)
        self.assertEqual(
            value,
            "p-1",
        )

        ok, value = (
            parse_provider_success(
                202,
                (
                    b'{"providerPaymentId":"p-1",'
                    b'"status":"ACCEPTED",'
                    b'"extra":1}'
                ),
            )
        )

        self.assertFalse(ok)
        self.assertEqual(
            value,
            "response.invalid.terminal",
        )

    def test_invalid_utf8_provider_response(
        self,
    ) -> None:
        ok, value = (
            parse_provider_success(
                202,
                b"\xff",
            )
        )

        self.assertFalse(ok)
        self.assertEqual(
            value,
            "response.invalid.terminal",
        )


class ConfigurationTests(unittest.TestCase):
    def test_failpoint_is_exact_profile_gated_and_logs_only_safe_ids(self) -> None:
        from .common import reach_failpoint
        output = io.StringIO()
        for profile, requested in (("0", "after_outbox_claim"),
                                   ("1", "unknown"), ("1", "after_provider_response")):
            with patch.dict(os.environ, {"COURSE_TEST_PROFILE": profile,
                                        "COURSE_FAILPOINT": requested}):
                with redirect_stdout(output), patch("time.sleep") as sleep:
                    reach_failpoint("after_outbox_claim", "dispatcher-a", outboxId="outbox-1")
                    sleep.assert_not_called()
        self.assertEqual(output.getvalue(), "")
        with patch.dict(os.environ, {"COURSE_TEST_PROFILE": "1",
                                    "COURSE_FAILPOINT": "after_outbox_claim"}):
            with redirect_stdout(output), patch("time.sleep", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    reach_failpoint("after_outbox_claim", "dispatcher-a", outboxId="outbox-1")
        self.assertEqual(json.loads(output.getvalue()), {
            "event": "failpoint.reached", "name": "after_outbox_claim",
            "instanceId": "dispatcher-a", "outboxId": "outbox-1"})

    def test_profile_defaults_and_milliseconds(self) -> None:
        with patch.dict(os.environ, {"COURSE_TEST_PROFILE": "1"}, clear=True):
            settings = dispatcher_settings()
            self.assertEqual((settings.timeout_ms, settings.lease_ms, settings.poll_ms, settings.max_attempts),
                             (500, 2000, 100, 4))
            self.assertEqual(settings.timeout_ms / 1000.0, 0.5)
            self.assertEqual(inbox_poll_ms(), 100)
        with patch.dict(os.environ, {}, clear=True):
            settings = dispatcher_settings()
            self.assertEqual((settings.timeout_ms, settings.lease_ms, settings.poll_ms), (10000, 20000, 400))
            self.assertEqual(inbox_poll_ms(), 400)

    def test_explicit_overrides(self) -> None:
        with patch.dict(os.environ, {"COURSE_TEST_PROFILE": "1", "COURSE_PROVIDER_TIMEOUT_MS": "123",
                         "COURSE_OUTBOX_LEASE_MS": "1000", "COURSE_OUTBOX_POLL_MS": "37",
                         "COURSE_OUTBOX_MAX_ATTEMPTS": "2", "COURSE_INBOX_POLL_MS": "59"}, clear=True):
            settings = dispatcher_settings()
            self.assertEqual((settings.timeout_ms, settings.lease_ms, settings.poll_ms, settings.max_attempts),
                             (123, 1000, 37, 2))
            self.assertEqual(settings.timeout_ms / 1000.0, 0.123)
            self.assertEqual(inbox_poll_ms(), 59)

    def test_integer_bounds_and_decimal(self) -> None:
        for raw, expected in (("1", 1), ("60000", 60000), ("000100", 100)):
            with self.subTest(raw=raw), patch.dict(os.environ, {"TEST_SETTING": raw}, clear=True):
                self.assertEqual(integer_setting("TEST_SETTING", 50, 1, 60000), expected)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(integer_setting("TEST_SETTING", 50, 1, 60000), 50)

    def test_invalid_explicit_values_fail_closed_without_echo(self) -> None:
        for raw in ("", " ", "-1", "+1", "1.5", "1\n", "\uff11\uff12", "60001", "0", "9" * 100, "secret-canary"):
            with self.subTest(raw=raw), patch.dict(os.environ, {"TEST_SETTING": raw}, clear=True):
                with self.assertRaises(ValueError) as error:
                    integer_setting("TEST_SETTING", 50, 1, 60000)
                self.assertIn(str(error.exception), ("TEST_SETTING: invalid integer configuration",
                                                    "TEST_SETTING: integer configuration out of range"))

    def test_dispatcher_consistency_validation(self) -> None:
        for values in ({"COURSE_OUTBOX_MAX_ATTEMPTS": "0"}, {"COURSE_OUTBOX_MAX_ATTEMPTS": "101"},
                       {"COURSE_OUTBOX_LEASE_MS": "99"}, {"COURSE_PROVIDER_TIMEOUT_MS": "2000"}):
            with self.subTest(values=values), patch.dict(os.environ, {"COURSE_TEST_PROFILE": "1", **values}, clear=True):
                with self.assertRaises(ValueError):
                    dispatcher_settings()

    def test_reconciler_invalid_configuration(self) -> None:
        with patch.dict(os.environ, {"COURSE_INBOX_POLL_MS": ""}, clear=True):
            with self.assertRaises(ValueError):
                inbox_poll_ms()


class ObservabilityTests(unittest.TestCase):
    def handler(self, kind=common.ObservabilityHandler, path="/health/live", ready=lambda: True):
        handler = object.__new__(kind)
        handler.path = path
        handler.readiness = ready
        handler.wfile = io.BytesIO()
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        return handler

    def test_live_and_metrics_do_not_probe_dependencies(self):
        probe = Mock(side_effect=AssertionError("must not probe"))
        for path, media, expected in (
            ("/health/live", "application/json", b'{"status":"live"}'),
            ("/metrics", "application/openmetrics-text; version=1.0.0; charset=utf-8",
             b"# TYPE process_up gauge\nprocess_up 1\n# EOF\n"),
        ):
            self.assertEqual(common.observability_response(path, probe), (200, media, expected))
            self.assertNotIn(b"\r", expected)
            expected.decode("utf-8", errors="strict")
        probe.assert_not_called()

    def test_exact_ready_and_not_ready_wire_responses(self):
        for probe, status, body in (
            (lambda: True, 200, b'{"status":"ready"}'),
            (lambda: False, 503, b'{"status":"not_ready","code":"dependency.unavailable"}'),
            (Mock(side_effect=RuntimeError("secret-canary")), 503,
             b'{"status":"not_ready","code":"dependency.unavailable"}'),
        ):
            for kind in (common.ObservabilityHandler, adapter.CallbackHandler):
                handler = self.handler(kind, "/health/ready", probe)
                handler.do_GET()
                handler.send_response.assert_called_once_with(status)
                handler.send_header.assert_any_call("Content-Type", "application/json")
                handler.send_header.assert_any_call("Content-Length", str(len(body)))
                self.assertEqual(handler.wfile.getvalue(), body)

    def test_live_and_metrics_content_length(self):
        for path, expected in (("/health/live", common.LIVE_BODY), ("/metrics", common.PROCESS_METRICS)):
            handler = self.handler(path=path)
            handler.do_GET()
            handler.send_response.assert_called_once_with(200)
            handler.send_header.assert_any_call("Content-Length", str(len(expected)))
            self.assertEqual(handler.wfile.getvalue(), expected)

    def test_unknown_path_and_non_get_health_are_404(self):
        for kind in (common.ObservabilityHandler, adapter.CallbackHandler):
            handler = self.handler(kind, "/unknown", Mock(side_effect=AssertionError()))
            handler.do_GET()
            handler.send_response.assert_called_once_with(404)
            for method in ("do_POST", "do_PUT", "do_PATCH", "do_DELETE", "do_HEAD"):
                handler = self.handler(kind, "/health/ready", Mock(side_effect=AssertionError()))
                getattr(handler, method)()
                handler.send_response.assert_called_once_with(404)

    def test_postgres_probe_has_separate_bounded_connection(self):
        driver = SimpleNamespace(connect=MagicMock())
        cursor = driver.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (1,)
        with patch.dict("sys.modules", {"psycopg": driver}):
            self.assertTrue(common.postgres_ready())
        driver.connect.assert_called_once_with("", autocommit=True, connect_timeout=2,
                                               options="-c statement_timeout=1500 -c lock_timeout=1000")
        cursor.execute.assert_called_once_with("SELECT 1")
        driver.connect.return_value.__exit__.assert_called_once()

    def test_postgres_probe_failure_is_safe(self):
        driver = SimpleNamespace(connect=Mock(side_effect=RuntimeError("secret-canary")))
        with patch.dict("sys.modules", {"psycopg": driver}):
            self.assertFalse(common.postgres_ready())

    def test_gateway_readiness_uses_validated_target_without_credentials(self):
        with patch.dict(os.environ, {"RECEIPT_API_URL": "http://custom-gateway:8099/api/receipt/accept",
                                    "PROVIDER_CALLBACK_TOKEN": "secret-canary", "PROVIDER_HMAC_SECRET": "secret-canary"}, clear=True), \
                patch.object(adapter.http.client, "HTTPConnection") as factory:
            response = factory.return_value.getresponse.return_value
            response.status = 200
            response.getheader.return_value = "application/json; charset=utf-8"
            response.read.return_value = b'{"status":"ready"}'
            self.assertTrue(adapter.gateway_ready())
            factory.assert_called_once_with("custom-gateway", 8099, timeout=2)
            factory.return_value.request.assert_called_once_with("GET", "/health/ready")
            response.read.assert_called_once_with(2049)
            factory.return_value.close.assert_called_once()

    def test_gateway_probe_rejects_bad_status_body_type_and_exceptions(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(adapter.http.client, "HTTPConnection") as factory:
            response = factory.return_value.getresponse.return_value
            for status, body, media in ((503, b'{"status":"ready"}', "application/json"),
                                       (200, b'{"status":"live"}', "application/json"),
                                       (200, b'\xff', "application/json"),
                                       (200, b'{"status":"ready"}', "text/plain"),
                                       (200, b'x' * 2049, "application/json")):
                response.status, response.read.return_value, response.getheader.return_value = status, body, media
                self.assertFalse(adapter.gateway_ready())
            factory.side_effect = OSError("secret-canary")
            self.assertFalse(adapter.gateway_ready())

    def test_service_ports_and_gateway_config(self):
        self.assertEqual((common.OBSERVABILITY_PORT, common.ADAPTER_PORT), (8080, 8082))
        for url in ("http://user:secret@gateway", "https://gateway", "http://gateway:0", "http://gateway:99999",
                    "http://gateway?secret", "http://gateway/#fragment", "//gateway"):
            with patch.dict(os.environ, {"RECEIPT_API_URL": url}, clear=True):
                with self.assertRaisesRegex(OSError, "^invalid API URL$"):
                    adapter._gateway_endpoint()
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(adapter._gateway_endpoint(), ("gateway", 8080, "/api/receipt/accept"))

    def test_background_server_binds_synchronously_and_closes(self):
        with patch.object(common, "ObservabilityServer") as server, patch.object(common.threading, "Thread") as thread:
            probe = Mock()
            with common.observability_server(probe):
                self.assertEqual(server.call_args.args[0], ("0.0.0.0", 8080))
                self.assertTrue(thread.call_args.kwargs["daemon"])
                thread.return_value.start.assert_called_once()
            server.return_value.shutdown.assert_called_once()
            server.return_value.server_close.assert_called_once()
            thread.return_value.join.assert_called_once_with(timeout=3)
        with patch.object(common, "ObservabilityServer", side_effect=OSError("bind failed")), \
                patch.object(common.threading, "Thread") as thread:
            with self.assertRaises(OSError):
                with common.observability_server(Mock()):
                    self.fail("processing loop must not start")
            thread.assert_not_called()

    def test_processing_services_wire_only_postgres_readiness(self):
        # Import with a fake driver even on hosts where psycopg is not installed.
        driver = SimpleNamespace(Error=RuntimeError, connect=MagicMock())
        with patch.dict("sys.modules", {"psycopg": driver}), patch.dict(os.environ, {}, clear=True):
            for name in ("dispatcher", "reconciler"):
                module = importlib.import_module("." + name, __package__)
                with patch.object(module, "observability_server") as server, \
                        patch.object(module, "_run_connection", side_effect=KeyboardInterrupt):
                    with self.assertRaises(KeyboardInterrupt):
                        module.main()
                    server.assert_called_once_with(common.postgres_ready)
            driver.connect.assert_not_called()

    def test_callback_wire_bytes_signature_headers_and_response_are_preserved(self):
        callback = (b'{"providerPaymentId":"provider-123","operationId":"external-123",'
                    b'"result":"COMPLETED","message":"discard me",'
                    b'"occurredAt":"2026-09-04T12:00:00.123456789Z"}')
        expected = (b'{"externalRequestId":"external-123","messageId":"provider-123",'
                    b'"occurredAt":"2026-09-04T12:00:00.123456789Z","outcome":"COMPLETED",'
                    b'"providerPaymentId":"provider-123","version":1}')
        secret = " synthetic key ü "
        with patch.dict(os.environ, {"PROVIDER_CALLBACK_CAPABILITY": "synthetic-capability",
                                    "PROVIDER_CALLBACK_TOKEN": "synthetic-token",
                                    "PROVIDER_HMAC_SECRET": secret, "COURSE_TEST_PROFILE": "1"}, clear=True), \
                patch.object(adapter.http.client, "HTTPConnection") as factory:
            response = factory.return_value.getresponse.return_value
            response.status = 200
            response.read.return_value = b'{"status":"ok"}'
            response.getheader.return_value = "application/json"
            handler = self.handler(adapter.CallbackHandler, "/callbacks/provider-v02/synthetic-capability")
            handler.headers = Message()
            handler.headers["Content-Type"] = "application/json"
            handler.headers["Content-Length"] = str(len(callback))
            handler.rfile = io.BytesIO(callback)
            handler.do_POST()
            factory.assert_called_once_with("gateway", 8080, timeout=2.0)
            connection = factory.return_value
            connection.putrequest.assert_called_once_with("POST", "/api/receipt/accept", skip_accept_encoding=True)
            self.assertEqual(connection.putheader.call_args_list, [
                unittest.mock.call("Authorization", "Bearer synthetic-token"),
                unittest.mock.call("Content-Type", "application/json"),
                unittest.mock.call("Content-Length", str(len(expected))),
                unittest.mock.call("Idempotency-Key", "provider-123"),
                unittest.mock.call("X-Action-Version", "1"),
                unittest.mock.call("X-Provider-Signature", "v1=" + hmac.new(
                    secret.encode("utf-8"), expected, hashlib.sha256).hexdigest()),
            ])
            connection.endheaders.assert_called_once_with(expected)
            connection.close.assert_called_once()
            handler.send_response.assert_called_once_with(200)
            self.assertEqual(handler.wfile.getvalue(), b'{"status":"ok"}')

    def test_thread_start_failure_closes_socket_without_shutdown_deadlock(self):
        with patch.object(common, "ObservabilityServer") as server, patch.object(common.threading, "Thread") as thread:
            thread.return_value.start.side_effect = RuntimeError("thread unavailable")
            with self.assertRaises(RuntimeError):
                with common.observability_server(Mock()):
                    self.fail("processing loop must not start")
            server.return_value.shutdown.assert_not_called()
            server.return_value.server_close.assert_called_once()
            thread.return_value.join.assert_not_called()


if __name__ == "__main__":
    unittest.main()
