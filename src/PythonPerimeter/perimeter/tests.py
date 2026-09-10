from __future__ import annotations

import hashlib
import hmac
import unittest

from .common import (
    ContractError,
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


if __name__ == "__main__":
    unittest.main()
