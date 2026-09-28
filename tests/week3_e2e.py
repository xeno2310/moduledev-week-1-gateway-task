"""Participant-owned Week 3 E2E. Run from the repository root with Python 3.10+.

Only the standard library is used on the host. Docker commands always name a
fresh, generated Compose project. No checker code or reference checkout is used.
Raw subprocess output and HTTP bodies are deliberately never logged.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
PYTHON_SERVICES = ("outbox-dispatcher", "receipt-adapter", "inbox-reconciler")
WORKERS = ("worker-a", "worker-b")
SCENARIOS = {
    "success": "Provider success, HTTP/DB completion and one provider effect",
    "reject": "Provider rejection and exactly one rejection event",
    "lost-response": "Dispatcher retry with stable identity and one provider effect",
    "early-duplicate-conflict": "Durable receipt before wait, exact replay and conflict",
    "manual-concurrency": "Concurrent approve/reject with one authoritative decision",
    "stop-recreate": "Pending Outbox/Inbox survive Python stop/resume and recreate",
}

# Executed inside an existing Python service, not a mock HTTP server. Secrets and
# request bodies travel over stdin, never command arguments or terminal output.
INTERNAL_HTTP = r"""
import base64, json, sys, urllib.error, urllib.request
try:
    data = json.load(sys.stdin)
    body = base64.b64decode(data['body']) if data['body'] is not None else None
    request = urllib.request.Request(data['url'], data=body,
        headers=data['headers'], method=data['method'])
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        response = opener.open(request, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        result = {'status': response.code,
            'body': base64.b64encode(response.read(1048576)).decode('ascii')}
except Exception:
    result = {'status': 0, 'body': ''}
print(json.dumps(result))
"""


class CheckFailed(RuntimeError):
    """Messages must contain only locally authored, non-sensitive diagnostics."""


def require(condition: bool, label: str) -> None:
    if not condition:
        raise CheckFailed(label)


def compact(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def make_token(env: dict[str, str], principal: str, scopes: str) -> str:
    now = int(time.time())
    claims = {
        "iss": env["COURSE_JWT_ISSUER"], "aud": env["COURSE_JWT_AUDIENCE"],
        "sub": principal, "consumer": "week3-e2e", "scope": scopes,
        "iat": now, "nbf": now - 30, "exp": now + 7200,
    }
    unsigned = b64url(compact({"alg": "HS256", "typ": "JWT"})) + "." + b64url(compact(claims))
    signature = hmac.new(env["COURSE_JWT_SIGNING_KEY"].encode(), unsigned.encode(),
                         hashlib.sha256).digest()
    return unsigned + "." + b64url(signature)


def synthetic_environment() -> dict[str, str]:
    env = {f"COURSE_{name}_PASSWORD": secrets.token_hex(24)
           for name in ("POSTGRES", "MIGRATOR", "PUBLISHER", "RUNTIME", "WORKER", "OUTBOX", "INBOX")}
    env.update({
        "COURSE_JWT_ISSUER": "week3-e2e", "COURSE_JWT_AUDIENCE": "week3-e2e-api",
        "COURSE_JWT_SIGNING_KEY": secrets.token_hex(32),
        "PROVIDER_HMAC_SECRET": secrets.token_hex(32),
        "PROVIDER_CALLBACK_CAPABILITY": secrets.token_hex(24),
        "PROVIDER_AUDIT_TOKEN": secrets.token_hex(24),
        "COURSE_TEST_PROFILE": "1", "COURSE_FAILPOINT": "",
        "COURSE_GATEWAY_PORT": "0", "SIMULATOR_MODE": "success",
        "CALLBACK_DELAY": "200ms", "CALLBACK_TIMEOUT": "1s",
        "CALLBACK_MAX_ATTEMPTS": "100", "CALLBACK_RETRY_DELAY": "200ms",
        "PROVIDER_URL": "http://provider-simulator:8081",
        "RECEIPT_API_URL": "http://gateway:8080/api/receipt/accept",
        "OUTBOX_OWNER": "week3-e2e-dispatcher",
        "WORKFLOW_WORKER_SCOPES": "payment:read,payment:write,payment:internal,workflow:execute",
    })
    env["PROVIDER_CALLBACK_TOKEN"] = make_token(env, "receipt-provider", "receipt:write")
    return env


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def decode_json(raw: bytes | str, label: str) -> object:
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise CheckFailed(label + ": invalid JSON (body withheld)") from None


def response_json(raw: bytes) -> object:
    # Readiness and provider 404 responses may legitimately have no JSON body.
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        return None


class Stack:
    def __init__(self, directory: Path, timeout: float) -> None:
        self.project = "week3-e2e-" + uuid.uuid4().hex
        self.timeout = timeout
        self.cleanup_armed = False
        self.base_url = ""
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("COMPOSE_", "COURSE_", "PROVIDER_"))}
        self.env.update(synthetic_environment())
        self.client_token = make_token(self.env, "e2e-client", "payment:read payment:write")
        # Ignore a user's .env and implicit compose.override.yaml. Give shared
        # explicit image tags private names so builds cannot retag another stack.
        env_file = directory / "empty.env"
        env_file.write_text("", encoding="utf-8")
        override = directory / "images.json"
        override.write_text(json.dumps({"services": {
            name: {"image": self.project + ("-worker:local" if name in WORKERS else "-python:local")}
            for name in (*WORKERS, *PYTHON_SERVICES)
        }}), encoding="utf-8")
        self.compose_prefix = ["docker", "compose", "--project-name", self.project,
                               "--project-directory", str(ROOT), "--env-file", str(env_file),
                               "-f", str(ROOT / "compose.yaml"), "-f", str(override)]

    def run(self, command: list[str], label: str, stdin: str | None = None,
            timeout: float = 120) -> str:
        try:
            result = subprocess.run(command, cwd=ROOT, env=self.env, input=stdin,
                                    text=True, encoding="utf-8", errors="replace",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            raise CheckFailed(label + ": command timed out (output withheld)") from None
        except OSError:
            raise CheckFailed(label + ": could not start command") from None
        require(result.returncode == 0,
                label + f": command exited {result.returncode} (output withheld)")
        return result.stdout.strip()

    def compose(self, *args: str, stdin: str | None = None, timeout: float = 120) -> str:
        return self.run(self.compose_prefix + list(args), "Compose " + args[0], stdin, timeout)

    def until(self, label: str, probe, timeout: float | None = None):
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            result = probe()
            if result:
                return result
            if time.monotonic() >= deadline:
                raise CheckFailed(label + ": deadline exceeded")
            time.sleep(0.2)

    def start(self) -> None:
        label = "label=com.docker.compose.project=" + self.project
        for command in (["docker", "ps", "-aq", "--filter", label],
                        ["docker", "volume", "ls", "-q", "--filter", label],
                        ["docker", "network", "ls", "-q", "--filter", label]):
            require(not self.run(command, "Project isolation preflight"),
                    "Generated project already exists; refusing to own or clean it")
        self.cleanup_armed = True
        print("SETUP | building isolated Compose stack", flush=True)
        self.compose("build", "postgres", "api", "gateway", "cli", "worker-a",
                     "outbox-dispatcher", timeout=1200)
        self.compose("up", "-d", "--no-build", timeout=240)
        binding = self.compose("port", "gateway", "8080")
        require(binding.startswith("127.0.0.1:"), "Gateway must bind only to localhost")
        self.base_url = "http://" + binding
        self.until("Gateway readiness", lambda: self.http("GET", "/health/ready")[0] == 200,
                   timeout=120)
        self.wait_provider()
        for service in (*WORKERS, *PYTHON_SERVICES):
            self.container(service, running=True)

    def cleanup(self) -> None:
        if not self.cleanup_armed:
            return
        # No global prune, volume glob, image removal, or unrelated project name.
        self.compose("down", "--volumes", "--remove-orphans", "--timeout", "5", timeout=120)
        label = "label=com.docker.compose.project=" + self.project
        require(not self.run(["docker", "ps", "-aq", "--filter", label], "Cleanup containers"),
                "Project containers remain after cleanup")
        require(not self.run(["docker", "volume", "ls", "-q", "--filter", label], "Cleanup volumes"),
                "Project volume remains after cleanup")
        require(not self.run(["docker", "network", "ls", "-q", "--filter", label], "Cleanup networks"),
                "Project network remains after cleanup")
        print("CLEANUP | isolated project and volume removed", flush=True)

    def container(self, service: str, running: bool = False) -> tuple[str, str]:
        container = self.compose("ps", "-a", "-q", service)
        require(bool(container) and "\n" not in container, service + ": expected exactly one container")
        identity = self.run(["docker", "inspect", "--format",
                             "{{.Image}} {{.State.Running}}", container], service + " identity")
        parts = identity.split()
        require(len(parts) == 2, service + ": invalid container identity")
        image, is_running = parts
        require(not running or is_running == "true", service + ": container is not running")
        return container, image

    def http(self, method: str, path: str, body: bytes | None = None,
             headers: dict[str, str] | None = None) -> tuple[int, object]:
        request = urllib.request.Request(self.base_url + path, data=body,
                                         headers=headers or {}, method=method)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            try:
                response = opener.open(request, timeout=10)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return response.code, response_json(response.read(1048576))
        except (OSError, urllib.error.URLError):
            return 0, None

    def internal_http(self, method: str, url: str, body: bytes | None = None,
                      headers: dict[str, str] | None = None,
                      service: str = "receipt-adapter") -> tuple[int, object]:
        request = {"method": method, "url": url, "headers": headers or {},
                   "body": None if body is None else base64.b64encode(body).decode("ascii")}
        raw = self.compose("exec", "-T", service, "python", "-c", INTERNAL_HTTP,
                           stdin=json.dumps(request), timeout=20)
        result = decode_json(raw, "Internal HTTP transport")
        require(isinstance(result, dict), "Internal HTTP transport envelope")
        payload = base64.b64decode(result["body"])
        return result["status"], response_json(payload)

    def action(self, module: str, action: str, payload: dict,
               token: str | None = None, key: str | None = None) -> tuple[int, dict]:
        status, body = self.http("POST", f"/api/{module}/{action}", compact(payload), {
            "Authorization": "Bearer " + (token or self.client_token),
            "Content-Type": "application/json", "X-Action-Version": "1",
            "Idempotency-Key": key or uuid.uuid4().hex,
        })
        require(isinstance(body, dict), f"{module}.{action}: HTTP {status}, no JSON envelope")
        return status, body

    def ok_action(self, module: str, action: str, payload: dict) -> dict:
        status, body = self.action(module, action, payload)
        require(status == 200 and body.get("status") == "ok",
                f"{module}.{action}: expected successful HTTP 200, received HTTP {status}")
        require(isinstance(body.get("result"), dict), "HTTP action result must be an object")
        return body["result"]

    def signed_receipt(self, receipt: dict) -> tuple[int, object]:
        body = compact(receipt)
        signature = hmac.new(self.env["PROVIDER_HMAC_SECRET"].encode(), body, hashlib.sha256).hexdigest()
        return self.http("POST", "/api/receipt/accept", body, {
            "Authorization": "Bearer " + self.env["PROVIDER_CALLBACK_TOKEN"],
            "Content-Type": "application/json", "X-Action-Version": "1",
            "Idempotency-Key": uuid.uuid4().hex, "X-Provider-Signature": "v1=" + signature,
        })

    def sql(self, sql: str) -> object:
        # Bound the server session as well as the Docker client: terminating a
        # timed-out docker exec alone need not cancel its PostgreSQL statement.
        bounded_sql = "SET statement_timeout='15s'; SET lock_timeout='5s'; " + sql
        raw = self.compose("exec", "-T", "postgres", "psql", "-X", "-q", "-A", "-t",
                           "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "course",
                           stdin=bounded_sql, timeout=30)
        return decode_json(raw, "SQL result")

    def rows(self, query: str) -> list[dict]:
        value = self.sql("SELECT coalesce(jsonb_agg(to_jsonb(evidence)), '[]'::jsonb) "
                         "FROM (" + query + ") AS evidence;")
        require(isinstance(value, list), "SQL evidence must be an array")
        return value

    def process(self, process_id: str) -> dict:
        rows = self.rows("SELECT * FROM autocheck.processes WHERE process_id=" + sql_text(process_id))
        require(len(rows) == 1, "Expected one pinned process")
        return rows[0]

    def external(self, operation_id: str) -> dict:
        rows = self.rows("SELECT * FROM autocheck.external_requests WHERE operation_id=" + sql_text(operation_id))
        require(len(rows) == 1, "Expected exactly one external request")
        return rows[0]

    def outbox(self, external_id: str) -> dict:
        rows = self.rows("SELECT * FROM autocheck.outbox WHERE external_request_id=" + sql_text(external_id))
        require(len(rows) == 1, "Expected exactly one outbox row")
        return rows[0]

    def receipt_rows(self, external_id: str) -> list[dict]:
        return self.rows("SELECT r.*, i.state AS inbox_state, i.applied_at AS inbox_applied_at, "
                         "i.body_hash AS inbox_body_hash FROM autocheck.receipts r "
                         "JOIN autocheck.inbox i USING (message_id) WHERE r.external_request_id="
                         + sql_text(external_id))

    def audit(self, external_id: str, service: str = "receipt-adapter") -> tuple[int, object]:
        return self.internal_http("GET", "http://provider-simulator:8081/internal/audit/"
                                  + urllib.parse.quote(external_id, safe=""),
                                  headers={"X-Audit-Token": self.env["PROVIDER_AUDIT_TOKEN"]},
                                  service=service)

    def wait_provider(self) -> None:
        self.container("provider-simulator", running=True)
        self.container("receipt-adapter", running=True)
        probe_id = "absent-" + uuid.uuid4().hex
        last_status = 0

        def probe():
            nonlocal last_status
            try:
                last_status = self.audit(probe_id)[0]
            except CheckFailed:
                return False
            return last_status in (200, 404)

        try:
            self.until("Provider audit readiness", probe, timeout=60)
        except CheckFailed:
            raise CheckFailed("Provider audit readiness: deadline exceeded; "
                              f"lastStatus={last_status}") from None

    def provider_mode(self, mode: str) -> None:
        # All earlier scenarios have settled before resetting this volatile
        # external simulator. NEVER recreate it during a recovery scenario.
        self.compose("stop", "--timeout", "3", "outbox-dispatcher")
        self.env["SIMULATOR_MODE"] = mode
        self.compose("up", "-d", "--no-build", "--no-deps", "--force-recreate", "provider-simulator")
        self.wait_provider()

    def submit(self, kind: str = "PAYMENT_EXECUTION", amount: str = "123.45") -> tuple[str, str]:
        operation = self.ok_action("payment", "request", {
            "operationKind": kind, "amount": amount, "currency": "RUB"})
        operation_id = str(uuid.UUID(operation["operationId"]))
        started = self.ok_action("payment", "submit", {"operationId": operation_id})
        require(started["status"] == "PROCESSING", "Submit must return pinned PROCESSING result")
        return operation_id, str(uuid.UUID(started["processId"]))

    def wait_state(self, process_id: str, state: str) -> dict:
        def probe():
            process = self.process(process_id)
            require(process["state"] != "FAILED", "Workflow unexpectedly failed")
            return process if process["state"] == state else None
        return self.until("Workflow " + state, probe)

    def event_counts(self, operation_id: str) -> dict[str, int]:
        return {row["event_type"]: row["count"] for row in self.rows(
            "SELECT event_type, count(*) AS count FROM autocheck.operation_events "
            "WHERE operation_id=" + sql_text(operation_id) + " GROUP BY event_type")}

    def assert_terminal(self, operation_id: str, process_id: str, status: str) -> None:
        self.wait_state(process_id, "COMPLETED")
        operation = self.ok_action("operation", "get", {"operationId": operation_id})
        require(operation["status"] == status, "HTTP operation status disagrees with expected outcome")
        rows = self.rows("SELECT status, process_id FROM autocheck.operations WHERE operation_id="
                         + sql_text(operation_id))
        require(len(rows) == 1 and rows[0] == {"status": status, "process_id": process_id},
                "SQL operation status or pinned process differs")
        events = self.event_counts(operation_id)
        other = "REJECTED" if status == "COMPLETED" else "COMPLETED"
        require(events.get("OPERATION_" + status, 0) == 1, "Expected one terminal operation event")
        require(events.get("OPERATION_" + other, 0) == 0, "Unexpected opposite terminal event")
        steps = self.rows("SELECT step_key, state FROM autocheck.steps WHERE process_id="
                          + sql_text(process_id) + " AND step_type='END'")
        require(steps == [{"step_key": "done", "state": "COMPLETED"}], "Expected one completed end step")
        completed = self.rows("SELECT event_id FROM autocheck.workflow_events WHERE process_id="
                              + sql_text(process_id) + " AND event_type='ProcessCompleted'")
        require(len(completed) == 1, "Expected exactly one completed workflow transition")

    def assert_delivery(self, operation_id: str, process_id: str, outcome: str,
                        minimum_requests: int = 1) -> dict:
        self.assert_terminal(operation_id, process_id, outcome)
        external_id = self.external(operation_id)["external_request_id"]
        require(self.outbox(external_id)["state"] == "CONFIRMED", "Outbox must be CONFIRMED")
        require(self.external(operation_id)["state"] == "CONFIRMED", "External request must be CONFIRMED")
        receipts = self.receipt_rows(external_id)
        require(len(receipts) == 1, "Expected one receipt and inbox row")
        receipt = receipts[0]
        require(receipt["outcome"] == outcome and receipt["signature_valid"] is True,
                "Receipt outcome/signature evidence differs")
        require(receipt["inbox_state"] == "APPLIED" and receipt["applied_at"] is not None
                and receipt["inbox_applied_at"] is not None, "Receipt must be durably applied")
        require(receipt["body_hash"] == receipt["inbox_body_hash"], "Receipt/inbox hashes differ")
        signals = self.rows("SELECT message_id, status FROM autocheck.signals WHERE process_id="
                            + sql_text(process_id))
        require(signals == [{"message_id": receipt["message_id"], "status": "APPLIED"}],
                "Expected one applied workflow signal")
        code, audit = self.audit(external_id)
        require(code == 200 and isinstance(audit, dict), "Provider audit is unavailable")
        require(audit.get("operationId") == external_id and audit.get("idempotencyKey") == external_id,
                "Provider idempotency identity changed")
        require(audit.get("paymentCount") == 1, "Provider effect count must equal one")
        require(type(audit.get("requestCount")) is int and audit["requestCount"] >= minimum_requests,
                "Provider audit does not prove required HTTP attempts")
        require(audit.get("providerPaymentId") == receipt["message_id"], "Provider/receipt identity differs")
        print(f"EVIDENCE | payment={outcome} workflow=COMPLETED inbox=APPLIED "
              f"outbox=CONFIRMED providerRequests={audit['requestCount']} providerPayments=1", flush=True)
        return audit

    def snapshot(self, operation_id: str, process_id: str) -> dict:
        # Compare durable identities, timestamps and hashes, not only counts.
        queries = {
            "operation": "SELECT * FROM autocheck.operations WHERE operation_id=" + sql_text(operation_id),
            "events": "SELECT * FROM autocheck.operation_events WHERE operation_id=" + sql_text(operation_id) + " ORDER BY event_id",
            "process": "SELECT * FROM autocheck.processes WHERE process_id=" + sql_text(process_id),
        }
        for view, order in (("steps", "step_instance_id"), ("jobs", "job_id"),
                            ("signals", "message_id"), ("workflow_events", "event_id")):
            queries[view] = f"SELECT * FROM autocheck.{view} WHERE process_id={sql_text(process_id)} ORDER BY {order}"
        result = {name: self.rows(query) for name, query in queries.items()}
        result["attempts"] = self.rows(
            "SELECT a.* FROM autocheck.attempts a JOIN autocheck.jobs j USING (job_id) "
            "WHERE j.process_id=" + sql_text(process_id) + " ORDER BY a.attempt_id")
        external_id = self.external(operation_id)["external_request_id"]
        result["external"] = self.external(operation_id)
        result["outbox"] = self.outbox(external_id)
        result["receipts"] = self.receipt_rows(external_id)
        return result

    def scenario_success(self) -> None:
        self.provider_mode("success")
        self.compose("start", "outbox-dispatcher")
        operation_id, process_id = self.submit()
        self.assert_delivery(operation_id, process_id, "COMPLETED")

    def scenario_reject(self) -> None:
        self.provider_mode("reject")
        self.compose("start", "outbox-dispatcher")
        operation_id, process_id = self.submit()
        self.assert_delivery(operation_id, process_id, "REJECTED")
        steps = self.rows("SELECT step_key FROM autocheck.steps WHERE process_id=" + sql_text(process_id))
        require(any(row["step_key"] == "reject_operation" for row in steps)
                and not any(row["step_key"] == "complete_operation" for row in steps),
                "Provider rejection must traverse reject path")

    def scenario_lost_response(self) -> None:
        self.provider_mode("lost-response")
        # Hold callbacks away from the API until audit proves another provider
        # request. Otherwise CONFIRMED may legitimately suppress the retry.
        self.compose("stop", "--timeout", "3", "receipt-adapter")
        operation_id, process_id = self.submit()
        self.wait_state(process_id, "WAITING_SIGNAL")
        external_id = self.external(operation_id)["external_request_id"]
        self.compose("start", "outbox-dispatcher")

        def retried():
            code, audit = self.audit(external_id, service="inbox-reconciler")
            if code != 200 or not isinstance(audit, dict):
                return False
            require(audit.get("paymentCount") == 1, "Lost response duplicated provider effect")
            return type(audit.get("requestCount")) is int and audit["requestCount"] >= 2

        self.until("Lost-response provider retry", retried, timeout=45)
        require(self.outbox(external_id)["attempt_count"] >= 2, "SQL outbox must record retry")
        self.compose("start", "receipt-adapter")
        self.assert_delivery(operation_id, process_id, "COMPLETED", minimum_requests=2)
        require(self.external(operation_id)["external_request_id"] == external_id,
                "Retry created a different external request")

    def claim(self, process_id: str, expected_action: str) -> dict:
        rows = self.sql("SET ROLE workflow_worker; SELECT coalesce(jsonb_agg(jsonb_build_object("
                        "'job_id', job_id, 'process_id', process_id, 'execution_id', execution_id, "
                        "'attempt_id', attempt_id, 'lease_version', lease_version, 'action', action)), '[]') "
                        "FROM workflow.claim_jobs('e2e-early-window', 1, 2000);")
        require(isinstance(rows, list) and len(rows) == 1, "Early window must claim one real job")
        job = rows[0]
        require(job["process_id"] == process_id and job["action"] == expected_action,
                "Early window claimed an unexpected job")
        return job

    def invoke_claimed(self, operation_id: str, job: dict, finish: bool) -> None:
        context = {
            "principal": "workflow-worker", "consumer": "workflow-worker",
            "scopes": self.env["WORKFLOW_WORKER_SCOPES"].split(","),
            "requestId": job["execution_id"], "executionId": job["execution_id"],
            "processId": job["process_id"], "jobId": job["job_id"], "attemptId": job["attempt_id"],
            "correlationId": str(uuid.uuid4()),
        }
        invoke = ("api.invoke('payment', " + sql_text(job["action"]) + ", 1, "
                  + sql_text(compact(context).decode()) + "::jsonb, "
                  + sql_text(compact({"operationId": operation_id}).decode()) + "::jsonb)")
        if finish:
            query = ("WITH invoked AS MATERIALIZED (SELECT " + invoke + " AS envelope) "
                     "SELECT workflow.finish_job(" + sql_text(job["job_id"]) + "::uuid, "
                     "'e2e-early-window', " + str(job["lease_version"])
                     + ", envelope->>'outcome', envelope->'result') FROM invoked "
                     "WHERE envelope->>'status'='ok';")
        else:
            query = "SELECT " + invoke + ";"
        result = self.sql("BEGIN; SET LOCAL ROLE workflow_worker; " + query + " COMMIT;")
        require(isinstance(result, dict) and result.get("status") == "ok", "Early SQL boundary failed")

    def scenario_early_duplicate_conflict(self) -> None:
        self.provider_mode("success")
        self.compose("stop", "--timeout", "3", *WORKERS)
        operation_id, process_id = self.submit()
        # The ordinary worker commits prepare + finish + entry into wait in ONE
        # transaction. Services alone cannot pause between those durable states.
        # Stage the exceptional order via existing public SQL boundaries only:
        # finish validate; commit prepare under its real claimed executionId but
        # deliberately leave its job unfinished. No tables/maps/functions change.
        self.invoke_claimed(operation_id, self.claim(process_id, "validate"), finish=True)
        prepared_job = self.claim(process_id, "prepare_external")
        self.invoke_claimed(operation_id, prepared_job, finish=False)
        external_id = self.external(operation_id)["external_request_id"]
        self.compose("start", "outbox-dispatcher")
        self.until("Early durable receipt", lambda: self.receipt_rows(external_id))
        before = self.receipt_rows(external_id)
        require(len(before) == 1 and before[0]["inbox_state"] == "RECEIVED"
                and before[0]["applied_at"] is None, "Receipt was not durably saved before application")
        process = self.process(process_id)
        require(process["state"] == "RUNNING" and process["current_step_key"] == "prepare_external_request",
                "Early receipt was not received before the wait step")
        require(not self.rows("SELECT * FROM autocheck.signals WHERE process_id=" + sql_text(process_id)),
                "Early receipt unexpectedly already produced a signal")
        self.compose("start", *WORKERS)
        self.assert_delivery(operation_id, process_id, "COMPLETED")
        require(self.external(operation_id)["external_request_id"] == external_id,
                "Reclaimed prepare created another external request")
        applied = self.receipt_rows(external_id)[0]
        require(all(applied[key] == before[0][key]
                    for key in ("message_id", "body_hash", "received_at")),
                "Early receipt identity/evidence changed during application")

        # Read the preserved provider timestamp to replay the exact legacy callback
        # through the real adapter. It is not exposed by the stable receipt view.
        receipts = self.rows("SELECT provider_payment_id, occurred_at_text FROM payment.receipts "
                             "WHERE external_request_id=" + sql_text(external_id))
        require(len(receipts) == 1, "Expected one callback identity")
        callback = {"providerPaymentId": receipts[0]["provider_payment_id"],
                    "operationId": external_id, "result": "COMPLETED", "message": "",
                    "occurredAt": receipts[0]["occurred_at_text"]}
        original = self.snapshot(operation_id, process_id)
        receipt = {"version": 1, "messageId": callback["providerPaymentId"],
                   "externalRequestId": external_id, "providerPaymentId": callback["providerPaymentId"],
                   "outcome": callback["result"], "occurredAt": callback["occurredAt"]}
        require(hashlib.sha256(compact(receipt)).hexdigest() == before[0]["body_hash"],
                "Replay must reconstruct exact signed receipt bytes")
        callback_url = "http://receipt-adapter:8080/callbacks/provider-v02/" + self.env["PROVIDER_CALLBACK_CAPABILITY"]
        code, body = self.internal_http("POST", callback_url, compact(callback), {"Content-Type": "application/json"})
        require(code == 200 and isinstance(body, dict) and body.get("status") == "ok", "Exact callback replay failed")
        require(self.snapshot(operation_id, process_id) == original, "Duplicate changed durable history")
        # A fresh command key reaches receipt.accept's own messageId deduplication
        # instead of returning only api.invoke's cached idempotent response.
        code, body = self.signed_receipt(receipt)
        require(code == 200 and isinstance(body, dict) and body.get("outcome") == "DUPLICATE",
                "Receipt messageId duplicate must reach the domain duplicate path")
        require(self.snapshot(operation_id, process_id) == original, "Domain duplicate changed durable history")
        callback["result"] = "REJECTED"
        code, body = self.internal_http("POST", callback_url, compact(callback), {"Content-Type": "application/json"})
        require(code == 409 and isinstance(body, dict) and body.get("code") == "idempotency.conflict",
                "Conflicting callback must return contractual conflict")
        require(self.snapshot(operation_id, process_id) == original, "Conflicting callback changed evidence")
        receipt["outcome"] = "REJECTED"
        code, body = self.signed_receipt(receipt)
        require(code == 409 and isinstance(body, dict) and body.get("code") == "idempotency.conflict",
                "Receipt messageId conflict must survive a fresh command key")
        require(self.snapshot(operation_id, process_id) == original, "Domain conflict changed evidence")
        self.assert_delivery(operation_id, process_id, "COMPLETED")

    def scenario_manual_concurrency(self) -> None:
        operation_id, process_id = self.submit("PAYMENT_APPROVAL", "100001.00")
        self.wait_state(process_id, "WAITING_MANUAL")
        steps = self.rows("SELECT step_instance_id FROM autocheck.steps WHERE process_id="
                          + sql_text(process_id) + " AND step_type='MANUAL' AND state='WAITING'")
        require(len(steps) == 1, "Expected one waiting manual step")
        step_id = steps[0]["step_instance_id"]
        barrier = threading.Barrier(2)

        def decide(decision: str):
            principal = "e2e-" + decision.lower()
            reason = "Synthetic " + decision.lower()
            token = make_token(self.env, principal, "workflow:manual")
            barrier.wait(timeout=10)
            code, body = self.action("workflow", "manual", {
                "processId": process_id, "stepInstanceId": step_id,
                "decision": decision, "reason": reason}, token=token)
            return decision, principal, reason, code, body

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(decide, value) for value in ("APPROVED", "REJECTED")]
            results = [future.result(timeout=30) for future in futures]
        winners = [item for item in results if item[3] == 200 and item[4].get("status") == "ok"]
        losers = [item for item in results if item[3] == 409 and item[4].get("code") == "workflow.decision_conflict"]
        require(len(winners) == 1 and len(losers) == 1, "Manual race must have one winner and one contractual conflict")
        decision, principal, reason, _, envelope = winners[0]
        final_status = "COMPLETED" if decision == "APPROVED" else "REJECTED"
        self.assert_terminal(operation_id, process_id, final_status)
        decisions = self.rows("SELECT * FROM autocheck.decisions WHERE process_id=" + sql_text(process_id))
        require(len(decisions) == 1, "Manual race must persist one authoritative decision")
        saved = decisions[0]
        require(saved["decision_id"] == envelope["result"]["decisionId"]
                and saved["step_instance_id"] == step_id and saved["source"] == "MANUAL"
                and saved["principal"] == principal and saved["outcome"] == decision
                and saved["reason_hash"] == hashlib.sha256(reason.encode()).hexdigest(),
                "Authoritative decision differs from winning HTTP response")
        events = self.rows("SELECT event_id FROM autocheck.workflow_events WHERE process_id="
                           + sql_text(process_id) + " AND event_type='ManualDecided'")
        require(len(events) == 1, "Manual race appended extra decisions/transitions")
        branches = self.rows("SELECT step_key, outcome FROM autocheck.steps WHERE process_id="
                             + sql_text(process_id) + " AND step_key IN ('approve_operation', 'reject_operation')")
        expected_step = "approve_operation" if decision == "APPROVED" else "reject_operation"
        require(branches == [{"step_key": expected_step, "outcome": decision}],
                "Manual workflow did not follow only the winning branch")
        require(not self.rows("SELECT * FROM autocheck.external_requests WHERE operation_id="
                              + sql_text(operation_id)), "Manual review must not call provider")
        print(f"EVIDENCE | manualWinner={decision} decisions=1 conflicts=1 payment={final_status}", flush=True)

    def scenario_stop_recreate(self) -> None:
        self.provider_mode("success")
        self.compose("stop", "--timeout", "3", "outbox-dispatcher", "inbox-reconciler")
        provider_before = self.container("provider-simulator", running=True)
        operation_a, process_a = self.submit()
        self.wait_state(process_a, "WAITING_SIGNAL")
        external_a = self.external(operation_a)["external_request_id"]
        require(self.outbox(external_a)["state"] == "PENDING" and self.audit(external_a)[0] == 404,
                "Stopped dispatcher must leave durable, unsent outbox")
        self.compose("start", "outbox-dispatcher")
        self.until("Receipt saved while reconciler stopped", lambda: self.receipt_rows(external_a))
        receipt = self.receipt_rows(external_a)
        require(len(receipt) == 1 and receipt[0]["inbox_state"] == "RECEIVED"
                and receipt[0]["applied_at"] is None, "Stopped reconciler lost pending inbox window")
        require(self.process(process_a)["state"] == "WAITING_SIGNAL", "Receipt applied with reconciler stopped")
        code, audit_before = self.audit(external_a)
        require(code == 200 and isinstance(audit_before, dict) and audit_before.get("paymentCount") == 1,
                "Provider must have one effect before recreate")
        self.compose("stop", "--timeout", "3", "outbox-dispatcher", "receipt-adapter")
        # A has durable Inbox; B has durable, unsent Outbox at the SAME restart.
        operation_b, process_b = self.submit()
        self.wait_state(process_b, "WAITING_SIGNAL")
        external_b = self.external(operation_b)["external_request_id"]
        pending_b = self.outbox(external_b)
        require(pending_b["state"] == "PENDING" and pending_b["attempt_count"] == 0,
                "Second delivery was sent while dispatcher stopped")
        before = {service: self.container(service) for service in PYTHON_SERVICES}
        self.compose("up", "-d", "--no-build", "--no-deps", "--force-recreate", *PYTHON_SERVICES)
        after = {service: self.container(service, running=True) for service in PYTHON_SERVICES}
        require(all(before[name][0] != after[name][0] and before[name][1] == after[name][1]
                    for name in PYTHON_SERVICES), "All three Python services must recreate with unchanged images")
        require(self.container("provider-simulator", running=True) == provider_before,
                "Provider audit memory was reset during recovery")
        self.wait_provider()
        audit_after = self.assert_delivery(operation_a, process_a, "COMPLETED")
        self.assert_delivery(operation_b, process_b, "COMPLETED")
        require(audit_after["requestCount"] == audit_before["requestCount"], "Confirmed outbox was redelivered after restart")
        require(self.outbox(external_b)["outbox_id"] == pending_b["outbox_id"], "Pending outbox identity changed")
        final_receipt = self.receipt_rows(external_a)[0]
        require(final_receipt["message_id"] == receipt[0]["message_id"]
                and final_receipt["body_hash"] == receipt[0]["body_hash"]
                and final_receipt["received_at"] == receipt[0]["received_at"], "Pending inbox identity/evidence changed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List scenarios without Docker or file writes")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS, help="Run selected scenario(s); default: all six")
    parser.add_argument("--timeout", type=float, default=90, help="State convergence deadline in seconds (default: 90)")
    args = parser.parse_args()
    if args.list:
        for name, description in SCENARIOS.items():
            print(name + " | " + description)
        return 0
    require(Path.cwd().resolve() == ROOT, "Run this script from the repository root")
    require(10 <= args.timeout <= 300, "--timeout must be between 10 and 300 seconds")
    selected = list(dict.fromkeys(args.scenario or SCENARIOS))
    passed = 0
    failure = False

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix="week3-e2e-") as directory:
            stack = Stack(Path(directory), args.timeout)
            try:
                print("PROJECT | " + stack.project, flush=True)
                stack.start()
                for name in selected:
                    print("RUN | " + name, flush=True)
                    getattr(stack, "scenario_" + name.replace("-", "_"))()
                    passed += 1
                    print("PASS | " + name, flush=True)
            except CheckFailed as error:
                failure = True
                print("FAIL | " + str(error), flush=True)
            except KeyboardInterrupt:
                failure = True
                print("FAIL | interrupted; cleaning isolated stack", flush=True)
            except Exception as error:
                failure = True
                print("FAIL | unexpected " + type(error).__name__ + " (details withheld)", flush=True)
            finally:
                # A second Ctrl+C must not interrupt removal of our project.
                old_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                try:
                    stack.cleanup()
                except CheckFailed as error:
                    failure = True
                    print("FAIL | cleanup: " + str(error), flush=True)
                    print("CLEANUP REQUIRED | project=" + stack.project, flush=True)
                except Exception as error:
                    failure = True
                    print("FAIL | cleanup: unexpected " + type(error).__name__
                          + " (details withheld)", flush=True)
                    print("CLEANUP REQUIRED | project=" + stack.project, flush=True)
                finally:
                    signal.signal(signal.SIGINT, old_int)
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(f"WEEK3 E2E | passed={passed} total={len(selected)} "
          f"result={'FAILED' if failure else 'PASSED'}", flush=True)
    return 1 if failure else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CheckFailed as error:
        print("FAIL | " + str(error))
        raise SystemExit(1)
