"""Participant-owned Week 4 crash matrix. --list never starts Docker.

Each scenario owns a fresh Compose project and volume. The existing Week 3
stack supplies synthetic credentials, private image tags and public API calls.
Provider outage/recovery is also checked in after_outbox_claim; the permanent
lost-response regression is tests/week3_e2e.py::scenario_lost_response.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import uuid

from week3_e2e import (CheckFailed, ROOT, Stack as BaseStack, compact,
                       make_token, require, sql_text)


SCENARIOS = (
    "after_job_claim", "after_action_before_finish", "after_outbox_claim",
    "after_provider_response", "after_inbox_saved", "after_manual_decision",
)


class Stack(BaseStack):
    def __init__(self, directory: Path, timeout: float) -> None:
        super().__init__(directory, timeout)
        images = directory / "images.json"
        image_config = json.loads(images.read_text(encoding="utf-8"))
        for service in ("outbox-dispatcher-b", "inbox-reconciler-b"):
            image_config["services"][service] = {"image": self.project + "-python:local"}
        for service in ("postgres", "api", "gateway", "cli"):
            image_config["services"][service] = {"image": self.project + "-" + service + ":local"}
        images.write_text(json.dumps(image_config), encoding="utf-8")
        self.private_images = tuple(self.project + "-" + name + ":local"
                                    for name in ("postgres", "api", "gateway", "cli", "worker", "python"))
        self.failpoint_file = directory / "failpoint.json"
        self.failpoint_file.write_text('{"services":{}}', encoding="utf-8")
        self.compose_prefix += ["-f", str(self.failpoint_file)]

    def start(self) -> None:
        require(all(not self.image_exists(tag) for tag in self.private_images),
                "Generated project image tag already exists; refusing to own it")
        super().start()
        self.compose("up", "-d", "--no-build", "--no-deps",
                     "outbox-dispatcher-b", "inbox-reconciler-b")
        self.container("outbox-dispatcher-b", running=True)
        self.container("inbox-reconciler-b", running=True)

    def image_exists(self, tag: str) -> bool:
        result = subprocess.run(["docker", "image", "inspect", tag], cwd=ROOT,
                                env=self.env, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=30, check=False)
        return result.returncode == 0

    def cleanup(self) -> None:
        try:
            super().cleanup()
        finally:
            if self.cleanup_armed:
                for tag in self.private_images:
                    if self.image_exists(tag):
                        self.run(["docker", "image", "rm", tag], "Private image cleanup", timeout=120)
                require(all(not self.image_exists(tag) for tag in self.private_images),
                        "Private image tags remain after cleanup")
                print("CLEANUP | private image tags=0", flush=True)

    def set_failpoint(self, service: str, name: str) -> None:
        # Only one replica receives the point. Recreate just that process; no
        # provider or database recreation can erase the evidence being tested.
        self.failpoint_file.write_text(json.dumps({"services": {
            service: {"environment": {"COURSE_FAILPOINT": name}}
        }}), encoding="utf-8")
        self.compose("up", "-d", "--no-build", "--no-deps",
                     "--force-recreate", service)
        self.container(service, running=True)

    def acknowledgement(self, service: str, name: str) -> dict:
        def probe():
            lines = self.compose("logs", "--no-color", "--no-log-prefix",
                                 "--tail", "200", service).splitlines()
            for line in lines:
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if (isinstance(value, dict) and value.get("event") == "failpoint.reached"
                        and value.get("name") == name
                        and isinstance(value.get("instanceId"), str)
                        and value["instanceId"]):
                    require(set(value) <= {"event", "name", "instanceId", "outboxId",
                                           "externalRequestId"},
                            "Failpoint acknowledgement contains an unsafe field")
                    return value
            return None
        return self.until(name + " acknowledgement", probe)

    def crash(self, service: str) -> None:
        self.compose("kill", "-s", "SIGKILL", service)

    def recover(self, service: str) -> None:
        self.set_failpoint(service, "")
        self.until(service + " readiness", lambda: self.container(service, running=True))

    def trace(self, operation_id: str) -> None:
        token = make_token(self.env, "crash-matrix-reader", "diagnostics:read")
        status, envelope = self.action("diagnostics", "trace",
                                       {"identifier": operation_id}, token=token)
        require(status == 200 and envelope.get("status") == "ok",
                "diagnostics.trace failed after recovery")
        result = envelope.get("result")
        require(isinstance(result, dict) and isinstance(result.get("operation"), dict),
                "diagnostics.trace has no operation evidence")
        operation = self.rows("SELECT operation_id, process_id FROM autocheck.operations "
                              "WHERE operation_id=" + sql_text(operation_id))
        require(len(operation) == 1 and result["operation"].get("operationId") == operation_id
                and isinstance(result.get("process"), dict)
                and result["process"].get("processId") == operation[0]["process_id"],
                "diagnostics.trace disagrees with SQL operation/process identity")
        jobs = self.jobs(operation[0]["process_id"])
        require({row["jobId"] for row in result["jobs"]} == {row["job_id"] for row in jobs},
                "diagnostics.trace disagrees with SQL jobs")
        counts = self.rows("SELECT count(*)::int AS n FROM autocheck.operation_events "
                           "WHERE operation_id=" + sql_text(operation_id))
        require(counts == [{"n": len(result["operationEvents"])}],
                "diagnostics.trace disagrees with SQL operation history")

    def jobs(self, process_id: str) -> list[dict]:
        return self.rows("SELECT * FROM autocheck.jobs WHERE process_id="
                         + sql_text(process_id) + " ORDER BY job_id")

    def check_no_crash_loop(self, service: str, name: str) -> None:
        # A fresh container has only its own logs. It must not acknowledge the
        # removed point while processing the reclaimed work.
        lines = self.compose("logs", "--no-color", "--no-log-prefix",
                             "--tail", "200", service).splitlines()
        for line in lines:
            try:
                value = json.loads(line)
            except ValueError:
                continue
            require(not (isinstance(value, dict) and value.get("event") == "failpoint.reached"
                         and value.get("name") == name), "Failpoint repeated after recovery")


def worker_crash(stack: Stack, name: str) -> None:
    stack.set_failpoint("worker-a", name)
    stack.compose("stop", "--timeout", "3", "worker-b")
    operation, process = stack.submit()
    ack = stack.acknowledgement("worker-a", name)
    leased = [job for job in stack.jobs(process) if job["state"] == "LEASED"]
    require(len(leased) == 1 and leased[0]["lease_owner"] == ack["instanceId"],
            "Worker acknowledgement did not preserve a leased job")
    first = leased[0]
    remaining_ms = stack.sql("SELECT to_jsonb(round(extract(epoch FROM lease_until "
                             "- clock_timestamp()) * 1000)) FROM workflow.workflow_jobs "
                             "WHERE job_id=" + sql_text(first["job_id"]) + "::uuid;")
    require(isinstance(remaining_ms, (int, float)) and 0 < remaining_ms <= 2500,
            "Live job lease is not near COURSE_JOB_LEASE_MS=2000")
    print(f"EVIDENCE | {name} liveLeaseRemainingMs={remaining_ms}", flush=True)
    if name == "after_job_claim":
        require(not stack.rows("SELECT * FROM autocheck.external_requests WHERE operation_id="
                               + sql_text(operation)), "Action ran before claim boundary")
    stack.crash("worker-a")
    crashed_at = time.monotonic()
    stack.compose("start", "worker-b")
    stack.until("Worker lease reclaim", lambda: next((job for job in stack.jobs(process)
        if job["job_id"] == first["job_id"] and job["lease_version"] > first["lease_version"]), None))
    reclaim_seconds = time.monotonic() - crashed_at
    require(reclaim_seconds < 30, "Worker reclaim exceeded 30000ms task deadline")
    print(f"EVIDENCE | {name} reclaimSeconds={reclaim_seconds:.3f}", flush=True)
    stack.assert_delivery(operation, process, "COMPLETED")
    reclaimed = [job for job in stack.jobs(process) if job["job_id"] == first["job_id"]]
    require(len(reclaimed) == 1 and reclaimed[0]["execution_id"] == first["execution_id"]
            and reclaimed[0]["lease_version"] > first["lease_version"]
            and reclaimed[0]["attempt_count"] >= 2,
            "Worker reclaim lost stable identity, fencing or attempt history")
    stale_job = sql_text(first["job_id"]) + "::uuid," + sql_text(first["lease_owner"])
    stale_version = "," + str(first["lease_version"])
    finish_probe = stack.sql("SET ROLE workflow_worker; SELECT workflow.finish_job("
                             + stale_job + stale_version + ",'DONE','{}'::jsonb)::text;")
    fail_probe = stack.sql("SET ROLE workflow_worker; SELECT workflow.fail_job("
                           + stale_job + stale_version + ",'stale.probe',false)::text;")
    require(finish_probe.get("code") == "workflow.lease_stale"
            and fail_probe.get("code") == "workflow.lease_stale",
            "Stale worker owner/version could finish or fail reclaimed job")
    print("EVIDENCE | staleWorkerFinish=workflow.lease_stale "
          "staleWorkerFail=workflow.lease_stale", flush=True)
    stack.recover("worker-a")
    stack.check_no_crash_loop("worker-a", name)
    stack.until("Recovered worker-a readiness", lambda:
                stack.internal_http("GET", "http://worker-a:8080/health/ready")[0] == 200)
    stack.compose("stop", "--timeout", "3", "worker-b")
    control_operation, control_process = stack.submit()
    stack.assert_delivery(control_operation, control_process, "COMPLETED")
    stack.compose("start", "worker-b")
    stack.check_no_crash_loop("worker-a", name)
    stack.trace(operation)


def provider_outage(stack: Stack) -> None:
    # Stop/start preserves the simulator container; recovery occurs before the
    # four-attempt test budget. No provider state is recreated during retry.
    provider_id = stack.container("provider-simulator")[0]
    stack.compose("stop", "--timeout", "3", "provider-simulator")
    operation, process = stack.submit()
    stack.wait_state(process, "WAITING_SIGNAL")
    external = stack.external(operation)["external_request_id"]
    before = stack.until("Retryable provider outage", lambda:
                         (lambda row: row if row["attempt_count"] >= 1
                          and row["state"] != "LEASED" else None)(stack.outbox(external)))
    print(f"EVIDENCE | providerOutage beforeRestore state={before['state']} "
          f"attempts={before['attempt_count']}", flush=True)
    require(before["attempt_count"] < 4,
            "Provider outage exhausted retry budget before recovery")
    # Docker start on the captured container avoids Compose's configuration
    # round trip inside the short documented retry window.
    stack.run(["docker", "start", provider_id], "Provider recovery", timeout=30)
    require(stack.container("provider-simulator", running=True)[0] == provider_id,
            "Provider was recreated during outage recovery")
    try:
        stack.assert_delivery(operation, process, "COMPLETED", minimum_requests=1)
    except CheckFailed:
        after = stack.outbox(external)
        audit_status, audit = stack.audit(external)
        print(f"EVIDENCE | providerOutage afterRestore state={after['state']} "
              f"attempts={after['attempt_count']} auditStatus={audit_status} "
              f"requests={audit.get('requestCount') if isinstance(audit, dict) else None} "
              f"payments={audit.get('paymentCount') if isinstance(audit, dict) else None}",
              flush=True)
        raise
    require(stack.outbox(external)["attempt_count"] >= 2,
            "Outage recovery did not record a retry")


def dispatcher_crash(stack: Stack, name: str) -> None:
    stack.set_failpoint("outbox-dispatcher", name)
    stack.compose("stop", "--timeout", "3", "outbox-dispatcher-b")
    if name == "after_provider_response":
        # Keep the asynchronous callback from confirming Outbox before this
        # exact post-response, pre-completion boundary is inspected.
        stack.compose("stop", "--timeout", "3", "receipt-adapter")
    operation, process = stack.submit()
    ack = stack.acknowledgement("outbox-dispatcher", name)
    external = stack.external(operation)
    outbox = stack.outbox(external["external_request_id"])
    identity = stack.rows("SELECT correlation_id::text AS correlation_id, "
                          "amount::text AS amount, currency, payload_hash "
                          "FROM payment.external_requests WHERE external_request_id="
                          + sql_text(external["external_request_id"]))
    require(len(identity) == 1 and identity[0]["payload_hash"] == hashlib.sha256(
        compact({"amount": identity[0]["amount"], "currency": identity[0]["currency"],
                 "operationId": external["external_request_id"]})).hexdigest(),
        "Durable provider body hash is not the exact request bytes")
    require(outbox["state"] == "LEASED" and outbox["lease_owner"] == ack["instanceId"],
            "Dispatcher acknowledgement did not preserve a leased outbox")
    provider_id = stack.container("provider-simulator")[0]
    code, audit = stack.audit(external["external_request_id"],
                              service="inbox-reconciler" if name == "after_provider_response"
                              else "receipt-adapter")
    if name == "after_outbox_claim":
        require(code == 404, "Provider received request before claim boundary")
    else:
        require(code == 200 and isinstance(audit, dict)
                and audit.get("paymentCount") == 1,
                "Provider response boundary lacks one durable provider effect")
    stack.crash("outbox-dispatcher")
    crashed_at = time.monotonic()
    stack.compose("start", "outbox-dispatcher-b")
    stack.until("Dispatcher lease reclaim", lambda:
                stack.outbox(external["external_request_id"])["lease_version"] > outbox["lease_version"])
    print(f"EVIDENCE | {name} reclaimSeconds={time.monotonic() - crashed_at:.3f}", flush=True)
    if name == "after_provider_response":
        try:
            stack.until("Retried Outbox delivery before callback", lambda:
                        stack.outbox(external["external_request_id"])["state"] == "DELIVERED")
        except CheckFailed:
            pending = stack.outbox(external["external_request_id"])
            audit_status, pending_audit = stack.audit(external["external_request_id"],
                                                      service="inbox-reconciler")
            print(f"EVIDENCE | responseRetry state={pending['state']} "
                  f"attempts={pending['attempt_count']} leaseVersion={pending['lease_version']} "
                  f"lastError={pending['last_error_code']} auditStatus={audit_status} "
                  f"requests={pending_audit.get('requestCount') if isinstance(pending_audit, dict) else None} "
                  f"payments={pending_audit.get('paymentCount') if isinstance(pending_audit, dict) else None}",
                  flush=True)
            raise
        stack.compose("start", "receipt-adapter")
        stack.wait_provider()
    recovered_audit = stack.assert_delivery(operation, process, "COMPLETED",
                                             minimum_requests=2 if name == "after_provider_response" else 1)
    final = stack.outbox(external["external_request_id"])
    require(final["outbox_id"] == outbox["outbox_id"]
            and final["lease_version"] > outbox["lease_version"]
            and final["attempt_count"] >= 2
            and stack.external(operation)["payload_hash"] == external["payload_hash"]
            and recovered_audit["idempotencyKey"] == external["external_request_id"],
            "Dispatcher retry changed identity, body or fencing")
    stale_outbox = sql_text(outbox["outbox_id"]) + "::uuid," + sql_text(outbox["lease_owner"])
    stale_version = "," + str(outbox["lease_version"])
    succeed_probe = stack.sql("SET ROLE outbox_dispatcher; SELECT delivery.succeed_outbox("
                              + stale_outbox + stale_version + ",'stale-probe')::text;")
    fail_probe = stack.sql("SET ROLE outbox_dispatcher; SELECT delivery.fail_outbox("
                           + stale_outbox + stale_version + ",'stale.probe')::text;")
    require(succeed_probe.get("code") == "delivery.lease_stale"
            and fail_probe.get("code") == "delivery.lease_stale",
            "Stale dispatcher owner/version could complete reclaimed delivery")
    print("EVIDENCE | staleOutboxSucceed=delivery.lease_stale "
          "staleOutboxFail=delivery.lease_stale", flush=True)
    require(stack.rows("SELECT correlation_id::text AS correlation_id, "
                       "amount::text AS amount, currency, payload_hash "
                       "FROM payment.external_requests WHERE external_request_id="
                       + sql_text(external["external_request_id"])) == identity,
            "Dispatcher retry changed durable correlation or request body identity")
    require(stack.container("provider-simulator", running=True)[0] == provider_id,
            "Provider was recreated during crash recovery")
    stack.recover("outbox-dispatcher")
    stack.check_no_crash_loop("outbox-dispatcher", name)
    stack.until("Recovered dispatcher readiness", lambda:
                stack.internal_http("GET", "http://outbox-dispatcher:8080/health/ready")[0] == 200)
    stack.compose("stop", "--timeout", "3", "outbox-dispatcher-b")
    control_operation, control_process = stack.submit()
    stack.assert_delivery(control_operation, control_process, "COMPLETED")
    stack.compose("start", "outbox-dispatcher-b")
    stack.check_no_crash_loop("outbox-dispatcher", name)
    stack.trace(operation)
    if name == "after_outbox_claim":
        provider_outage(stack)


def inbox_crash(stack: Stack) -> None:
    stack.compose("stop", "--timeout", "3", "outbox-dispatcher",
                  "outbox-dispatcher-b", "inbox-reconciler", "inbox-reconciler-b")
    operation, process = stack.submit()
    stack.wait_state(process, "WAITING_SIGNAL")
    external = stack.external(operation)["external_request_id"]
    stack.set_failpoint("api", "after_inbox_saved")
    stack.compose("start", "outbox-dispatcher")
    stack.acknowledgement("api", "after_inbox_saved")
    before = stack.receipt_rows(external)
    require(len(before) == 1 and before[0]["inbox_state"] == "RECEIVED"
            and before[0]["inbox_applied_at"] is None,
            "Inbox was not committed before acknowledgement")
    stack.crash("api")
    stack.recover("api")
    stack.compose("start", "outbox-dispatcher-b", "inbox-reconciler-b")
    stack.assert_delivery(operation, process, "COMPLETED")
    stack.compose("start", "inbox-reconciler")
    applied = stack.receipt_rows(external)
    require(len(applied) == 1 and all(applied[0][key] == before[0][key]
            for key in ("message_id", "body_hash", "received_at")),
            "Callback retry changed Inbox identity or added another effect")
    receipt = stack.rows("SELECT provider_payment_id, occurred_at_text FROM payment.receipts "
                         "WHERE external_request_id=" + sql_text(external))[0]
    replay = {"version": 1, "messageId": before[0]["message_id"],
              "externalRequestId": external,
              "providerPaymentId": receipt["provider_payment_id"],
              "outcome": "COMPLETED", "occurredAt": receipt["occurred_at_text"]}
    require(hashlib.sha256(compact(replay)).hexdigest() == before[0]["body_hash"],
            "Duplicate receipt bytes differ from saved callback")
    code, response = stack.signed_receipt(replay)
    require(code == 200 and isinstance(response, dict)
            and response.get("outcome") == "DUPLICATE"
            and len(stack.receipt_rows(external)) == 1,
            "Duplicate callback added another Inbox effect")
    signals = stack.rows("SELECT event_type, count(*)::int AS n FROM autocheck.workflow_events "
                         "WHERE process_id=" + sql_text(process)
                         + " AND event_type IN ('SignalAccepted','SignalApplied') GROUP BY event_type")
    require({row["event_type"]: row["n"] for row in signals}
            == {"SignalAccepted": 1, "SignalApplied": 1},
            "Callback retry or reconciler added a duplicate signal event")
    print("EVIDENCE | inbox=1 receipt=1 SignalAccepted=1 SignalApplied=1 reconciler=b",
          flush=True)
    stack.check_no_crash_loop("api", "after_inbox_saved")
    stack.trace(operation)


def manual_crash(stack: Stack) -> None:
    operation, process = stack.submit("PAYMENT_APPROVAL", "100001.00")
    stack.wait_state(process, "WAITING_MANUAL")
    steps = stack.rows("SELECT step_instance_id FROM autocheck.steps WHERE process_id="
                       + sql_text(process) + " AND step_type='MANUAL' AND state='WAITING'")
    require(len(steps) == 1, "Expected one pending manual step")
    payload = {"processId": process, "stepInstanceId": steps[0]["step_instance_id"],
               "decision": "APPROVED", "reason": "Synthetic approval"}
    token = make_token(stack.env, "crash-matrix-reviewer", "workflow:manual")
    key = uuid.uuid4().hex
    stack.set_failpoint("api", "after_manual_decision")
    stack.until("API readiness after failpoint recreate",
                lambda: stack.http("GET", "/health/ready")[0] == 200)
    request_result: dict = {}

    def invoke_pending() -> None:
        status, body = stack.http("POST", "/api/workflow/manual", compact(payload),
                                  {"Authorization": "Bearer " + token,
                                   "Content-Type": "application/json", "X-Action-Version": "1",
                                   "Idempotency-Key": key})
        request_result.update(status=status,
                              code=body.get("code") if isinstance(body, dict) else None)

    request = threading.Thread(target=invoke_pending, daemon=True)
    request.start()
    try:
        stack.acknowledgement("api", "after_manual_decision")
    except CheckFailed:
        print(f"EVIDENCE | manualRequest status={request_result.get('status')} "
              f"code={request_result.get('code')}", flush=True)
        raise
    stack.crash("api")
    request.join(timeout=12)
    require(not stack.rows("SELECT * FROM autocheck.decisions WHERE process_id="
                           + sql_text(process)), "Uncommitted manual decision survived crash")
    require(stack.process(process)["state"] == "WAITING_MANUAL"
            and stack.rows("SELECT state FROM autocheck.steps WHERE step_instance_id="
                           + sql_text(steps[0]["step_instance_id"])) == [{"state": "WAITING"}],
            "Manual step changed despite transaction rollback")
    stack.recover("api")
    stack.until(
        "API readiness after recovery",
        lambda: stack.http("GET", "/health/ready")[0] == 200,
    )
    code, envelope = stack.action("workflow", "manual", payload, token=token, key=key)
    require(code == 200 and envelope.get("status") == "ok",
            "Same authoritative manual command did not recover")
    stack.assert_terminal(operation, process, "COMPLETED")
    code, duplicate = stack.action("workflow", "manual", payload, token=token, key=key)
    require(code == 200 and duplicate.get("result") == envelope.get("result"),
            "Same manual decision is not idempotent")
    opposite = dict(payload, decision="REJECTED")
    code, conflict = stack.action("workflow", "manual", opposite, token=token)
    require(code == 409 and conflict.get("code") == "workflow.decision_conflict",
            "Opposite manual decision lacks contractual conflict")
    require(len(stack.rows("SELECT * FROM autocheck.decisions WHERE process_id="
                           + sql_text(process))) == 1,
            "Manual recovery wrote multiple authoritative decisions")
    require(stack.rows("SELECT count(*)::int AS n FROM autocheck.workflow_events "
                       "WHERE process_id=" + sql_text(process)
                       + " AND event_type='ManualDecided'") == [{"n": 1}],
            "Manual recovery added duplicate decision history")
    print("EVIDENCE | manualDecisions=1 ManualDecided=1 conflict=1", flush=True)
    stack.check_no_crash_loop("api", "after_manual_decision")
    stack.trace(operation)


def recovery_supplement(stack: Stack) -> None:
    # One new payment chain per replica set proves that every worker,
    # dispatcher and reconciler can process fresh work after crash recovery.
    controls = []
    for active, inactive in (("a", "b"), ("b", "a")):
        stack.compose("stop", "--timeout", "3", "worker-" + inactive,
                      "outbox-dispatcher" + ("-b" if inactive == "b" else ""),
                      "inbox-reconciler" + ("-b" if inactive == "b" else ""))
        operation, process = stack.submit()
        stack.assert_delivery(operation, process, "COMPLETED")
        stack.trace(operation)
        controls.append(operation)
        stack.compose("start", "worker-" + inactive,
                      "outbox-dispatcher" + ("-b" if inactive == "b" else ""),
                      "inbox-reconciler" + ("-b" if inactive == "b" else ""))
        print("EVIDENCE | fresh work completed by replica set " + active, flush=True)

    services = ("api", "worker-a", "worker-b", "outbox-dispatcher",
                "outbox-dispatcher-b", "inbox-reconciler", "inbox-reconciler-b",
                "receipt-adapter")
    for service in services:
        stack.container(service, running=True)
        port = 8082 if service == "receipt-adapter" else 8080
        for endpoint in ("/health/ready", "/metrics"):
            status, _ = stack.internal_http("GET", f"http://{service}:{port}{endpoint}")
            require(status == 200, service + endpoint + " did not return HTTP 200")
    for view in ("jobs", "outbox"):
        counts = stack.rows(f"SELECT count(*)::int AS n FROM autocheck.{view} WHERE state='LEASED'")
        require(counts == [{"n": 0}], "Old " + view + " leases remain stuck")
    token = make_token(stack.env, "crash-matrix-reader", "diagnostics:read")
    code, response = stack.action("diagnostics", "stalled", {}, token=token)
    require(code == 200 and response.get("status") == "ok"
            and isinstance(response.get("result"), dict)
            and isinstance(response["result"].get("items"), list),
            "diagnostics.stalled response is invalid")
    stalled_ids = {item["operationId"] for item in response["result"]["items"]}
    require(not stalled_ids.intersection(controls),
            "Recovered terminal control chain appears in diagnostics.stalled")
    print("SUPPLEMENT | replicas=6/6 readiness=8/8 metrics=8/8 stuckLeases=0 stalledControls=0",
          flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List six scenarios without Docker")
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    if args.list:
        print("\n".join(SCENARIOS))
        return 0
    require(Path.cwd().resolve() == ROOT, "Run from the repository root")
    require(30 <= args.timeout <= 300, "--timeout must be between 30 and 300 seconds")
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        for name in dict.fromkeys(args.scenario or SCENARIOS):
            with tempfile.TemporaryDirectory(prefix="week4-crash-") as directory:
                stack = Stack(Path(directory), args.timeout)
                try:
                    stack.start()
                    if name in SCENARIOS[:2]:
                        worker_crash(stack, name)
                    elif name in SCENARIOS[2:4]:
                        dispatcher_crash(stack, name)
                    elif name == "after_inbox_saved":
                        inbox_crash(stack)
                    else:
                        manual_crash(stack)
                    print("PASS | " + name, flush=True)
                    if name == "after_manual_decision" and not args.scenario:
                        recovery_supplement(stack)
                finally:
                    stack.cleanup()
    finally:
        signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CheckFailed as error:
        print("FAIL | " + str(error), flush=True)
        raise SystemExit(1) from None
