"""Six isolated Week 4 observability scenarios; run from the repository root.

Host dependencies: Python stdlib and Docker Compose. Reuses only participant-
owned transport/SQL helpers. --list and --help never invoke Docker. The metrics
fixtures exercise production CLI/SQL/HTTP boundaries with consumers stopped;
they do not claim to test provider effects or the Worker action mapper.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import tempfile
import uuid

import week3_e2e as e2e
import week4_sql_regression as sql
from signal_process_concurrency import CheckFailed, PsqlSession, interrupted, require, sql_text


ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = ("all-endpoints", "postgres-readiness", "provider-readiness",
             "metrics-contract", "metrics-values", "restart-recovery")
SERVICES = {name: 8080 for name in (
    "api", "worker-a", "worker-b", "outbox-dispatcher", "outbox-dispatcher-b",
    "inbox-reconciler", "inbox-reconciler-b")}
SERVICES["receipt-adapter"] = 8082
CONSUMERS = tuple(name for name in SERVICES if name not in ("api", "receipt-adapter"))
MEDIA = "application/openmetrics-text; version=1.0.0; charset=utf-8"
METRICS = {
    "workflow_jobs_ready": "gauge", "workflow_job_oldest_age_seconds": "gauge",
    "workflow_processes_waiting": "gauge", "outbox_pending": "gauge",
    "outbox_oldest_age_seconds": "gauge", "workflow_failures_total": "counter",
}
COUNTS = ("workflow_jobs_ready", "workflow_processes_waiting", "outbox_pending", "workflow_failures_total")
AGES = ("workflow_job_oldest_age_seconds", "outbox_oldest_age_seconds")
LIVE = b'{"status":"live"}'
READY = b'{"status":"ready"}'
NOT_READY = b'{"status":"not_ready","code":"dependency.unavailable"}'

# Executed in the existing adapter container. No credentials, request bodies,
# provider requests, proxies or redirects. Captured responses are never logged.
PROBES = r"""
import base64, concurrent.futures, http.client, json, sys
def probe(item):
    connection = None
    try:
        connection = http.client.HTTPConnection(item['service'], item['port'], timeout=6)
        connection.request('GET', item['path'])
        response = connection.getresponse()
        body = response.read(65537)
        return item['key'], {'status': response.status,
            'type': response.getheader('Content-Type', ''),
            'body': base64.b64encode(body).decode('ascii')}
    except Exception:
        return item['key'], {'status': 0, 'type': '', 'body': ''}
    finally:
        if connection is not None:
            connection.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    result = dict(executor.map(probe, json.load(sys.stdin)))
print(json.dumps(result))
"""


def parse_metrics(response: dict, api: bool) -> dict[str, float]:
    require(response["status"] == 200 and response["type"] == MEDIA, "Metrics status/media type differs")
    raw = response["body"]
    require(len(raw) <= 65536 and b"\r" not in raw and raw.endswith(b"# EOF\n"),
            "Metrics size/LF/EOF contract differs")
    try:
        document = raw.decode("utf-8", errors="strict")
    except UnicodeError:
        raise CheckFailed("Metrics must be strict UTF-8") from None
    types, values = {}, {}
    # This implementation intentionally exposes only unlabelled scalar series.
    # The restricted grammar also rejects identifiers/UUIDs, labels, timestamps,
    # exemplars, NaN/Inf, duplicate TYPE/sample lines and text after EOF.
    for line in document.splitlines()[:-1]:
        declaration = re.fullmatch(r"# TYPE ([a-z_][a-z0-9_]*) (gauge|counter)", line)
        if declaration:
            name, kind = declaration.groups()
            require(name not in types, "Duplicate metric TYPE")
            types[name] = kind
            continue
        sample = re.fullmatch(r"([a-z_][a-z0-9_]*) ([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)", line)
        require(sample is not None, "Invalid or labelled OpenMetrics sample")
        name, number = sample.groups()
        value = float(number)
        family = name.removesuffix("_total") if name.endswith("_total") else name
        require(name not in values and family in types and math.isfinite(value) and value >= 0,
                "Invalid/duplicate metric or missing preceding TYPE")
        values[name] = value
    expected = METRICS if api else {"process_up": "gauge"}
    expected_types = {name.removesuffix("_total") if kind == "counter" else name: kind
                      for name, kind in expected.items()}
    require(types == expected_types and set(values) == set(expected), "Metric families/TYPE declarations differ")
    if api:
        require(all(values[key].is_integer() for key in COUNTS), "Metric counts must be integers")
    else:
        require(values == {"process_up": 1}, "Unexpected process metric value")
    return values


class Stack(e2e.Stack):
    def __init__(self, directory: Path) -> None:
        super().__init__(directory, timeout=45)
        self.project = "week4-obs-" + uuid.uuid4().hex
        self.compose_prefix[self.compose_prefix.index("--project-name") + 1] = self.project
        # Case-insensitive filtering matters on Windows. Keep only generated
        # Course/Provider settings; never use an inherited Compose override/.env.
        generated = e2e.synthetic_environment()
        self.env = {key: value for key, value in os.environ.items()
                    if not key.upper().startswith(("COMPOSE_", "COURSE_", "PROVIDER_"))}
        self.env.update(generated)
        self.env.update({"COMPOSE_DISABLE_ENV_FILE": "1", "COURSE_OUTBOX_BACKOFF_BASE_MS": "15000",
                         "COURSE_OUTBOX_BACKOFF_MAX_MS": "15000", "COURSE_OUTBOX_JITTER_MAX_MS": "0",
                         "COURSE_OUTBOX_LEASE_MS": "2000", "COURSE_JOB_LEASE_MS": "2000"})
        self.client_token = e2e.make_token(self.env, "observability-client", "payment:read payment:write")
        mapping = {name: name for name in ("postgres", "cli", "api", "gateway")}
        mapping.update({name: "worker" if name.startswith("worker-") else "python"
                        for name in SERVICES if name != "api"})
        images = {name: self.project + "-" + suffix + ":phase2" for name, suffix in mapping.items()}
        self.tags = sorted(set(images.values()))
        self.images_owned = False
        override = directory / "observability-images.json"
        override.write_text(json.dumps({"services": {name: {"image": tag} for name, tag in images.items()}}),
                            encoding="utf-8")
        self.compose_prefix += ["-f", str(override)]
        self.prefix = self.compose_prefix  # Shared persistent psql transport.
        self.sessions: list[PsqlSession] = []

    def start(self) -> None:
        for tag in self.tags:
            require(not self.run(["docker", "image", "ls", "-q", "--filter", "reference=" + tag],
                                 "Private image preflight", timeout=30), "Private image already exists")
        self.images_owned = True
        super().start()
        self.compose("up", "-d", "--no-build", "--no-deps", "outbox-dispatcher-b", "inbox-reconciler-b")
        self.wait_ready()
        for service in SERVICES:
            self.container(service, running=True)
        expected = [{"file_name": path.name, "checksum": hashlib.sha256(path.read_bytes()).hexdigest()}
                    for path in sql.forward_files()]
        history = self.sql("SELECT jsonb_agg(jsonb_build_object('file_name',file_name,'checksum',checksum) "
                           "ORDER BY file_name) FROM public.course_schema_migrations;")
        require(history == expected, "Fresh bootstrap forward migration checksums differ")
        print("SETUP | fresh 001-020 init plus CLI forward 021-028; eight services ready", flush=True)

    def close_sessions(self) -> None:
        failed = False
        for session in self.sessions:
            try:
                session.close()
            except Exception:
                failed = True
        self.sessions.clear()
        require(not failed, "A psql session did not close cleanly")

    def start_existing(self, *services: str) -> None:
        # Compose start traverses depends_on and can rerun the exited CLI.
        # Resume only these existing containers, without a dependency traversal.
        allowed = set(SERVICES) | {"postgres", "gateway", "provider-simulator"}
        require(bool(services) and set(services) <= allowed, "Unexpected service resume request")
        containers = [self.container(service)[0] for service in services]
        self.run(["docker", "start", *containers], "Resume existing containers", timeout=120)

    def cleanup(self) -> None:
        failed = False
        try:
            self.close_sessions()
        except Exception:
            failed = True
        try:
            super().cleanup()
        except Exception:
            failed = True
        finally:
            if self.cleanup_armed:
                label = "label=com.docker.compose.project=" + self.project
                for args in (("ps", "-aq"), ("volume", "ls", "-q"), ("network", "ls", "-q")):
                    try:
                        require(not self.run(["docker", *args, "--filter", label], "Independent cleanup", timeout=30),
                                "Isolated project resources remain")
                    except Exception:
                        failed = True
            if self.images_owned:
                for tag in self.tags:
                    lookup = ["docker", "image", "ls", "-q", "--filter", "reference=" + tag]
                    try:
                        if self.run(lookup, "Private image lookup", timeout=30):
                            self.run(["docker", "image", "rm", tag], "Private image removal", timeout=60)
                        require(not self.run(lookup, "Private image verification", timeout=30), "Private image remains")
                    except Exception:
                        failed = True
        require(not failed, "Isolated cleanup failed (details withheld)")
        print("CLEANUP | containers=0 volumes=0 networks=0 private image tags=0", flush=True)

    def probes(self, paths: tuple[str, ...], services=SERVICES) -> dict:
        requests = [{"key": name + path, "service": name, "port": SERVICES[name], "path": path}
                    for name in services for path in paths]
        raw = self.compose("exec", "-T", "receipt-adapter", "python", "-c", PROBES,
                           stdin=json.dumps(requests), timeout=30)
        results = e2e.decode_json(raw, "Health transport")
        require(isinstance(results, dict) and set(results) == {r["key"] for r in requests},
                "Incomplete health transport result")
        for result in results.values():
            result["body"] = base64.b64decode(result["body"], validate=True)
        return results

    def wait_ready(self, ready: bool = True) -> None:
        status, body = (200, READY) if ready else (503, NOT_READY)
        last = {}

        def probe():
            nonlocal last
            last = self.probes(("/health/ready",))
            return all(result["status"] == status and result["body"] == body
                       and result["type"].split(";", 1)[0] == "application/json" for result in last.values())

        try:
            self.until("Eight-service readiness", probe)
        except e2e.CheckFailed:
            statuses = ",".join(name + "=" + str(last.get(name + "/health/ready", {}).get("status", 0))
                                for name in SERVICES)
            raise CheckFailed("Readiness deadline exceeded; statuses=" + statuses) from None

    def assert_live(self) -> None:
        for name, result in self.probes(("/health/live",)).items():
            require(result["status"] == 200 and result["body"] == LIVE
                    and result["type"].split(";", 1)[0] == "application/json", name + ": liveness differs")

    def api_metrics(self) -> dict[str, float]:
        return parse_metrics(self.probes(("/metrics",), ("api",))["api/metrics"], api=True)

    def cli(self, *args: str, payload: object | None = None) -> dict:
        raw = self.compose("run", "--rm", "-T", "--no-deps", "cli", *args,
                           stdin=None if payload is None else json.dumps(payload) + "\n")
        return sql.ok(e2e.decode_json(raw, "CLI envelope"), "CLI")


def all_endpoints(stack: Stack) -> None:
    stack.wait_ready()
    stack.assert_live()
    for name, result in stack.probes(("/metrics",)).items():
        parse_metrics(result, api=name == "api/metrics")


def postgres_readiness(stack: Stack) -> None:
    stack.wait_ready()
    payload = {"operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"}
    # Populate the production action pool BEFORE the outage. Health probes use
    # separate connections and cannot create the stale-idle condition themselves.
    before = stack.ok_action("payment", "request", payload)["operationId"]
    try:
        stack.compose("stop", "--timeout", "5", "postgres")
        stack.wait_ready(False)
        stack.assert_live()
        response = stack.probes(("/metrics",), ("api",))["api/metrics"]
        require(response["status"] == 503 and response["body"] == NOT_READY
                and response["type"].split(";", 1)[0] == "application/json",
                "Unavailable DB produced cached metrics or unsafe error")
    finally:
        stack.start_existing("postgres")
    stack.wait_ready()
    # Exactly one action after readiness recovers: no warm-up/retry on 503.
    status, envelope = stack.action("payment", "request", payload)
    require(status == 200 and envelope.get("status") == "ok" and envelope.get("outcome") == "CREATED",
            f"First post-outage action failed: HTTP {status} (body withheld)")
    operation = str(uuid.UUID(envelope.get("result", {}).get("operationId", "")))
    require(operation != before, "Post-outage action did not create a new operation")
    rows = stack.rows("SELECT status FROM autocheck.operations WHERE operation_id=" + sql_text(operation))
    require(rows == [{"status": "CREATED"}], "First post-outage action lacks durable evidence")
    print("EVIDENCE | first-post-recovery-action ready=200 attempts=1 http=200 outcome=CREATED durable=1", flush=True)


def provider_readiness(stack: Stack) -> None:
    stack.wait_ready()
    try:
        stack.compose("stop", "--timeout", "5", "provider-simulator")
        stack.wait_ready()
        stack.assert_live()
    finally:
        stack.start_existing("provider-simulator")
    stack.wait_provider()


def metrics_contract(stack: Stack) -> None:
    for name, result in stack.probes(("/metrics",)).items():
        parse_metrics(result, api=name == "api/metrics")
    metadata = stack.sql("""
        SELECT jsonb_build_object('owner',pg_get_userbyid(p.proowner), 'definer',p.prosecdef,
          'config',p.proconfig, 'volatility',p.provolatile, 'language',l.lanname,
          'grantees',(SELECT jsonb_agg(pg_get_userbyid(a.grantee) ORDER BY pg_get_userbyid(a.grantee))
             FROM aclexplode(p.proacl) a WHERE a.privilege_type='EXECUTE'),
          'runtime',has_function_privilege('course_runtime',p.oid,'EXECUTE'),
          'denied',(SELECT bool_and(NOT has_function_privilege(r,p.oid,'EXECUTE')) FROM unnest(
             ARRAY['course_publication','workflow_worker','outbox_dispatcher','inbox_reconciler','autocheck_reader']) r))
        FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
        WHERE p.oid='api.observability_metrics()'::regprocedure;
    """)
    require(metadata == {"owner": "course_owner", "definer": True, "config": ["search_path=pg_catalog, api"],
                         "volatility": "s", "language": "sql", "grantees": ["course_owner", "course_runtime"],
                         "runtime": True, "denied": True}, "Metrics SQL security boundary differs")


def facts(db: PsqlSession) -> dict:
    # Independent read-only evidence; never call the function under test.
    # jobs.created_at is not in the reader projection, so the privileged test
    # observer reads that table without granting runtime any table access.
    return db.json("""
        BEGIN READ ONLY;
        WITH stamp AS (SELECT statement_timestamp() AS at), jobs AS (
          SELECT created_at FROM workflow.workflow_jobs, stamp
          WHERE state='READY' OR (state='RETRY_WAIT' AND next_attempt_at<=at)
             OR (state='LEASED' AND lease_until<=at)
        ), pending AS (SELECT created_at FROM autocheck.outbox WHERE state IN ('PENDING','LEASED','RETRY_WAIT'))
        SELECT jsonb_build_object(
          'workflow_jobs_ready',(SELECT count(*) FROM jobs),
          'workflow_job_oldest_age_seconds',GREATEST(0,COALESCE(extract(epoch FROM (at-(SELECT min(created_at) FROM jobs))),0)),
          'workflow_processes_waiting',(SELECT count(*) FROM autocheck.processes WHERE state IN ('WAITING_SIGNAL','WAITING_MANUAL')),
          'outbox_pending',(SELECT count(*) FROM pending),
          'outbox_oldest_age_seconds',GREATEST(0,COALESCE(extract(epoch FROM (at-(SELECT min(created_at) FROM pending))),0)),
          'workflow_failures_total',(SELECT count(*) FROM autocheck.processes WHERE state='FAILED')) FROM stamp;
        COMMIT;
    """, "read-only metric evidence")


def assert_values(stack: Stack, db: PsqlSession, expected: tuple[int, int, int, int], label: str) -> None:
    before = facts(db)
    values = stack.api_metrics()
    after = facts(db)
    for key, wanted in zip(COUNTS, expected):
        require(before[key] == after[key] == values[key] == wanted, label + ": count differs: " + key)
    # The HTTP snapshot must fall between the surrounding database snapshots.
    # Consumers are stopped and each tested state is stable across this window.
    for key in AGES:
        require(float(before[key]) - 0.001 <= values[key] <= float(after[key]) + 0.001,
                label + ": oldest age outside SQL time bracket")
    for count, age in ((COUNTS[0], AGES[0]), (COUNTS[2], AGES[1])):
        require(values[age] == 0 if values[count] == 0 else values[age] > 0,
                label + ": empty/nonempty age differs")
    print("EVIDENCE | " + label + ": counts and ages match SQL", flush=True)


def job_fixture(stack: Stack) -> str:
    name = "metrics-" + uuid.uuid4().hex
    # An ordinary published map gives this test a bounded observation window.
    # It references the existing action; no SQL/action implementation is added.
    document = {
        "contract_version": "course-1", "flow_name": name, "version": 1, "start_step": "validate",
        "steps": [{"key": "validate", "type": "automatic", "task": {
            "service": "postgres", "module": "payment", "action": "validate", "action_version": 1,
            "required_policy": ["payment:internal"], "timeout_ms": 5000,
            "retry": {"max_attempts": 3, "delays_ms": [15000, 15000]},
            "input_mapping": {"/operationId": "/operationId"}, "input_constants": {}}},
            {"key": "done", "type": "end", "outcome": "FINISHED"}],
        "transitions": [{"from": "validate", "outcome": "VALID", "to": "done"}],
    }
    operation = stack.ok_action("payment", "request", {
        "operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"})["operationId"]
    stack.cli("flow", "publish", "/dev/stdin", payload=document)
    stack.cli("flow", "activate", name, "--version", "1")
    result = stack.cli("flow", "start", name, "--business-key", uuid.uuid4().hex,
                       "--data", "/dev/stdin", payload={"operationId": operation})
    return str(uuid.UUID(result["result"]["processId"]))


def claim_job(db: PsqlSession, process: str) -> dict:
    jobs = sql.boundary(db, "workflow_worker", "COALESCE((SELECT jsonb_agg(to_jsonb(j)) "
                        "FROM workflow.claim_jobs('observability-worker',1,2000) j),'[]'::jsonb)")
    require(len(jobs) == 1 and jobs[0]["process_id"] == process, "Unexpected metrics job claim")
    return jobs[0]


def fail_job(db: PsqlSession, job: dict, retry: bool) -> None:
    envelope = sql.boundary(db, "workflow_worker", "workflow.fail_job(" + sql_text(job["job_id"])
                            + "::uuid,'observability-worker'," + str(job["lease_version"])
                            + ",'observability.synthetic_failure'," + ("true" if retry else "false") + ")")
    sql.ok(envelope, "Metrics failure boundary")
    require(envelope["result"]["state"] == ("RETRY_WAIT" if retry else "DEAD"), "Unexpected job failure state")


def job_state(db: PsqlSession, job: dict, state: str, due: bool = False) -> bool:
    predicate = "state=" + sql_text(state)
    if state in ("RETRY_WAIT", "LEASED"):
        field = "next_attempt_at" if state == "RETRY_WAIT" else "lease_until"
        predicate += " AND " + field + (" <= " if due else " > ") + "clock_timestamp()"
    return db.json("SELECT to_jsonb(" + predicate + ") FROM autocheck.jobs WHERE job_id="
                   + sql_text(job["job_id"]) + "::uuid;", "job state barrier") is True


def metrics_values(stack: Stack) -> None:
    stack.compose("stop", "--timeout", "5", *CONSUMERS)
    db = PsqlSession(stack, "metric-evidence")
    try:
        assert_values(stack, db, (0, 0, 0, 0), "initial-zero")
        process = job_fixture(stack)
        assert_values(stack, db, (1, 0, 0, 0), "READY job")
        job = claim_job(db, process)
        require(job_state(db, job, "LEASED"), "Job lease expired before observation")
        assert_values(stack, db, (0, 0, 0, 0), "unexpired LEASED job")
        fail_job(db, job, True)
        require(job_state(db, job, "RETRY_WAIT"), "Job retry became due before observation")
        assert_values(stack, db, (0, 0, 0, 0), "delayed job retry; failed attempt is not terminal")
        stack.until("Job retry due", lambda: job_state(db, job, "RETRY_WAIT", True), timeout=30)
        assert_values(stack, db, (1, 0, 0, 0), "due RETRY_WAIT job")
        job = claim_job(db, process)
        stack.until("Job lease expired", lambda: job_state(db, job, "LEASED", True), timeout=20)
        assert_values(stack, db, (1, 0, 0, 0), "expired LEASED job")
        job = claim_job(db, process)
        assert_values(stack, db, (0, 0, 0, 0), "stale attempt is not terminal")
        fail_job(db, job, False)
        assert_values(stack, db, (0, 0, 0, 1), "one terminal FAILED process")

        dead = sql.delivery(db)
        require(sql.outbox(db, dead)["state"] == "PENDING", "Expected PENDING Outbox")
        assert_values(stack, db, (0, 1, 1, 1), "PENDING Outbox and WAITING_SIGNAL")
        lease = sql.claim(db, dead, "observability-outbox")
        require(sql.outbox(db, dead)["state"] == "LEASED", "Expected LEASED Outbox")
        assert_values(stack, db, (0, 1, 1, 1), "LEASED Outbox")
        sql.ok(sql.complete(db, dead, "observability-outbox", lease["lease_version"], "http.503.retryable"), "Outbox retry")
        require(db.json("SELECT to_jsonb(state='RETRY_WAIT' AND next_attempt_at>clock_timestamp()) FROM autocheck.outbox "
                        "WHERE outbox_id=" + sql_text(dead["outbox"]) + "::uuid;", "delayed outbox") is True,
                "Outbox retry became due before observation")
        assert_values(stack, db, (0, 1, 1, 1), "delayed RETRY_WAIT Outbox included")
        stack.until("Outbox retry due", lambda: db.json("SELECT to_jsonb(next_attempt_at<=clock_timestamp()) "
                    "FROM autocheck.outbox WHERE outbox_id=" + sql_text(dead["outbox"]) + "::uuid;", "outbox time barrier"), timeout=30)
        lease = sql.claim(db, dead, "observability-outbox")
        sql.ok(sql.complete(db, dead, "observability-outbox", lease["lease_version"], "http.400.terminal"), "Outbox terminal")
        sql.assert_dead(db, dead, 2)
        assert_values(stack, db, (0, 1, 0, 1), "DEAD excluded; workflow still waiting")

        delivered = sql.delivery(db)
        lease = sql.claim(db, delivered, "observability-outbox")
        provider_id = "metrics-provider-" + uuid.uuid4().hex
        sql.ok(sql.boundary(db, "outbox_dispatcher", "delivery.succeed_outbox(" + sql_text(delivered["outbox"])
                           + "::uuid,'observability-outbox'," + str(lease["lease_version"]) + ","
                           + sql_text(provider_id) + ")"), "Delivery acknowledgement boundary")
        require(sql.outbox(db, delivered)["state"] == "DELIVERED", "Expected DELIVERED Outbox")
        assert_values(stack, db, (0, 2, 0, 1), "DELIVERED excluded")
        receipt = {"version": 1, "messageId": provider_id, "providerPaymentId": provider_id,
                   "externalRequestId": delivered["external"], "outcome": "COMPLETED", "occurredAt": "2026-09-04T12:00:00Z"}
        status, envelope = stack.signed_receipt(receipt)
        require(status == 200, "Signed receipt HTTP status differs")
        sql.ok(envelope, "Signed receipt")
        require(sql.outbox(db, delivered)["state"] == "CONFIRMED", "Expected CONFIRMED Outbox")
        assert_values(stack, db, (0, 2, 0, 1), "CONFIRMED excluded")
        require(sql.boundary(db, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 1, "Receipt not reconciled")
        sql.finish_payment(db, delivered)

        operation, process = stack.submit("PAYMENT_APPROVAL", "100001.00")
        for action in ("validate", "check_limit"):
            sql.run_job(db, operation, process, action)
        require(stack.process(process)["state"] == "WAITING_MANUAL", "Manual fixture did not wait")
        assert_values(stack, db, (0, 2, 0, 1), "WAITING_MANUAL and WAITING_SIGNAL counted")
    finally:
        try:
            stack.close_sessions()
        finally:
            stack.start_existing(*CONSUMERS)
    stack.wait_ready()


def durable_snapshot(stack: Stack) -> object:
    fields = ["'migrations',(SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) FROM public.course_schema_migrations m)"]
    for view, key in (("operations", "operation_id"), ("operation_events", "event_id"),
                      ("processes", "process_id"), ("steps", "step_instance_id"),
                      ("jobs", "job_id"), ("attempts", "attempt_id"),
                      ("signals", "message_id"), ("workflow_events", "event_id"),
                      ("external_requests", "external_request_id"), ("outbox", "outbox_id"),
                      ("inbox", "message_id"), ("receipts", "message_id"),
                      ("decisions", "decision_id")):
        fields.append(sql_text(view) + ", (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY " + key
                      + "),'[]'::jsonb) FROM autocheck." + view + " r)")
    return stack.sql("SELECT jsonb_build_object(" + ",".join(fields) + ");")


def restart_flow(stack: Stack) -> str:
    name = "restart-" + uuid.uuid4().hex
    # payment.request is a real, idempotent action. Its requestId is the job's
    # stable executionId, so the recovered Worker can commit one domain effect.
    document = {
        "contract_version": "course-1", "flow_name": name, "version": 1, "start_step": "request",
        "steps": [{"key": "request", "type": "automatic", "task": {
            "service": "postgres", "module": "payment", "action": "request", "action_version": 1,
            "required_policy": ["payment:write"], "timeout_ms": 5000,
            "retry": {"max_attempts": 2, "delays_ms": [30000]},
            "input_mapping": {}, "input_constants": {
                "operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"}}},
            {"key": "done", "type": "end", "outcome": "FINISHED"}],
        "transitions": [{"from": "request", "outcome": "CREATED", "to": "done"}],
    }
    stack.cli("flow", "publish", "/dev/stdin", payload=document)
    stack.cli("flow", "activate", name, "--version", "1")
    return name


def start_restart_job(stack: Stack, flow: str) -> str:
    result = stack.cli("flow", "start", flow, "--business-key", uuid.uuid4().hex,
                       "--data", "/dev/stdin", payload={})
    return str(uuid.UUID(result["result"]["processId"]))


def one_job(db: PsqlSession, process: str) -> dict:
    row = db.json("SELECT to_jsonb(j) FROM autocheck.jobs j WHERE process_id="
                  + sql_text(process) + "::uuid;", "restart job")
    require(isinstance(row, dict), "Restart job is missing")
    return row


def restart_retry_clock(db: PsqlSession, job: dict, outbox: dict) -> dict:
    return db.json("SELECT jsonb_build_object('workflowMs',extract(epoch FROM ("
                   + sql_text(job["next_attempt_at"]) + "::timestamptz-clock_timestamp()))*1000,"
                   "'outboxMs',extract(epoch FROM (" + sql_text(outbox["next_attempt_at"])
                   + "::timestamptz-clock_timestamp()))*1000);", "server retry clock")


def retry_before_due(db: PsqlSession, job: dict, outbox: dict, check_job: bool) -> dict:
    job_id = sql_text(job["job_id"]) + "::uuid"
    outbox_id = sql_text(outbox["outbox_id"]) + "::uuid"
    facts = db.json("WITH stamp AS (SELECT clock_timestamp() AS at) SELECT jsonb_build_object("
                    "'workflowMs',extract(epoch FROM (" + sql_text(job["next_attempt_at"])
                    + "::timestamptz-at))*1000,'outboxMs',extract(epoch FROM ("
                    + sql_text(outbox["next_attempt_at"]) + "::timestamptz-at))*1000,"
                    "'job',(SELECT to_jsonb(j) FROM autocheck.jobs j WHERE job_id=" + job_id + "),"
                    "'jobAttempts',(SELECT count(*) FROM autocheck.attempts WHERE job_id=" + job_id + "),"
                    "'jobEffects',(SELECT count(*) FROM autocheck.operations WHERE request_id="
                    + sql_text(job["execution_id"]) + "),"
                    "'outbox',(SELECT to_jsonb(o) FROM autocheck.outbox o WHERE outbox_id=" + outbox_id + ")) "
                    "FROM stamp;", "pre-deadline evidence")
    # A read can straddle the exact deadline. The persisted claim timestamp
    # below supplies the precise no-early proof for the final 200 ms.
    if check_job and facts["workflowMs"] > 200:
        require(facts["job"]["state"] == "RETRY_WAIT" and facts["jobAttempts"] == 1
                and facts["jobEffects"] == 0, "Worker retried before workflow deadline")
    if facts["outboxMs"] > 200:
        require(facts["outbox"]["state"] == "RETRY_WAIT"
                and facts["outbox"]["attempt_count"] == 1,
                "Dispatcher retried before Outbox deadline")
    return facts


def restart_recovery(stack: Stack) -> None:
    stack.compose("stop", "--timeout", "5", *CONSUMERS)
    db = PsqlSession(stack, "restart-fixtures")
    try:
        old_dead = stack.rows("SELECT o.operation_id, p.process_id, d.external_request_id, d.outbox_id "
                              "FROM autocheck.operations o JOIN autocheck.processes p USING(process_id) "
                              "JOIN autocheck.external_requests x USING(operation_id) "
                              "JOIN autocheck.outbox d USING(external_request_id) "
                              "WHERE d.state='DEAD' AND p.state='WAITING_SIGNAL'")
        manual = stack.rows("SELECT o.operation_id, p.process_id, s.step_instance_id "
                            "FROM autocheck.operations o JOIN autocheck.processes p USING(process_id) "
                            "JOIN autocheck.steps s USING(process_id) "
                            "WHERE p.state='WAITING_MANUAL' AND s.step_type='MANUAL' AND s.state='WAITING'")
        require(len(old_dead) == len(manual) == 1, "Earlier waiting fixtures are missing")

        pending_inbox = sql.delivery(db)
        message = "restart-inbox-" + uuid.uuid4().hex
        receipt = {"version": 1, "messageId": message, "providerPaymentId": message,
                   "externalRequestId": pending_inbox["external"], "outcome": "COMPLETED",
                   "occurredAt": "2026-09-04T12:00:00Z"}
        status, envelope = stack.signed_receipt(receipt)
        require(status == 200, "Pending Inbox receipt was rejected")
        sql.ok(envelope, "Pending Inbox receipt")
        inbox = db.json("SELECT to_jsonb(i) FROM autocheck.inbox i WHERE message_id="
                        + sql_text(message) + ";", "pending Inbox")
        require(inbox["state"] == "RECEIVED" and inbox["applied_at"] is None,
                "Inbox was applied before restart")
        pending_receipt = stack.receipt_rows(pending_inbox["external"])
        require(len(pending_receipt) == 1 and pending_receipt[0]["message_id"] == message,
                "Pending receipt identity differs")
        flow = restart_flow(stack)

        delivery = sql.delivery(db)
        first_lease = sql.claim(db, delivery, "restart-outbox")
        failure = db.json("BEGIN; SET LOCAL ROLE outbox_dispatcher; "
                          "SET LOCAL course.outbox_backoff_base_ms='60000'; "
                          "SET LOCAL course.outbox_backoff_max_ms='60000'; "
                          "SET LOCAL course.outbox_jitter_max_ms='0'; "
                          "SELECT delivery.fail_outbox(" + sql_text(delivery["outbox"]) + "::uuid,"
                          "'restart-outbox'," + str(first_lease["lease_version"])
                          + ",'http.503.retryable'); COMMIT;", "durable Outbox retry")
        require(sql.ok(failure, "Outbox retry")["outcome"] == "RETRY_WAIT",
                "Outbox did not enter RETRY_WAIT")
        outbox = sql.outbox(db, delivery)
        require(outbox["attempt_count"] == 1 and outbox["lease_version"] == first_lease["lease_version"]
                and outbox["last_error_code"] == "http.503.retryable"
                and outbox["lease_owner"] is None, "Outbox retry evidence differs")
        identity = db.json("SELECT jsonb_build_object('correlationId',correlation_id,"
                           "'amount',amount::text,'currency',currency,'payloadHash',payload_hash) "
                           "FROM payment.external_requests WHERE external_request_id="
                           + sql_text(delivery["external"]) + ";", "provider request identity")
        body = e2e.compact({"operationId": delivery["external"], "amount": identity["amount"],
                            "currency": identity["currency"]})
        require(hashlib.sha256(body).hexdigest() == identity["payloadHash"],
                "Stored provider body hash differs from exact request bytes")
        require(stack.audit(delivery["external"])[0] == 404,
                "Provider received the retry fixture before restart")
        require(stack.receipt_rows(delivery["external"]) == [],
                "Outbox retry fixture already has a receipt")

        retry_process = start_restart_job(stack, flow)
        first_job = claim_job(db, retry_process)
        fail_job(db, first_job, True)
        retry_job = one_job(db, retry_process)
        require(retry_job["state"] == "RETRY_WAIT" and retry_job["attempt_count"] == 1
                and retry_job["job_id"] == first_job["job_id"]
                and retry_job["execution_id"] == first_job["execution_id"],
                "Workflow retry fixture differs")
        first_attempt = db.json("SELECT to_jsonb(a) FROM autocheck.attempts a WHERE attempt_id="
                                + sql_text(first_job["attempt_id"]) + "::uuid;", "first retry attempt")
        require(first_attempt["status"] == "FAILED", "First workflow attempt did not fail")
        require(db.json("SELECT to_jsonb(count(*)) FROM autocheck.operations WHERE request_id="
                        + sql_text(retry_job["execution_id"]) + ";", "pre-restart job effects") == 0,
                "Workflow retry action ran before restart")
        retry_events = db.json("SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY event_id),'[]'::jsonb) "
                               "FROM workflow.workflow_events e WHERE process_id="
                               + sql_text(retry_process) + "::uuid;", "retry history before restart")
        require(any(event["event_type"] == "TaskRetryScheduled"
                    and event["metadata"]["delayMs"] == 30000 for event in retry_events),
                "Workflow retry did not persist the contractual 30000 ms delay")
        ready_process = start_restart_job(stack, flow)
        ready_job = one_job(db, ready_process)
        require(ready_job["state"] == "READY" and ready_job["attempt_count"] == 0,
                "READY fixture was consumed before restart")
        clock = restart_retry_clock(db, retry_job, outbox)
        outbox_delay = db.json("SELECT to_jsonb(extract(epoch FROM (next_attempt_at-updated_at))*1000) "
                               "FROM delivery.outbox WHERE outbox_id="
                               + sql_text(delivery["outbox"]) + "::uuid;", "Outbox retry delay")
        require(outbox_delay == 60000, "Outbox did not persist the exact 60000 ms policy delay")
        require(0 < clock["workflowMs"] <= 30000 and 0 < clock["outboxMs"] <= 60000,
                "Retry deadline passed before full restart")
        mixed = db.json("SELECT jsonb_build_object("
                        "'ready',(SELECT count(*) FROM autocheck.jobs WHERE state='READY'),"
                        "'jobRetry',(SELECT count(*) FROM autocheck.jobs WHERE state='RETRY_WAIT'),"
                        "'outboxRetry',(SELECT count(*) FROM autocheck.outbox WHERE state='RETRY_WAIT'),"
                        "'waitingSignal',(SELECT count(*) FROM autocheck.processes WHERE state='WAITING_SIGNAL'),"
                        "'waitingManual',(SELECT count(*) FROM autocheck.processes WHERE state='WAITING_MANUAL'),"
                        "'inboxReceived',(SELECT count(*) FROM autocheck.inbox WHERE state='RECEIVED'));",
                        "mixed state before restart")
        require(mixed == {"ready": 1, "jobRetry": 1, "outboxRetry": 1,
                          "waitingSignal": 3, "waitingManual": 1, "inboxReceived": 1},
                "Pre-restart mixed state is incomplete")
        before = durable_snapshot(stack)
        print("EVIDENCE | mixedBefore ready=1 jobRetry=1 outboxRetry=1 "
              "waitingSignal=3 waitingManual=1 inboxReceived=1", flush=True)
    finally:
        stack.close_sessions()

    label = "label=com.docker.compose.project=" + stack.project
    volumes = stack.run(["docker", "volume", "ls", "-q", "--filter", label], "Volume identity")
    require(bool(volumes) and "\n" not in volumes, "Expected one isolated volume")
    old_binding = stack.base_url.removeprefix("http://")
    require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", old_binding) is not None,
            "Original gateway has no isolated loopback port")
    stack.compose("down", "--remove-orphans", "--timeout", "5")
    stack.compose("up", "-d", "--no-build", "--no-deps", "postgres")

    def postgres_ready() -> bool:
        try:
            stack.compose("exec", "-T", "postgres", "pg_isready", "-U", "postgres", "-d", "course")
            return True
        except e2e.CheckFailed:
            return False

    stack.until("PostgreSQL restart readiness", postgres_ready, timeout=120)
    require(durable_snapshot(stack) == before,
            "PostgreSQL-only restart changed durable business state or exact deadlines")
    pg_identity = stack.sql("SELECT jsonb_build_object('correlationId',correlation_id,"
                            "'amount',amount::text,'currency',currency,'payloadHash',payload_hash) "
                            "FROM payment.external_requests WHERE external_request_id="
                            + sql_text(delivery["external"]) + ";")
    require(pg_identity == identity, "PostgreSQL-only restart changed provider request identity")
    pg_clock = stack.sql("SELECT jsonb_build_object('workflowMs',extract(epoch FROM ("
                         + sql_text(retry_job["next_attempt_at"])
                         + "::timestamptz-clock_timestamp()))*1000,'outboxMs',extract(epoch FROM ("
                         + sql_text(outbox["next_attempt_at"])
                         + "::timestamptz-clock_timestamp()))*1000);")
    print(f"EVIDENCE | postgresOnlySnapshot=equal workflowRemainingMs={pg_clock['workflowMs']:.0f} "
          f"outboxRemainingMs={pg_clock['outboxMs']:.0f}", flush=True)
    stack.compose("up", "-d", "--no-build")
    new_binding = stack.compose("port", "gateway", "8080")
    require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", new_binding) is not None,
            "Restarted gateway has no isolated loopback port")
    stack.base_url = "http://" + new_binding
    require(stack.base_url == "http://" + stack.compose("port", "gateway", "8080"),
            "Gateway base URL differs from the published port")
    stack.until("Gateway host readiness", lambda: stack.http("GET", "/health/ready")[0] == 200,
                timeout=45)
    print(f"EVIDENCE | gatewayHostPort old={old_binding.rsplit(':', 1)[1]} "
          f"new={new_binding.rsplit(':', 1)[1]} readiness=200", flush=True)
    stack.wait_ready()
    require(stack.run(["docker", "volume", "ls", "-q", "--filter", label], "Restored volume") == volumes,
            "Restart changed the volume")
    cli_id = stack.container("cli")[0]
    require(stack.run(["docker", "inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", cli_id],
                      "CLI restart result") == "exited 0", "Restart CLI did not finish successfully")
    history = stack.sql("SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) FROM public.course_schema_migrations m;")
    require(history == before["migrations"], "Restart reapplied or changed migration history")
    db = PsqlSession(stack, "recovered-retries")
    try:
        ready_at = db.json("SELECT to_jsonb(clock_timestamp());", "consumer readiness timestamp")
        after_ready = restart_retry_clock(db, retry_job, outbox)
        print(f"EVIDENCE | consumersReady workflowRemainingMs={after_ready['workflowMs']:.0f} "
              f"outboxRemainingMs={after_ready['outboxMs']:.0f}", flush=True)
        require(after_ready["outboxMs"] > 0,
                "Environment timing missed the Outbox live pre-deadline window")
        live_workflow = pg_clock["workflowMs"] >= 5000 and after_ready["workflowMs"] > 0
        if not live_workflow:
            print("EVIDENCE | workflow deadline window elapsed during restart; "
                  "PostgreSQL-only snapshot preserved the first attempt", flush=True)
        retry_before_due(db, retry_job, outbox, live_workflow)

        if live_workflow:
            stack.until("Workflow pre-deadline barrier", lambda: retry_before_due(
                db, retry_job, outbox, True)["workflowMs"] <= 0,
                timeout=after_ready["workflowMs"] / 1000 + 10)
        stack.until("Workflow retry completion", lambda: (
            db.json("SELECT to_jsonb(state='COMPLETED') FROM autocheck.processes WHERE process_id="
                    + sql_text(retry_process) + "::uuid;", "recovered workflow") is True), timeout=10)
        attempts = db.json("SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY attempt_number),'[]'::jsonb) "
                           "FROM autocheck.attempts a WHERE job_id="
                           + sql_text(retry_job["job_id"]) + "::uuid;", "recovered attempts")
        final_job = one_job(db, retry_process)
        require(len(attempts) == 2 and attempts[0] == first_attempt
                and attempts[1]["status"] == "SUCCEEDED"
                and attempts[1]["attempt_id"] != first_attempt["attempt_id"]
                and attempts[1]["execution_id"] == retry_job["execution_id"]
                and final_job["job_id"] == retry_job["job_id"]
                and final_job["step_instance_id"] == retry_job["step_instance_id"]
                and final_job["execution_id"] == retry_job["execution_id"]
                and final_job["lease_version"] > retry_job["lease_version"]
                and final_job["attempt_count"] == 2 and final_job["state"] == "SUCCEEDED",
                "Workflow retry lost identity, fencing or attempt history")
        start_ms = db.json("SELECT to_jsonb(extract(epoch FROM (a.started_at-"
                           + sql_text(retry_job["next_attempt_at"])
                           + "::timestamptz))*1000) FROM autocheck.attempts a WHERE attempt_id="
                           + sql_text(attempts[1]["attempt_id"]) + "::uuid;", "workflow pickup timestamp")
        bound_ms = db.json("SELECT to_jsonb(extract(epoch FROM (a.started_at-"
                           + sql_text(ready_at) + "::timestamptz))*1000) "
                           "FROM autocheck.attempts a WHERE attempt_id="
                           + sql_text(attempts[1]["attempt_id"]) + "::uuid;", "consumer pickup bound")
        require(start_ms >= -50 and (start_ms <= 10000 if live_workflow else bound_ms <= 10000),
                "Workflow retry started before deadline or outside bounded pickup interval")
        job_effects = stack.rows("SELECT operation_id, request_id FROM autocheck.operations WHERE request_id="
                                 + sql_text(retry_job["execution_id"]))
        job_events = stack.rows("SELECT event_type, count(*)::int AS n FROM autocheck.workflow_events "
                                "WHERE process_id=" + sql_text(retry_process)
                                + "::uuid GROUP BY event_type")
        event_counts = {row["event_type"]: row["n"] for row in job_events}
        final_events = stack.rows("SELECT event_id FROM autocheck.workflow_events WHERE process_id="
                                  + sql_text(retry_process) + "::uuid")
        require({event["event_id"] for event in retry_events}.issubset(
                {event["event_id"] for event in final_events}),
                "Restart lost pre-existing workflow history")
        require(len(job_effects) == 1 and event_counts.get("TaskClaimed") == 2
                and event_counts.get("TaskRetryScheduled") == 1
                and event_counts.get("TaskSucceeded") == 1
                and event_counts.get("ProcessCompleted") == 1
                and event_counts.get("TaskFailed", 0) == 0,
                "Workflow retry created duplicate or missing terminal effects/events")
        created = stack.rows("SELECT event_type FROM autocheck.operation_events WHERE operation_id="
                             + sql_text(job_effects[0]["operation_id"]) + "::uuid")
        require(created == [{"event_type": "OPERATION_CREATED"}],
                "Workflow retry did not create exactly one operation event")
        print(f"EVIDENCE | workflowRetry deadline={retry_job['next_attempt_at']} "
              f"startedAt={attempts[1]['started_at']} offsetMs={start_ms:.1f} effects=1", flush=True)

        stack.until("Outbox pre-deadline barrier", lambda: retry_before_due(
            db, retry_job, outbox, False)["outboxMs"] <= 0,
            timeout=after_ready["outboxMs"] / 1000 + 10)
        stack.until("Outbox retry claim", lambda: sql.outbox(db, delivery)["attempt_count"] >= 2,
                    timeout=10)
        final_outbox = sql.outbox(db, delivery)
        claim_ms = db.json("SELECT to_jsonb(extract(epoch FROM (next_attempt_at-"
                           + sql_text(outbox["next_attempt_at"])
                           + "::timestamptz))*1000) FROM autocheck.outbox WHERE outbox_id="
                           + sql_text(delivery["outbox"]) + "::uuid;", "Outbox claim timestamp")
        require(final_outbox["outbox_id"] == outbox["outbox_id"]
                and final_outbox["external_request_id"] == outbox["external_request_id"]
                and final_outbox["attempt_count"] == 2
                and final_outbox["lease_version"] > outbox["lease_version"]
                and 0 <= claim_ms <= 10000,
                "Dispatcher lost identity or claimed outside Outbox deadline window")
        after_identity = db.json("SELECT jsonb_build_object('correlationId',correlation_id,"
                                 "'amount',amount::text,'currency',currency,'payloadHash',payload_hash) "
                                 "FROM payment.external_requests WHERE external_request_id="
                                 + sql_text(delivery["external"]) + ";", "recovered provider identity")
        require(after_identity == identity, "Provider correlation or exact body changed on retry")
        audit = stack.assert_delivery(delivery["operation"], delivery["process"], "COMPLETED")
        require(audit["requestCount"] == 1, "Outbox retry sent more than one provider request")
        print(f"EVIDENCE | outboxRetry deadline={outbox['next_attempt_at']} "
              f"claimAt={final_outbox['next_attempt_at']} offsetMs={claim_ms:.1f} "
              "attempts=2 providerPayments=1", flush=True)

        stack.assert_terminal(pending_inbox["operation"], pending_inbox["process"], "COMPLETED")
        applied_inbox = stack.rows("SELECT state, applied_at, body_hash, received_at "
                                   "FROM autocheck.inbox WHERE message_id="
                                   + sql_text(message))
        require(len(applied_inbox) == 1 and applied_inbox[0]["state"] == "APPLIED"
                and applied_inbox[0]["applied_at"] is not None
                and applied_inbox[0]["body_hash"] == inbox["body_hash"]
                and applied_inbox[0]["received_at"] == inbox["received_at"],
                "Restart did not apply saved Inbox exactly once")
        final_receipt = stack.receipt_rows(pending_inbox["external"])
        require(len(final_receipt) == 1 and all(final_receipt[0][key] == pending_receipt[0][key]
                for key in ("message_id", "body_hash", "received_at")),
                "Restart changed the pending receipt identity")
        stack.until("READY job completion", lambda: (
            db.json("SELECT to_jsonb(state='COMPLETED') FROM autocheck.processes WHERE process_id="
                    + sql_text(ready_process) + "::uuid;", "READY recovery") is True), timeout=10)
        ready_final = one_job(db, ready_process)
        require(ready_final["job_id"] == ready_job["job_id"]
                and ready_final["execution_id"] == ready_job["execution_id"]
                and ready_final["attempt_count"] == 1 and ready_final["state"] == "SUCCEEDED"
                and len(stack.rows("SELECT operation_id FROM autocheck.operations WHERE request_id="
                                   + sql_text(ready_job["execution_id"]))) == 1,
                "READY job did not resume exactly once")

        reviewer = e2e.make_token(stack.env, "restart-reviewer", "workflow:manual")
        decision = {"processId": manual[0]["process_id"],
                    "stepInstanceId": manual[0]["step_instance_id"],
                    "decision": "APPROVED", "reason": "Restart recovery approval"}
        status, envelope = stack.action("workflow", "manual", decision, token=reviewer)
        require(status == 200, "Waiting manual decision was rejected after restart")
        sql.ok(envelope, "Manual restart decision")
        stack.assert_terminal(manual[0]["operation_id"], manual[0]["process_id"], "COMPLETED")
        require(len(stack.rows("SELECT decision_id FROM autocheck.decisions WHERE process_id="
                               + sql_text(manual[0]["process_id"]) + "::uuid")) == 1,
                "Manual restart created duplicate decisions")

        diagnostics_token = e2e.make_token(stack.env, "restart-diagnostics", "diagnostics:read")
        status, stalled = stack.action("diagnostics", "stalled", {}, token=diagnostics_token)
        require(status == 200 and stalled.get("status") == "ok"
                and old_dead[0]["operation_id"] in {item["operationId"] for item in stalled["result"]["items"]},
                "Old DEAD operation is absent from diagnostics.stalled")
        late_id = "restart-late-" + uuid.uuid4().hex
        late = {"version": 1, "messageId": late_id, "providerPaymentId": late_id,
                "externalRequestId": old_dead[0]["external_request_id"], "outcome": "COMPLETED",
                "occurredAt": "2026-09-04T12:00:00Z"}
        status, envelope = stack.signed_receipt(late)
        require(status == 200, "Late receipt was rejected after restart")
        sql.ok(envelope, "Late receipt")
        stack.assert_terminal(old_dead[0]["operation_id"], old_dead[0]["process_id"], "COMPLETED")
        status, stalled = stack.action("diagnostics", "stalled", {}, token=diagnostics_token)
        recovered_ids = {old_dead[0]["operation_id"], delivery["operation"],
                         pending_inbox["operation"], manual[0]["operation_id"]}
        require(status == 200 and stalled.get("status") == "ok"
                and not recovered_ids.intersection(item["operationId"] for item in stalled["result"]["items"]),
                "Recovered operation remains in diagnostics.stalled")
        for operation_id in (delivery["operation"], pending_inbox["operation"],
                             old_dead[0]["operation_id"], manual[0]["operation_id"]):
            status, trace = stack.action("diagnostics", "trace", {"identifier": operation_id},
                                         token=diagnostics_token)
            require(status == 200 and trace.get("status") == "ok"
                    and trace["result"]["operation"]["operationId"] == operation_id,
                    "Recovered diagnostics.trace disagrees with operation identity")
        all_endpoints(stack)
        assert_values(stack, db, (0, 0, 0, 1), "mixed full restart settled")
        print("EVIDENCE | mixedAfter ready=0 waiting=0 inbox=APPLIED "
              "manualDecisions=1 stalledRecovered=0 liveReadyMetrics=8/8", flush=True)
    finally:
        stack.close_sessions()


def report_failure(error: Exception) -> None:
    safe = isinstance(error, (CheckFailed, e2e.CheckFailed))
    print("FAIL | " + (str(error) if safe else type(error).__name__ + " (details withheld)"), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List six scenarios without Docker")
    args = parser.parse_args()
    if args.list:
        print("\n".join(SCENARIOS))
        return 0
    passed, failed = 0, False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix="week4-observability-") as directory:
            stack = Stack(Path(directory))
            try:
                print("PROJECT | " + stack.project, flush=True)
                stack.start()
                for name, action in zip(SCENARIOS, (all_endpoints, postgres_readiness, provider_readiness,
                                                    metrics_contract, metrics_values, restart_recovery)):
                    print("RUN | " + name, flush=True)
                    action(stack)
                    passed += 1
                    print("PASS | " + name, flush=True)
            finally:
                previous_int = signal.signal(signal.SIGINT, signal.SIG_IGN)
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                try:
                    try:
                        stack.cleanup()
                    except Exception as error:
                        failed = True
                        report_failure(error)
                finally:
                    signal.signal(signal.SIGINT, previous_int)
                    signal.signal(signal.SIGTERM, interrupted)
    except KeyboardInterrupt:
        failed = True
        print("FAIL | interrupted", flush=True)
    except Exception as error:
        failed = True
        report_failure(error)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    result = "PASSED" if passed == len(SCENARIOS) and not failed else "FAILED"
    print(f"SUMMARY | passed={passed} total={len(SCENARIOS)} result={result}", flush=True)
    return 0 if result == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
