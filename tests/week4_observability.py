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
        print("SETUP | fresh 001-020 init plus CLI forward 021-024; eight services ready", flush=True)

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
    for view, key in (("processes", "process_id"), ("jobs", "job_id"), ("attempts", "attempt_id"),
                      ("workflow_events", "event_id"), ("outbox", "outbox_id"), ("inbox", "message_id")):
        fields.append(sql_text(view) + ", (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY " + key
                      + "),'[]'::jsonb) FROM autocheck." + view + " r)")
    return stack.sql("SELECT jsonb_build_object(" + ",".join(fields) + ");")


def restart_recovery(stack: Stack) -> None:
    stack.wait_ready()
    before = durable_snapshot(stack)
    label = "label=com.docker.compose.project=" + stack.project
    volumes = stack.run(["docker", "volume", "ls", "-q", "--filter", label], "Volume identity")
    require(bool(volumes) and "\n" not in volumes, "Expected one isolated volume")
    cli_id = stack.container("cli")[0]
    cli_state = stack.run(["docker", "inspect", "--format", "{{.State.StartedAt}} {{.State.ExitCode}}", cli_id], "CLI identity")
    services = (*SERVICES, "gateway", "provider-simulator")
    try:
        stack.compose("stop", "--timeout", "5", *services, "postgres")
    finally:
        # Reuse only the named existing containers/volume and omit CLI.
        # No bootstrap, migration apply, rebuild or force-recreate is issued.
        stack.start_existing("postgres")
        stack.start_existing(*services)
    stack.wait_ready()
    all_endpoints(stack)
    require(stack.run(["docker", "volume", "ls", "-q", "--filter", label], "Restored volume") == volumes,
            "Restart changed the volume")
    require(stack.container("cli")[0] == cli_id and stack.run(
        ["docker", "inspect", "--format", "{{.State.StartedAt}} {{.State.ExitCode}}", cli_id], "CLI restart evidence") == cli_state,
        "Restart unexpectedly ran CLI bootstrap")
    require(durable_snapshot(stack) == before, "Restart changed durable state or migration history")
    db = PsqlSession(stack, "recovered-metrics")
    try:
        assert_values(stack, db, (0, 2, 0, 1), "same-volume recovery")
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
