"""Isolated Week 4 SQL regressions. Run from the repository root.

Only PostgreSQL and CLI are built/started. Production CLI publishes the tracked
actions/maps and applies only forward migrations. Domain writes go through
api.invoke and workflow/delivery boundaries, never direct table DML. This is a
SQL boundary test, not an HTTP signature/Worker mapper/provider E2E test.
--list and --help do not invoke Docker. Host dependencies: Python stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time
import uuid

from signal_process_concurrency import (
    CheckFailed, PsqlSession, Stack as BaseStack, compact, decode, interrupted,
    report_failure, require, sql_text, wait_until_blocked,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "db/migrations/week4"
FORWARD_FILES = (
    "021_week4_delivery_policy.sql",
    "022_fix_reconcile_inbox_applied_count.sql",
    "023_week4_observability_metrics.sql",
    "024_week4_job_lease_heartbeat.sql",
    "025_diagnostics_stalled.sql",
    "026_diagnostics_trace.sql",
    "027_workflow_lease_expiry_fencing.sql",
    "028_diagnostics_stalled_threshold.sql",
)
SCENARIOS = (
    "reader-privileges", "migration-repeat", "retry-backoff-dead",
    "expired-lease-budget", "late-receipt-preserves-dead-evidence",
    "reconcile-concurrency-commit", "reconcile-concurrency-rollback", "job-lease-heartbeat",
    "expired-finish-before-reclaim", "expired-fail-before-reclaim", "upgrade-026-to-028",
)
VIEWS = (
    "contract_info", "action_definitions", "action_dispatches", "operations", "operation_events",
    "flow_versions", "processes", "steps", "jobs", "attempts", "signals", "workflow_events",
    "external_requests", "receipts", "outbox", "inbox", "decisions",
)
REPLACED = (
    "delivery.claim_outbox(text,integer)", "delivery.fail_outbox(uuid,text,bigint,text)",
    "delivery.reconcile_inbox(integer)", "workflow.claim_jobs(text,integer,integer)",
    "workflow.finish_job(uuid,text,bigint,text,jsonb)",
    "workflow.fail_job(uuid,text,bigint,text,boolean)",
)
PRESERVED = (
    "delivery.succeed_outbox(uuid,text,bigint,text)", "receipt.accept_v1(jsonb,jsonb)",
    "workflow.signal_process(uuid,text,text,jsonb,text)",
)


def forward_files() -> list[Path]:
    files = sorted(MIGRATIONS.glob("*.sql"))
    require(tuple(p.name for p in files) == FORWARD_FILES, "Unexpected forward migration set")
    return files


def sql_json(value: object) -> str:
    return sql_text(compact(value)) + "::jsonb"


def ok(value: object, label: str) -> dict:
    require(isinstance(value, dict) and value.get("status") == "ok", label + ": expected success envelope")
    return value


def boundary(db: PsqlSession, role: str, expression: str):
    require(role in {"course_runtime", "workflow_worker", "outbox_dispatcher", "inbox_reconciler"},
            "Unknown test boundary role")
    return db.json("BEGIN; SET LOCAL ROLE " + role + "; SELECT " + expression + "; COMMIT;", "boundary")


def metadata(db: PsqlSession) -> list:
    names = ",".join(sql_text(name) + "::regprocedure" for name in REPLACED + PRESERVED)
    return db.json(
        "SELECT jsonb_agg(jsonb_build_object('oid', p.oid, 'name', p.oid::regprocedure::text, "
        "'owner', pg_get_userbyid(p.proowner), 'definer', p.prosecdef, 'config', p.proconfig, "
        "'volatility', p.provolatile, 'language', l.lanname, 'acl', p.proacl::text, 'body', p.prosrc) "
        "ORDER BY p.oid) FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid IN ("
        + names + ");", "function metadata")


class Stack(BaseStack):
    def __init__(self, directory: Path) -> None:
        super().__init__(directory)
        self.project = "week4-sql-" + uuid.uuid4().hex
        self.prefix[self.prefix.index("--project-name") + 1] = self.project
        self.env.update({
            "COURSE_OUTBOX_MAX_ATTEMPTS": "4", "COURSE_OUTBOX_BACKOFF_BASE_MS": "200",
            "COURSE_OUTBOX_BACKOFF_MAX_MS": "800", "COURSE_OUTBOX_JITTER_MAX_MS": "100",
            "COURSE_OUTBOX_LEASE_MS": "2000", "COURSE_JOB_LEASE_MS": "2000",
        })
        self.tags = [self.project + "-" + name + ":phase1" for name in ("postgres", "cli")]
        self.images_owned = False
        legacy = directory / "legacy-migrations"
        legacy.mkdir()
        for name in FORWARD_FILES[:6]:
            shutil.copyfile(MIGRATIONS / name, legacy / name)
        override = directory / "compose.json"
        override.write_text(compact({"services": {
            "postgres": {"image": self.tags[0]},
            "cli": {"image": self.tags[1], "volumes": [str(legacy) + ":/app/db/migrations/legacy:ro"]},
        }}), encoding="utf-8")
        self.prefix += ["-f", str(override)]

    def start(self) -> None:
        for tag in self.tags:
            require(not self.run(["docker", "image", "ls", "-q", "--filter", "reference=" + tag],
                                 "Private image preflight", timeout=30), "Private image tag already exists")
        self.images_owned = True
        super().start()

    def bootstrap(self, applied: int, skipped: int) -> None:
        raw = self.compose("run", "--rm", "-T", "--no-deps", "cli", "bootstrap")
        envelopes = [decode(line, "Bootstrap envelope") for line in raw.splitlines() if line.strip()]
        require(len(envelopes) == 2, "Bootstrap must return migration and publication envelopes")
        migration = ok(envelopes[0], "Bootstrap migration")["result"]
        require(migration == {"resource": "migration", "operation": "applied",
                              "applied": applied, "skipped": skipped}, "Unexpected migration counts")
        published = ok(envelopes[1], "Bootstrap publication")["result"]
        require(published.get("resource") == "bootstrap" and published.get("actions", 0) >= 13
                and published.get("flows", 0) >= 2, "Production bootstrap did not publish tracked fixtures")
        print(f"BOOTSTRAP | applied={applied} skipped={skipped}; tracked actions/maps published", flush=True)

    def cleanup(self) -> None:
        failed = False
        try:
            super().cleanup()
        except Exception:
            failed = True
        finally:
            if self.cleanup_armed:
                try:
                    self.assert_no_resources("Independent cleanup verification")
                    print("CLEANUP | containers=0 volumes=0 networks=0", flush=True)
                except Exception:
                    failed = True
            if self.images_owned:
                for tag in self.tags:
                    try:
                        command = ["docker", "image", "ls", "-q", "--filter", "reference=" + tag]
                        if self.run(command, "Private image lookup", timeout=30):
                            self.run(["docker", "image", "rm", tag], "Private image removal", timeout=60)
                        require(not self.run(command, "Private image verification", timeout=30),
                                "Private image tag remains")
                    except Exception:
                        failed = True
        require(not failed, "Isolated cleanup failed (details withheld)")
        print("CLEANUP | private image tags=0", flush=True)


def reader_privileges(stack: Stack, db: PsqlSession) -> None:
    # Repeated privileged invocation must retain the explicit view whitelist.
    stack.compose("exec", "-T", "postgres", "sh", "/docker-entrypoint-initdb.d/000_week4_roles.sh")
    facts = db.json("""
        SELECT jsonb_build_object(
          'role', (SELECT rolcanlogin AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
                   FROM pg_roles WHERE rolname='autocheck_reader'),
          'memberships', (SELECT count(*) FROM pg_auth_members WHERE member='autocheck_reader'::regrole),
          'connect', has_database_privilege('autocheck_reader','course','CONNECT'),
          'create', has_database_privilege('autocheck_reader','course','CREATE'),
          'temp', has_database_privilege('autocheck_reader','course','TEMP'),
          'schemas', (SELECT COALESCE(jsonb_agg(nspname ORDER BY nspname),'[]'::jsonb) FROM pg_namespace
                      WHERE nspname !~ '^pg_' AND nspname <> 'information_schema'
                        AND has_schema_privilege('autocheck_reader',oid,'USAGE')),
          'schemaCreate', (SELECT count(*) FROM pg_namespace WHERE nspname !~ '^pg_'
                           AND has_schema_privilege('autocheck_reader',oid,'CREATE')),
          'physical', (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','m','f')
                         AND (has_table_privilege('autocheck_reader',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                              OR has_any_column_privilege('autocheck_reader',c.oid,'SELECT,INSERT,UPDATE,REFERENCES'))),
          'functions', (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                        WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
                          AND has_function_privilege('autocheck_reader',p.oid,'EXECUTE')),
          'sequences', (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                        WHERE n.nspname !~ '^pg_' AND CASE WHEN c.relkind='S'
                          THEN has_sequence_privilege('autocheck_reader',c.oid,'USAGE,SELECT,UPDATE') ELSE false END),
          'views', (SELECT jsonb_agg(c.relname ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='autocheck' AND c.relkind='v' AND has_table_privilege('autocheck_reader',c.oid,'SELECT')),
          'viewWrite', (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                        WHERE n.nspname='autocheck' AND c.relkind='v'
                          AND has_table_privilege('autocheck_reader',c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')));
    """, "reader effective privileges")
    require(facts == {"role": True, "memberships": 0, "connect": True, "create": False, "temp": False,
                      "schemas": ["autocheck"], "schemaCreate": 0, "physical": 0, "functions": 0,
                      "sequences": 0, "views": sorted(VIEWS), "viewWrite": 0}, "Reader effective privileges differ")
    # Password is expanded only inside the owned container, never an argument.
    raw = stack.compose("exec", "-T", "postgres", "sh", "-c",
                        'PGPASSWORD="$COURSE_AUTOCHECK_PASSWORD" exec psql -X -q -A -t -v ON_ERROR_STOP=1 '
                        '-h 127.0.0.1 -U autocheck_reader -d course',
                        stdin="SELECT count(*) FROM autocheck.contract_info;\n")
    require(raw == "1", "Reader authenticated projection query failed")


def migration_repeat(stack: Stack, db: PsqlSession, baseline: list) -> None:
    installed = metadata(db)
    require(len(baseline) == len(installed) == len(REPLACED) + len(PRESERVED), "Missing migration boundary metadata")
    for before, after in zip(baseline, installed):
        require({k: v for k, v in before.items() if k != "body"}
                == {k: v for k, v in after.items() if k != "body"}, "Migration changed OID/owner/ACL/security")
        if before["name"] in PRESERVED:
            require(before == after, "An unchanged production boundary was replaced")
        else:
            name = before["name"].split("(")[0]
            path = MIGRATIONS / ("022_fix_reconcile_inbox_applied_count.sql" if "reconcile_inbox" in name
                                 else "024_week4_job_lease_heartbeat.sql" if "claim_jobs" in name
                                 else "027_workflow_lease_expiry_fencing.sql" if name in
                                 {"workflow.finish_job", "workflow.fail_job"}
                                 else "021_week4_delivery_policy.sql")
            match = re.search(r"CREATE OR REPLACE FUNCTION " + re.escape(name)
                              + r"\(.*?AS \$\$(.*?)\$\$;", path.read_text(encoding="utf-8"), re.S)
            require(match is not None and after["body"].replace("\r\n", "\n").strip() == match[1].strip(),
                    "Installed forward function body differs")
    for row in installed:
        if row["name"].startswith(("workflow.finish_job(", "workflow.fail_job(")):
            require({k: row[k] for k in ("owner", "definer", "config", "volatility", "language")} == {
                "owner": "course_owner", "definer": True,
                "config": ["search_path=pg_catalog, workflow"], "volatility": "v", "language": "plpgsql"},
                "Workflow function security metadata differs")
            require(row["acl"] == next(old["acl"] for old in baseline if old["name"] == row["name"]),
                    "Workflow function ACL changed")
    renewal = renewal_metadata(db)
    expected = [{"file_name": p.name, "checksum": hashlib.sha256(p.read_bytes()).hexdigest()}
                for p in forward_files()]
    history = "SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) FROM (SELECT file_name,checksum FROM public.course_schema_migrations) m;"
    require(db.json(history, "migration checksums") == expected, "Forward migration checksums/history differ")
    stack.bootstrap(0, len(expected))
    require(metadata(db) == installed and renewal_metadata(db) == renewal
            and db.json(history, "repeat checksums") == expected,
            "Repeated bootstrap changed functions or migration history")


def renewal_metadata(db: PsqlSession) -> dict:
    signature = "workflow.renew_job_lease(uuid,text,bigint,integer)"
    row = db.json("SELECT jsonb_build_object('oid',p.oid,'owner',pg_get_userbyid(p.proowner),"
                  "'definer',p.prosecdef,'config',p.proconfig,'volatility',p.provolatile,"
                  "'language',l.lanname,'body',p.prosrc,'acl',p.proacl::text,"
                  "'grantees',(SELECT jsonb_agg(pg_get_userbyid(a.grantee) ORDER BY a.grantee) "
                  "FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a "
                  "WHERE a.privilege_type='EXECUTE'),"
                  "'effective',(SELECT jsonb_object_agg(r,has_function_privilege(r,p.oid,'EXECUTE')) "
                  "FROM unnest(ARRAY['workflow_worker','course_runtime','course_publication',"
                  "'autocheck_reader','outbox_dispatcher','inbox_reconciler']) r)) "
                  "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid="
                  + sql_text(signature) + "::regprocedure;", "renewal security")
    require({k: row[k] for k in ('owner', 'definer', 'config', 'volatility', 'language')} == {
        'owner': 'course_owner', 'definer': True, 'config': ['search_path=pg_catalog, workflow'],
        'volatility': 'v', 'language': 'plpgsql'}, "Renewal security metadata differs")
    require(set(row['grantees']) == {'course_owner', 'workflow_worker'}
            and row['effective'] == {r: r == 'workflow_worker' for r in row['effective']},
            "Renewal EXECUTE privileges are too broad")
    body = re.search(r"CREATE FUNCTION workflow\.renew_job_lease\(.*?AS \$\$(.*?)\$\$;",
                     (MIGRATIONS / "024_week4_job_lease_heartbeat.sql").read_text(encoding="utf-8"), re.S)
    require(body is not None and row['body'].replace("\r\n", "\n").strip() == body[1].strip(),
            "Installed renewal body differs from migration 024")
    return row


def job_lease_heartbeat(stack: Stack, db: PsqlSession) -> None:
    renewal_metadata(db)
    name = "lease-" + uuid.uuid4().hex
    operation = invoke(db, "payment", "request", {
        "operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"})['operationId']
    document = {
        "contract_version": "course-1", "flow_name": name, "version": 1, "start_step": "validate",
        "steps": [{"key": "validate", "type": "automatic", "task": {
            "service": "postgres", "module": "payment", "action": "validate", "action_version": 1,
            "required_policy": ["payment:internal"], "timeout_ms": 30000,
            "retry": {"max_attempts": 1, "delays_ms": []},
            "input_mapping": {"/operationId": "/operationId"}, "input_constants": {}}},
            {"key": "done", "type": "end", "outcome": "FINISHED"}],
        "transitions": [{"from": "validate", "outcome": "VALID", "to": "done"}],
    }
    stack.cli("flow", "publish", "/dev/stdin", payload=document)
    stack.cli("flow", "activate", name, "--version", "1")
    process = stack.cli("flow", "start", name, "--business-key", uuid.uuid4().hex,
                        "--data", "/dev/stdin", payload={"operationId": operation})['result']['processId']

    def reserve(owner: str) -> dict:
        rows = boundary(db, "workflow_worker", "COALESCE((SELECT jsonb_agg(to_jsonb(j)) "
                        "FROM workflow.claim_jobs(" + sql_text(owner) + ",1,2000) j),'[]'::jsonb)")
        require(len(rows) == 1 and rows[0]['process_id'] == process and rows[0]['timeout_ms'] == 30000,
                "Wrong long-timeout job claimed")
        return rows[0]

    first = reserve('lease-a')
    job_id = sql_text(first['job_id']) + "::uuid"
    snapshot = ("SELECT jsonb_build_object('job',to_jsonb(j),'duration',"
                "extract(epoch FROM (j.lease_until-j.updated_at))*1000,'attempts',"
                "(SELECT jsonb_agg(to_jsonb(a) ORDER BY attempt_number) FROM workflow.task_attempts a WHERE a.job_id=j.job_id),"
                "'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM workflow.workflow_events e "
                "WHERE e.process_id=j.process_id)) FROM workflow.workflow_jobs j WHERE j.job_id=" + job_id + ";")

    def evidence() -> dict:
        return db.json(snapshot, "job lease evidence")

    def renew(owner: str, version: int, lease: int = 2000):
        return boundary(db, "workflow_worker", "to_jsonb(workflow.renew_job_lease(" + job_id + ","
                        + sql_text(owner) + "," + str(version) + "," + str(lease) + "))")

    before = evidence()
    require(before['duration'] == 2000, "Claim lease includes task timeout instead of only lease_ms")
    require(renew('lease-a', first['lease_version']) is True, "Live owner could not renew")
    after = evidence()
    require(after['duration'] == 2000 and after['job']['lease_until'] > before['job']['lease_until'],
            "Renewal did not extend by captured timestamp plus 2000ms")
    for row in (before, after):
        row['job'].pop('lease_until')
        row['job'].pop('updated_at')
    require(after == before, "Renewal changed identity, attempts, counts or workflow events")
    stable = evidence()
    for owner, version, lease in [('wrong-owner', first['lease_version'], 2000),
                                  ('lease-a', first['lease_version'] + 1, 2000),
                                  ('', first['lease_version'], 2000),
                                  ('lease-a', first['lease_version'], 99),
                                  ('lease-a', first['lease_version'], 300001)]:
        require(renew(owner, version, lease) is False and evidence() == stable, "Invalid renewal changed job")
    deadline = time.monotonic() + 10
    while True:
        require(time.monotonic() < deadline, "Job expiry barrier deadline exceeded")
        if db.json("SELECT to_jsonb(lease_until <= clock_timestamp()) FROM workflow.workflow_jobs WHERE job_id="
                   + job_id + ";", "job expiry", timeout=max(0.001, deadline-time.monotonic())) is True:
            break
    require(renew('lease-a', first['lease_version']) is False and evidence() == stable,
            "Expired lease was resurrected")
    second = reserve('lease-b')
    require(second['execution_id'] == first['execution_id']
            and second['lease_version'] > first['lease_version'], "Reclaim changed identity/fencing incorrectly")
    stable = evidence()
    require([a['status'] for a in stable['attempts']] == ['STALE', 'RUNNING'], "Reclaim attempt history differs")
    require(renew('lease-a', first['lease_version']) is False, "Previous worker renewed reclaimed lease")
    stale = boundary(db, "workflow_worker", "workflow.finish_job(" + job_id + ",'lease-a',"
                     + str(first['lease_version']) + ",'VALID','{}'::jsonb)")
    require(stale.get('code') == 'workflow.lease_stale' and evidence() == stable, "Stale finish changed job")
    # Finish through the production failure boundary, then prove terminal fencing.
    ok(boundary(db, "workflow_worker", "workflow.fail_job(" + job_id + ",'lease-b',"
                + str(second['lease_version']) + ",'regression.cleanup',false)"), "Job cleanup")
    terminal = evidence()
    require(terminal['job']['state'] != 'LEASED' and renew('lease-b', second['lease_version']) is False
            and evidence() == terminal, "Terminal job accepted renewal")
    print("EVIDENCE | taskTimeoutMs=30000 claimLeaseMs=2000 renewal=fenced counters=unchanged ACL=worker-only", flush=True)


def expiry_snapshot(db: PsqlSession) -> dict:
    # The isolated fixture owns this database. Include business, workflow,
    # delivery and idempotency history, not just the leased job row.
    tables = (("payment", "operations"), ("payment", "operation_events"),
              ("payment", "external_requests"), ("payment", "decisions"),
              ("workflow", "process_instances"), ("workflow", "step_instances"),
              ("workflow", "workflow_jobs"), ("workflow", "task_attempts"),
              ("workflow", "workflow_events"), ("delivery", "outbox"),
              ("catalog", "idempotency_records"), ("catalog", "action_dispatches"))
    entries = [sql_text(schema + "." + table) + ","
               + "(SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text COLLATE \"C\"),"
               + "'[]'::jsonb) FROM " + schema + "." + table + " r)"
               for schema, table in tables]
    return db.json("SELECT jsonb_build_object(" + ",".join(entries) + ");", "expiry evidence")


def expiry_job(db: PsqlSession, owner: str) -> tuple[dict, str, str]:
    operation = invoke(db, "payment", "request", {
        "operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"})["operationId"]
    process = invoke(db, "payment", "submit", {"operationId": operation})["processId"]
    run_job(db, operation, process, "validate")
    rows = boundary(db, "workflow_worker", "COALESCE((SELECT jsonb_agg(to_jsonb(j)) "
                    "FROM workflow.claim_jobs(" + sql_text(owner) + ",1,2000) j),'[]'::jsonb)")
    require(len(rows) == 1 and rows[0]["process_id"] == process
            and rows[0]["action"] == "prepare_external", "Wrong expiry fixture job")
    return rows[0], operation, process


def wait_job_expired(db: PsqlSession, job: dict) -> None:
    deadline = time.monotonic() + 10
    query = ("SELECT to_jsonb(lease_until <= clock_timestamp()) FROM workflow.workflow_jobs "
             "WHERE job_id=" + sql_text(job["job_id"]) + "::uuid;")
    while time.monotonic() < deadline:
        if db.json(query, "job lease expiry", timeout=max(0.001, deadline-time.monotonic())) is True:
            return
    raise CheckFailed("Job lease expiry barrier exceeded 10 seconds")


def prepare_and_finish(db: PsqlSession, job: dict, operation: str, process: str,
                       owner: str, rollback: bool = False) -> dict:
    ctx = context()
    ctx.update({"principal": "workflow-worker", "consumer": "workflow-worker",
                "requestId": job["execution_id"], "processId": process,
                "jobId": job["job_id"], "executionId": job["execution_id"],
                "attemptId": job["attempt_id"]})
    expression = ("WITH invoked AS MATERIALIZED (SELECT api.invoke('payment','prepare_external',1,"
                  + sql_json(ctx) + "," + sql_json({"operationId": operation}) + ") AS envelope) "
                  "SELECT workflow.finish_job(" + sql_text(job["job_id"]) + "::uuid,"
                  + sql_text(owner) + "," + str(job["lease_version"])
                  + ",envelope->>'outcome',envelope->'result') FROM invoked "
                  "WHERE envelope->>'status'='ok';")
    if rollback:
        raw = db.execute("BEGIN; SET LOCAL ROLE workflow_worker; " + expression + " ROLLBACK;",
                         "expired action and finish rollback")
        return decode(raw, "expired action and finish rollback")
    return boundary(db, "workflow_worker", "(" + expression.removesuffix(";") + ")")


def expired_job_boundary(db: PsqlSession, failing: bool) -> None:
    owner = "expired-fail" if failing else "expired-finish"
    first, operation, process = expiry_job(db, owner)
    before = expiry_snapshot(db)
    job_id = sql_text(first["job_id"]) + "::uuid"
    process_succeeded = ("SELECT to_jsonb(count(*)) FROM workflow.workflow_events "
                         "WHERE process_id=" + sql_text(process) + "::uuid "
                         "AND event_type='TaskSucceeded';")
    target_events = ("SELECT COALESCE(jsonb_agg(jsonb_build_object("
                     "'stepInstanceId',e.step_instance_id::text,'metadata',e.metadata) "
                     "ORDER BY e.event_id),'[]'::jsonb) FROM workflow.workflow_events e "
                     "WHERE e.process_id=" + sql_text(process) + "::uuid "
                     "AND e.step_instance_id=" + sql_text(first["step_instance_id"]) + "::uuid "
                     "AND e.event_type='TaskSucceeded' "
                     "AND e.metadata->>'jobId'=" + sql_text(first["job_id"]) + " "
                     "AND e.metadata->>'executionId'=" + sql_text(first["execution_id"]) + ";")
    effects = ("SELECT jsonb_build_object('externalRequests',"
               "(SELECT count(*) FROM payment.external_requests x WHERE x.operation_id="
               + sql_text(operation) + "::uuid),'outboxRows',"
               "(SELECT count(*) FROM delivery.outbox d JOIN payment.external_requests x "
               "USING(external_request_id) WHERE x.operation_id="
               + sql_text(operation) + "::uuid));")
    baseline_succeeded = db.json(process_succeeded, "process success baseline")
    require(baseline_succeeded >= 1 and db.json(target_events, "target success baseline") == []
            and db.json(effects, "effect baseline") == {"externalRequests": 0, "outboxRows": 0},
            "Expiry fixture already has the target result or effect")
    wait_job_expired(db, first)
    if failing:
        stale = boundary(db, "workflow_worker", "workflow.fail_job(" + job_id + ","
                         + sql_text(owner) + "," + str(first["lease_version"])
                         + ",'expiry.probe',true)")
    else:
        stale = boundary(db, "workflow_worker", "workflow.finish_job(" + job_id + ","
                         + sql_text(owner) + "," + str(first["lease_version"])
                         + ",'PREPARED','{}'::jsonb)")
    require(stale.get("status") == "error" and stale.get("code") == "workflow.lease_stale"
            and expiry_snapshot(db) == before, "Expired boundary changed business or history evidence")
    require(db.json(process_succeeded, "success after stale boundary") == baseline_succeeded
            and db.json(target_events, "target after stale boundary") == []
            and db.json(effects, "effect after stale boundary") ==
            {"externalRequests": 0, "outboxRows": 0},
            "Expired boundary created a terminal event or domain effect")
    if not failing:
        # Same SQL transaction shape as Worker: action effect is still open
        # when finish returns stale; rollback leaves no external request.
        stale = prepare_and_finish(db, first, operation, process, owner, rollback=True)
        require(stale.get("code") == "workflow.lease_stale" and expiry_snapshot(db) == before,
                "Expired action/finish transaction retained an effect")
        require(db.json(process_succeeded, "success after rollback") == baseline_succeeded
                and db.json(target_events, "target after rollback") == []
                and db.json(effects, "effect after rollback") ==
                {"externalRequests": 0, "outboxRows": 0},
                "Rollback retained a terminal event or domain effect")
    rows = boundary(db, "workflow_worker", "COALESCE((SELECT jsonb_agg(to_jsonb(j)) "
                    "FROM workflow.claim_jobs('reclaimed-owner',1,2000) j),'[]'::jsonb)")
    require(len(rows) == 1 and rows[0]["job_id"] == first["job_id"]
            and rows[0]["process_id"] == first["process_id"]
            and rows[0]["step_instance_id"] == first["step_instance_id"]
            and rows[0]["execution_id"] == first["execution_id"]
            and rows[0]["attempt_id"] != first["attempt_id"]
            and rows[0]["lease_version"] > first["lease_version"], "Reclaim identity/fence differs")
    second = rows[0]
    ok(prepare_and_finish(db, second, operation, process, "reclaimed-owner"),
       "New lease action and finish")
    final = db.json("SELECT jsonb_build_object('jobState',j.state,'failureCount',j.failure_count,"
                    "'attemptCount',j.attempt_count,"
                    "'attempts',(SELECT jsonb_agg(jsonb_build_object('attemptId',a.attempt_id::text,"
                    "'executionId',a.execution_id::text,'leaseVersion',a.lease_version,"
                    "'status',a.status,'finished',a.finished_at IS NOT NULL) ORDER BY a.attempt_number) "
                    "FROM workflow.task_attempts a WHERE a.job_id=j.job_id),"
                    "'processTerminalEvents',(SELECT count(*) FROM workflow.workflow_events e "
                    "WHERE e.process_id=j.process_id AND e.event_type='TaskSucceeded')) "
                    "FROM workflow.workflow_jobs j WHERE j.job_id=" + job_id + ";", "terminal evidence")
    require(final == {"jobState": "SUCCEEDED", "failureCount": 0, "attemptCount": 2,
                      "attempts": [
                          {"attemptId": first["attempt_id"], "executionId": first["execution_id"],
                           "leaseVersion": first["lease_version"], "status": "STALE", "finished": True},
                          {"attemptId": second["attempt_id"], "executionId": second["execution_id"],
                           "leaseVersion": second["lease_version"], "status": "SUCCEEDED", "finished": True}],
                      "processTerminalEvents": baseline_succeeded + 1},
            "Reclaimed job did not finish exactly once")
    events = db.json(target_events, "authoritative target success event")
    require(events == [{"stepInstanceId": second["step_instance_id"], "metadata": {
        "processId": process, "jobId": second["job_id"],
        "executionId": second["execution_id"], "attemptId": second["attempt_id"],
        "leaseVersion": second["lease_version"], "outcome": "PREPARED"}}]
            and db.json(effects, "single prepared effect") ==
            {"externalRequests": 1, "outboxRows": 1},
            "Authoritative attempt event or domain effect is not unique")
    print("EVIDENCE | expired=" + ("fail" if failing else "finish")
          + " stale=workflow.lease_stale baselineTaskSucceeded=" + str(baseline_succeeded)
          + " targetEvents=1 externalRequests=1 outboxRows=1"
          + " leaseVersion=" + str(first["lease_version"]) + "->" + str(second["lease_version"])
          + " executionIdStable=true", flush=True)


def upgrade_026_to_028(directory: Path) -> None:
    directory.mkdir()
    stack = Stack(directory)
    try:
        stack.start()
        db = PsqlSession(stack, "upgrade-observer")
        baseline = metadata(db)
        result = stack.cli("migration", "apply", "/app/db/migrations/legacy")["result"]
        require(result["applied"] == 6 and result["skipped"] == 0, "026 baseline not applied")
        at_026 = metadata(db)
        stalled_query = ("SELECT jsonb_build_object('oid',p.oid,'owner',pg_get_userbyid(p.proowner),"
                         "'acl',p.proacl::text,'definer',p.prosecdef,'language',l.lanname,"
                         "'volatility',p.provolatile,'config',p.proconfig,'body',p.prosrc) "
                         "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
                         "WHERE p.oid='diagnostics.stalled_v1(jsonb,jsonb)'::regprocedure;")
        stalled_at_026 = db.json(stalled_query, "stalled catalog at 026")
        result = stack.cli("migration", "apply", "/app/db/migrations/week4")["result"]
        require(result["applied"] == 2 and result["skipped"] == 6, "026 to 028 upgrade differs")
        upgraded = metadata(db)
        stalled_upgraded = db.json(stalled_query, "stalled catalog after 028")
        require(all({k: a[k] for k in a if k != "body"} ==
                    {k: b[k] for k in b if k != "body"} for a, b in zip(at_026, upgraded)),
                "Upgrade changed function identity/security")
        require({k: stalled_at_026[k] for k in stalled_at_026 if k != "body"} ==
                {k: stalled_upgraded[k] for k in stalled_upgraded if k != "body"},
                "Upgrade changed stalled OID/owner/ACL/security")
        require(len(baseline) == len(upgraded), "Upgrade changed function set")
        result = stack.cli("migration", "apply", "/app/db/migrations/week4")["result"]
        require(result["applied"] == 0 and result["skipped"] == 8,
                "Upgrade repeated apply did not skip eight migrations")
        require(metadata(db) == upgraded and db.json(stalled_query, "repeated stalled catalog") == stalled_upgraded,
                "Repeated upgrade changed function metadata")
    finally:
        stack.cleanup()


def context() -> dict:
    return {"principal": "week4-sql", "consumer": "week4-sql", "correlationId": str(uuid.uuid4()),
            "requestId": uuid.uuid4().hex,
            "scopes": ["payment:write", "payment:read", "payment:internal", "workflow:execute", "receipt:write"]}


def invoke(db: PsqlSession, module: str, action: str, payload: dict) -> dict:
    result = boundary(db, "course_runtime", "api.invoke(" + sql_text(module) + "," + sql_text(action)
                      + ",1," + sql_json(context()) + "," + sql_json(payload) + ")")
    return ok(result, "Action invocation")["result"]


def run_job(db: PsqlSession, operation: str, process: str, action: str) -> None:
    jobs = boundary(db, "workflow_worker", "COALESCE((SELECT jsonb_agg(to_jsonb(j)) "
                    "FROM workflow.claim_jobs('week4-sql-worker',1,2000) j),'[]'::jsonb)")
    require(len(jobs) == 1 and jobs[0]["process_id"] == process and jobs[0]["action"] == action,
            "Production worker boundary claimed an unexpected job")
    job = jobs[0]
    ctx = context()
    ctx.update({"principal": "workflow-worker", "consumer": "workflow-worker",
                "requestId": job["execution_id"], "processId": process, "jobId": job["job_id"],
                "executionId": job["execution_id"], "attemptId": job["attempt_id"]})
    expression = ("(WITH invoked AS MATERIALIZED (SELECT api.invoke('payment'," + sql_text(action)
                  + ",1," + sql_json(ctx) + "," + sql_json({"operationId": operation}) + ") AS envelope) "
                  "SELECT workflow.finish_job(" + sql_text(job["job_id"]) + "::uuid,'week4-sql-worker',"
                  + str(job["lease_version"]) + ",envelope->>'outcome',envelope->'result') FROM invoked "
                  "WHERE envelope->>'status'='ok')")
    ok(boundary(db, "workflow_worker", expression), "Action and finish transaction")


def delivery(db: PsqlSession) -> dict:
    operation = invoke(db, "payment", "request", {"operationKind": "PAYMENT_EXECUTION", "amount": "10.00", "currency": "RUB"})["operationId"]
    process = invoke(db, "payment", "submit", {"operationId": operation})["processId"]
    for action in ("validate", "prepare_external"):
        run_job(db, operation, process, action)
    row = db.json("SELECT jsonb_build_object('operation',x.operation_id,'process',x.process_id,"
                  "'external',x.external_request_id,'outbox',o.outbox_id) FROM payment.external_requests x "
                  "JOIN delivery.outbox o USING(external_request_id) WHERE x.operation_id=" + sql_text(operation)
                  + "::uuid;", "prepared delivery")
    require(isinstance(row, dict) and row["process"] == process, "Missing production delivery")
    return row


def outbox(db: PsqlSession, item: dict) -> dict:
    return db.json("SELECT to_jsonb(o) FROM delivery.outbox o WHERE outbox_id=" + sql_text(item["outbox"])
                   + "::uuid;", "outbox evidence")


def claim(db: PsqlSession, item: dict, owner: str) -> dict:
    rows = boundary(db, "outbox_dispatcher", "COALESCE((SELECT jsonb_agg(to_jsonb(o)) "
                    "FROM delivery.claim_outbox(" + sql_text(owner) + ",1) o),'[]'::jsonb)")
    require(len(rows) == 1 and rows[0]["outbox_id"] == item["outbox"], "Unexpected delivery reservation")
    return rows[0]


def complete(db: PsqlSession, item: dict, owner: str, version: int, code: str) -> dict:
    return boundary(db, "outbox_dispatcher", "delivery.fail_outbox(" + sql_text(item["outbox"])
                    + "::uuid," + sql_text(owner) + "," + str(version) + "," + sql_text(code) + ")")


def wait_due(db: PsqlSession, item: dict, lease: bool = False) -> None:
    # Poll a database fact, never advance table timestamps or use a fixed sleep.
    deadline = time.monotonic() + 10
    field = "lease_until" if lease else "next_attempt_at"
    while time.monotonic() < deadline:
        if db.json("SELECT to_jsonb(" + field + " <= clock_timestamp()) FROM delivery.outbox WHERE outbox_id="
                   + sql_text(item["outbox"]) + "::uuid;", "delivery time barrier",
                   timeout=max(0.001, deadline - time.monotonic())) is True:
            return
    raise CheckFailed("Delivery time barrier deadline exceeded")


def no_claim(db: PsqlSession) -> None:
    rows = boundary(db, "outbox_dispatcher", "COALESCE((SELECT jsonb_agg(to_jsonb(o)) "
                    "FROM delivery.claim_outbox('no-fifth-attempt',100) o),'[]'::jsonb)")
    require(rows == [], "An exhausted/terminal delivery produced another claim")


def assert_dead(db: PsqlSession, item: dict, attempts: int) -> dict:
    row = outbox(db, item)
    require(row["state"] == "DEAD" and row["attempt_count"] == attempts and row["dead_at"] is not None
            and row["lease_owner"] is None and row["lease_until"] is None and bool(row["last_error_code"]),
            "DEAD diagnostic evidence differs")
    state = db.json("SELECT jsonb_build_array(o.status,p.state) FROM payment.operations o JOIN workflow.process_instances p "
                    "ON p.process_id=o.process_id WHERE o.operation_id=" + sql_text(item["operation"]) + "::uuid;", "waiting business state")
    require(state == ["PROCESSING", "WAITING_SIGNAL"], "DEAD changed payment or process terminal state")
    no_claim(db)
    require(outbox(db, item) == row, "Terminal claim changed DEAD evidence")
    return row


def retry_backoff_dead(db: PsqlSession) -> None:
    item = delivery(db)
    identity = None
    for attempt in range(1, 5):
        lease = claim(db, item, "retry-owner")
        current_identity = {k: v for k, v in lease.items() if k != "lease_version"}
        require(identity is None or identity == current_identity, "Retry changed provider identity/body facts")
        identity = current_identity
        require(outbox(db, item)["attempt_count"] == attempt, "Incorrect reservation counter")
        before = outbox(db, item)
        stale = complete(db, item, "wrong-owner", lease["lease_version"], "http.503.retryable")
        require(stale.get("code") == "delivery.lease_stale" and outbox(db, item) == before, "Owner fencing failed")
        result = ok(complete(db, item, "retry-owner", lease["lease_version"], "http.503.retryable"), "Retry failure")
        require(result["outcome"] == ("DEAD" if attempt == 4 else "RETRY_WAIT"), "Incorrect retry state")
        if attempt < 4:
            delay = db.json("SELECT to_jsonb(extract(epoch FROM (next_attempt_at-updated_at))*1000) "
                            "FROM delivery.outbox WHERE outbox_id=" + sql_text(item["outbox"]) + "::uuid;", "backoff interval")
            expected = (200, 400, 800)[attempt - 1]
            require(expected <= float(delay) <= expected + 100 and float(delay).is_integer(),
                    "Backoff/jitter outside expected integer interval")
            wait_due(db, item)
    assert_dead(db, item, 4)


def expired_lease_budget(db: PsqlSession) -> None:
    item = delivery(db)
    first = None
    for attempt in range(1, 5):
        lease = claim(db, item, "lease-owner-" + str(attempt))
        row = outbox(db, item)
        require(row["attempt_count"] == attempt and lease["lease_version"] == attempt, "Reclaim counter/version differs")
        first = first or lease
        if attempt > 1:
            stale = complete(db, item, "lease-owner-1", first["lease_version"], "http.503.retryable")
            require(stale.get("code") == "delivery.lease_stale" and outbox(db, item) == row, "Lease version fencing failed")
        wait_due(db, item, lease=True)
    no_claim(db)  # Retires the expired fourth reservation atomically.
    row = assert_dead(db, item, 4)
    require(row["last_error_code"] == "delivery.lease_expired", "Exhausted lease diagnosis differs")


def save_receipt(db: PsqlSession, item: dict) -> str:
    message = "week4-receipt-" + uuid.uuid4().hex
    payload = {"version": 1, "messageId": message, "externalRequestId": item["external"],
               "providerPaymentId": message, "outcome": "COMPLETED", "occurredAt": "2026-09-04T12:00:00Z"}
    ctx = context()
    ctx["requestId"] = message
    ctx["transport"] = {"signatureVerified": True, "signatureVersion": 1}
    digest = hashlib.sha256(compact(payload).encode("utf-8")).hexdigest()
    # Exercise the same trusted SQL boundary as the API. HMAC verification is
    # outside this SQL test; no policy GUC or domain table is changed here.
    result = db.json("BEGIN; SET LOCAL ROLE course_runtime; SET LOCAL course.raw_body_sha256=" + sql_text(digest)
                     + "; SELECT api.invoke('receipt','accept',1," + sql_json(ctx) + "," + sql_json(payload)
                     + "); COMMIT;", "durable receipt")
    ok(result, "Receipt acceptance")
    return message


def finish_payment(db: PsqlSession, item: dict) -> None:
    for action in ("apply_receipt", "complete"):
        run_job(db, item["operation"], item["process"], action)
    facts = db.json("SELECT jsonb_build_array(o.status,p.state,(SELECT count(*) FROM payment.operation_events e "
                    "WHERE e.operation_id=o.operation_id AND e.event_type='OPERATION_COMPLETED')) "
                    "FROM payment.operations o JOIN workflow.process_instances p ON p.process_id=o.process_id "
                    "WHERE o.operation_id=" + sql_text(item["operation"]) + "::uuid;", "terminal evidence")
    require(facts == ["COMPLETED", "COMPLETED", 1], "Payment completion evidence differs")


def late_receipt(db: PsqlSession) -> None:
    item = delivery(db)
    lease = claim(db, item, "terminal-owner")
    ok(complete(db, item, "terminal-owner", lease["lease_version"], "http.400.terminal"), "Terminal failure")
    before = assert_dead(db, item, 1)
    save_receipt(db, item)
    after = outbox(db, item)
    require(after["state"] == "CONFIRMED" and all(after[k] == before[k] for k in
            ("attempt_count", "last_error_code", "dead_at")), "Late receipt erased DEAD evidence")
    result = complete(db, item, "terminal-owner", lease["lease_version"], "http.503.retryable")
    require(result.get("outcome") == "CONFIRMED" and outbox(db, item) == after, "Failure regressed CONFIRMED")
    result = boundary(db, "outbox_dispatcher", "delivery.succeed_outbox(" + sql_text(item["outbox"])
                      + "::uuid,'terminal-owner'," + str(lease["lease_version"]) + ",'late-provider')")
    require(result.get("outcome") == "CONFIRMED" and outbox(db, item) == after, "Success regressed CONFIRMED")
    require(boundary(db, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 1, "Late receipt not applied")
    finish_payment(db, item)
    require(outbox(db, item) == after, "Payment completion changed preserved DEAD evidence")


def evidence_sql(item: dict, message: str) -> str:
    process = sql_text(item["process"]) + "::uuid"
    msg = sql_text(message)
    return ("SELECT jsonb_build_object('signals',(SELECT jsonb_agg(to_jsonb(s)) FROM workflow.workflow_signals s WHERE message_id=" + msg
            + "),'inbox',(SELECT to_jsonb(i) FROM delivery.inbox i WHERE message_id=" + msg
            + "),'receipt',(SELECT to_jsonb(r) FROM payment.receipts r WHERE message_id=" + msg
            + "),'history',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM workflow.workflow_events e WHERE process_id=" + process
            + "),'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM workflow.workflow_jobs j WHERE process_id=" + process
            + "),'steps',(SELECT jsonb_agg(to_jsonb(s) ORDER BY step_instance_id) FROM workflow.step_instances s WHERE process_id=" + process
            + "),'process',(SELECT to_jsonb(p) FROM workflow.process_instances p WHERE process_id=" + process + "));")


def reconcile_race(stack: Stack, observer: PsqlSession, rollback: bool) -> None:
    item = delivery(observer)
    message = save_receipt(observer, item)
    query = evidence_sql(item, message)
    baseline = observer.json(query, "reconcile baseline")
    require(baseline["inbox"]["state"] == "RECEIVED" and baseline["signals"] is None, "Receipt is not pending")
    t1, t2 = PsqlSession(stack, "T1"), PsqlSession(stack, "T2")
    begin = "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL ROLE inbox_reconciler;"
    t1.execute(begin, "begin")
    require(t1.json("SELECT delivery.reconcile_inbox(100);", "reconcile") == 1, "T1 must apply one receipt")
    staged = t1.json("RESET ROLE; " + query, "staged evidence")
    require(observer.json(query, "uncommitted invisibility") == baseline, "Uncommitted reconciliation leaked")
    t2.execute(begin, "begin")
    t2.send("SELECT delivery.reconcile_inbox(100);", "reconcile")
    wait_until_blocked(observer, t1, t2)
    t1.execute("ROLLBACK;" if rollback else "COMMIT;", "release blocker")
    require(decode(t2.finish(), "T2 count") == (1 if rollback else 0), "T2 counted another transaction's application")
    require(observer.json(query, "visibility before T2 commit") == (baseline if rollback else staged),
            "Reconciliation commit/rollback visibility differs")
    t2.execute("COMMIT;", "commit")
    final = observer.json(query, "durable reconciliation")
    require(len(final["signals"]) == 1 and final["signals"][0]["status"] == "APPLIED"
            and final["inbox"]["state"] == "APPLIED" and final["receipt"]["applied_at"] is not None,
            "Reconciliation durable evidence differs")
    require(len(final["jobs"]) == len(baseline["jobs"]) + 1
            and len(final["steps"]) == len(baseline["steps"]) + 1,
            "Reconciliation created duplicate or missing next-step work")
    for event in ("SignalAccepted", "SignalApplied"):
        require(sum(row["event_type"] == event and row["metadata"].get("messageId") == message
                    for row in final["history"]) == 1, "Duplicate or missing signal event")
    if not rollback:
        require(final == staged, "Losing reconciler changed workflow history/state")
    require(boundary(observer, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 0,
            "Repeated reconciliation counted applied receipt")
    require(observer.json(query, "stable repeat") == final, "Repeated reconciliation changed evidence")
    finish_payment(observer, item)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List scenarios without Docker")
    args = parser.parse_args()
    if args.list:
        print("\n".join(SCENARIOS))
        return 0
    passed, failed = 0, False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix="week4-sql-") as directory:
            stack = Stack(Path(directory))
            try:
                print("PROJECT | " + stack.project, flush=True)
                stack.start()
                db = PsqlSession(stack, "observer")
                baseline = metadata(db)
                require(db.json("SELECT to_jsonb(to_regprocedure('delivery._outbox_policy()') IS NULL);", "baseline") is True,
                        "Fresh init unexpectedly executed forward migrations")
                stack.bootstrap(len(forward_files()), 0)
                actions = (
                    lambda: reader_privileges(stack, db), lambda: migration_repeat(stack, db, baseline),
                    lambda: retry_backoff_dead(db), lambda: expired_lease_budget(db),
                    lambda: late_receipt(db), lambda: reconcile_race(stack, db, False),
                    lambda: reconcile_race(stack, db, True), lambda: job_lease_heartbeat(stack, db),
                    lambda: expired_job_boundary(db, False),
                    lambda: expired_job_boundary(db, True),
                    lambda: upgrade_026_to_028(Path(directory) / "upgrade"),
                )
                for name, action in zip(SCENARIOS, actions):
                    print("RUN | " + name, flush=True)
                    action()
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
