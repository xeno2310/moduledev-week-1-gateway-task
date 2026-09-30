"""Narrow diagnostics.stalled SQL/HTTP regression, run from the repository root.

Uses PostgreSQL, production CLI, API and Gateway only. No Worker/provider is
needed: existing workflow/delivery boundaries prepare all durable fixtures.
No direct domain DML, reference checkout, checker dependency or third-party
Python package. Own project, fresh volume and private images; cleanup in finally.
--list and --help never invoke Docker. Runtime execution is a separate gate.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
from pathlib import Path
import re
import signal
import tempfile
import time
import urllib.error
import urllib.request
import uuid

import week4_sql_regression as sql
from signal_process_concurrency import CheckFailed, PsqlSession, compact, interrupted, require, sql_text
from week3_e2e import make_token, synthetic_environment

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "manifests/diagnostics-stalled-v1.action.json"
MIGRATION = ROOT / "db/migrations/week4/025_diagnostics_stalled.sql"
SCENARIOS = ("registration-acl-repeat", "empty-read-only", "single-exhausted-delivery",
             "multiple-unique-ordered", "exclude-non-dead-and-unrelated",
             "late-receipt-and-terminal-chains", "payload-and-access",
             "restart-preserves-stalled")


class Stack(sql.Stack):
    def __init__(self, directory: Path) -> None:
        super().__init__(directory)
        self.env.update(synthetic_environment())
        self.env["COMPOSE_DISABLE_ENV_FILE"] = "1"
        self.token = make_token(self.env, "stalled-reader", "diagnostics:read")
        self.other_token = make_token(self.env, "payment-reader", "payment:read")
        images = {name: self.project + "-" + name + ":stalled" for name in ("api", "gateway")}
        self.tags += list(images.values())
        override = directory / "http-images.json"
        override.write_text(compact({"services": {name: {"image": tag} for name, tag in images.items()}}),
                            encoding="utf-8")
        self.prefix += ["-f", str(override)]
        self.url = ""

    def start(self) -> None:
        super().start()
        self.bootstrap(len(sql.forward_files()), 0)
        self.compose("build", "api", "gateway", timeout=1200)
        # Bootstrap above has already committed migrations and publication.
        self.compose("up", "-d", "--no-build", "--no-deps", "api", "gateway", timeout=120)
        binding = self.compose("port", "gateway", "8080")
        require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", binding) is not None, "Expected isolated loopback port")
        self.url = "http://" + binding
        self.wait_ready()

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 60
        last_status = 0
        while time.monotonic() < deadline:
            last_status = self.http("GET", "/health/ready")[0]
            if last_status == 200:
                return
            time.sleep(0.1)
        raise CheckFailed(f"Gateway readiness deadline exceeded (last HTTP {last_status})")

    def http(self, method: str, path: str, body: bytes | None = None,
             headers: dict | None = None) -> tuple[int, object, str]:
        request = urllib.request.Request(self.url + path, data=body, headers=headers or {}, method=method)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            try:
                response = opener.open(request, timeout=10)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                raw = response.read(1048577)
                require(len(raw) <= 1048576, "HTTP response too large (body withheld)")
                envelope = sql.decode(raw, "HTTP envelope") if raw else None
                return response.code, envelope, response.headers.get("Content-Type", "")
        except (OSError, urllib.error.URLError):
            return 0, None, ""

    def stalled(self, payload: object, token: str | None) -> tuple[int, object, str]:
        headers = {"Content-Type": "application/json", "X-Action-Version": "1"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        # Intentionally no Idempotency-Key: mode/scope must be none/none.
        return self.http("POST", "/api/diagnostics/stalled", compact(payload).encode(), headers)

    def receipt(self, item: dict, outcome: str) -> None:
        message = "stalled-receipt-" + uuid.uuid4().hex
        payload = {"version": 1, "messageId": message, "externalRequestId": item["external"],
                   "providerPaymentId": message, "outcome": outcome, "occurredAt": "2026-09-30T00:00:00Z"}
        body = compact(payload).encode()
        signature = hmac.new(self.env["PROVIDER_HMAC_SECRET"].encode(), body, hashlib.sha256).hexdigest()
        status, envelope, _ = self.http("POST", "/api/receipt/accept", body, {
            "Authorization": "Bearer " + self.env["PROVIDER_CALLBACK_TOKEN"], "Content-Type": "application/json",
            "X-Action-Version": "1", "Idempotency-Key": message, "X-Provider-Signature": "v1=" + signature})
        require(status == 200, "Valid late receipt HTTP failed (body withheld)")
        sql.ok(envelope, "Signed receipt")


def snapshot(db: PsqlSession) -> dict:
    # Every domain/history row, including task/flow definitions. api.invoke's
    # ordinary action_dispatches audit is allowed and intentionally excluded.
    tables = db.json("SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname) ORDER BY n.nspname,c.relname) "
                     "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                     "WHERE n.nspname IN ('payment','workflow','delivery') AND c.relkind='r';", "domain tables")
    tables += [["catalog", "idempotency_records"]]
    fields = []
    for schema, table in tables:
        require(re.fullmatch(r"[a-z_][a-z0-9_]*", schema + table) is not None, "Unexpected table identifier")
        fields.append(sql_text(schema + "." + table) + ", (SELECT COALESCE(jsonb_agg(to_jsonb(r) "
                      "ORDER BY to_jsonb(r)::text COLLATE \"C\"),'[]'::jsonb) FROM \""
                      + schema + "\".\"" + table + "\" r)")
    return db.json("SELECT jsonb_build_object(" + ",".join(fields) + ");", "business/history snapshot")


def item_shape(result: object) -> list[dict]:
    # All constraints of the exact published result schema; semantic sorting
    # and unique operationId are stricter than JSON Schema uniqueItems alone.
    require(isinstance(result, dict) and set(result) == {"items"}, "Result has unexpected fields")
    items = result["items"]
    require(isinstance(items, list), "items must be an array")
    ids = []
    for item in items:
        require(isinstance(item, dict) and set(item) == {"operationId", "processId", "externalRequestId"},
                "Item has unexpected fields")
        for field in ("operationId", "processId"):
            value = item[field]
            require(isinstance(value, str), "Identifier is not a string")
            try:
                canonical = str(uuid.UUID(value))
            except ValueError:
                raise CheckFailed("Identifier is not a UUID") from None
            require(value == canonical, "UUID is not canonical")
        external = item["externalRequestId"]
        require(isinstance(external, str) and 1 <= len(external) <= 200
                and "\r" not in external and "\n" not in external, "Invalid externalRequestId")
        ids.append(item["operationId"])
    require(ids == sorted(set(ids)), "Items are not ordered or contain duplicate operations")
    return items


def read_stalled(stack: Stack, db: PsqlSession, expected: list[dict]) -> None:
    before = snapshot(db)
    for _ in range(2):
        status, envelope, content_type = stack.stalled({}, stack.token)
        require(status == 200 and content_type.split(";", 1)[0] == "application/json", "Stalled HTTP contract differs")
        require(isinstance(envelope, dict) and set(envelope) == {"status", "outcome", "result", "meta"}
                and envelope["status"] == "ok" and envelope["outcome"] == "FOUND", "Stalled envelope differs")
        meta = envelope["meta"]
        require(isinstance(meta, dict) and isinstance(meta.get("correlationId"), str)
                and bool(meta["correlationId"]) and meta.get("actionVersion") == 1, "Missing correlation metadata")
        require(item_shape(envelope["result"]) == sorted(expected, key=lambda row: row["operationId"]),
                "Stalled selection differs from production fixture identities")
    require(snapshot(db) == before, "Diagnostics changed business/history/idempotency evidence")


def expected_item(item: dict) -> dict:
    return {"operationId": item["operation"], "processId": item["process"], "externalRequestId": item["external"]}


def exhaust(db: PsqlSession) -> dict:
    item = sql.delivery(db)
    for attempt in range(1, 5):
        lease = sql.claim(db, item, "stalled-fixture")
        result = sql.ok(sql.complete(db, item, "stalled-fixture", lease["lease_version"], "http.503.retryable"), "Delivery retry")
        require(result["outcome"] == ("DEAD" if attempt == 4 else "RETRY_WAIT"), "Incorrect delivery retry state")
        if attempt < 4:
            sql.wait_due(db, item)
    sql.assert_dead(db, item, 4)
    return item


def registration(stack: Stack, db: PsqlSession) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    registered = db.json("SELECT to_jsonb(a) FROM catalog.action_definitions a "
                         "WHERE module='diagnostics' AND action='stalled' AND version=1;", "action registration")
    require(isinstance(registered, dict) and all(registered.get(k) == v for k, v in manifest.items()),
            "Published manifest/schema differs")
    facts = db.json("SELECT jsonb_build_object('oid',p.oid,'owner',pg_get_userbyid(p.proowner),"
                    "'definer',p.prosecdef,'config',p.proconfig,'volatility',p.provolatile,'language',l.lanname,"
                    "'body',p.prosrc,'acl',p.proacl::text,'grantees',(SELECT jsonb_agg(pg_get_userbyid(a.grantee)) "
                    "FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE privilege_type='EXECUTE'),"
                    "'directCallers',(SELECT jsonb_agg(r) FROM unnest(ARRAY['course_runtime','course_publication',"
                    "'workflow_worker','outbox_dispatcher','inbox_reconciler','autocheck_reader']) r "
                    "WHERE has_function_privilege(r,p.oid,'EXECUTE'))) FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang "
                    "WHERE p.oid='diagnostics.stalled_v1(jsonb,jsonb)'::regprocedure;", "target privileges")
    require({k: facts[k] for k in ('owner', 'definer', 'config', 'volatility', 'language')} == {
        'owner': 'course_owner', 'definer': True, 'config': ['search_path=pg_catalog'],
        'volatility': 's', 'language': 'sql'}, "Target security metadata differs")
    require(facts['grantees'] == ['course_owner'] and facts['directCallers'] is None,
            "Target can bypass the generic policy boundary")
    require(facts['body'].replace("\r\n", "\n").strip()
            == MIGRATION.read_text(encoding="utf-8").split('AS $$', 1)[1].split('$$;', 1)[0].strip(),
            "Installed target body differs")
    # Reuse the production reader whitelist assertions, including no direct
    # domain-table/function grants, rather than granting a diagnostics DB role.
    sql.reader_privileges(stack, db)
    before = snapshot(db)
    stack.bootstrap(0, len(sql.forward_files()))
    migration = stack.cli("migration", "apply", "/app/db/migrations/week4")["result"]
    require(migration == {"resource": "migration", "operation": "applied", "applied": 0,
                          "skipped": len(sql.forward_files())}, "Repeat apply was not skipped")
    history = db.json("SELECT jsonb_agg(jsonb_build_object('file_name',file_name,'checksum',checksum) "
                      "ORDER BY file_name) FROM public.course_schema_migrations;", "migration history")
    require(history == [{"file_name": p.name, "checksum": hashlib.sha256(p.read_bytes()).hexdigest()}
                        for p in sql.forward_files()], "Forward filenames/checksums differ")
    require(snapshot(db) == before, "Repeat bootstrap/apply changed domain evidence")
    require(db.json("SELECT to_jsonb(a) FROM catalog.action_definitions a WHERE module='diagnostics' "
                    "AND action='stalled' AND version=1;", "repeat registration") == registered,
            "Repeated bootstrap changed the immutable action")
    require(db.json("SELECT to_jsonb(oid=" + str(facts['oid']) + " AND proacl::text=" + sql_text(facts['acl'])
                    + ") FROM pg_proc WHERE oid='diagnostics.stalled_v1(jsonb,jsonb)'::regprocedure;", "repeat OID/ACL") is True,
            "Repeat apply changed target OID/ACL")


def non_dead(stack: Stack, db: PsqlSession, expected: list[dict]) -> None:
    item = sql.delivery(db)
    require(sql.outbox(db, item)['state'] == 'PENDING', "Missing PENDING fixture")
    read_stalled(stack, db, expected)
    lease = sql.claim(db, item, "non-dead")
    require(sql.outbox(db, item)['state'] == 'LEASED', "Missing LEASED fixture")
    read_stalled(stack, db, expected)
    sql.ok(sql.complete(db, item, "non-dead", lease['lease_version'], "http.503.retryable"), "Nonterminal retry")
    require(sql.outbox(db, item)['state'] == 'RETRY_WAIT', "Missing RETRY_WAIT fixture")
    read_stalled(stack, db, expected)
    sql.wait_due(db, item)
    lease = sql.claim(db, item, "non-dead")
    sql.ok(sql.boundary(db, "outbox_dispatcher", "delivery.succeed_outbox(" + sql_text(item['outbox'])
                       + "::uuid,'non-dead'," + str(lease['lease_version']) + ",'stalled-delivered')"), "Delivery success")
    require(sql.outbox(db, item)['state'] == 'DELIVERED', "Missing DELIVERED fixture")
    read_stalled(stack, db, expected)
    stack.receipt(item, "COMPLETED")
    require(sql.outbox(db, item)['state'] == 'CONFIRMED', "Missing CONFIRMED fixture")
    read_stalled(stack, db, expected)
    require(sql.boundary(db, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 1, "Receipt not reconciled")
    sql.finish_payment(db, item)
    stack.publish_flow()
    stack.start_process()  # An unrelated WAITING_SIGNAL process has no payment/outbox.
    read_stalled(stack, db, expected)


def late_receipts(stack: Stack, db: PsqlSession, stalled: list[dict]) -> None:
    for outcome in ("COMPLETED", "REJECTED"):
        item = stalled.pop(0)
        old = sql.outbox(db, item)
        stack.receipt(item, outcome)
        confirmed = sql.outbox(db, item)
        require(confirmed['state'] == 'CONFIRMED' and all(confirmed[k] == old[k] for k in
                ('attempt_count', 'last_error_code', 'dead_at')), "Late receipt erased delivery evidence")
        read_stalled(stack, db, [expected_item(i) for i in stalled])
        require(sql.boundary(db, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 1, "Late receipt not reconciled")
        for action in ("apply_receipt", "complete" if outcome == "COMPLETED" else "reject"):
            sql.run_job(db, item['operation'], item['process'], action)
        states = db.json("SELECT jsonb_build_array(o.status,p.state) FROM payment.operations o JOIN "
                         "workflow.process_instances p ON p.process_id=o.process_id WHERE o.operation_id="
                         + sql_text(item['operation']) + "::uuid;", "terminal payment/process")
        require(states == [outcome, "COMPLETED"], "Terminal chain differs")
        read_stalled(stack, db, [expected_item(i) for i in stalled])
    # Independently exclude DEAD + PROCESSING when the linked process failed.
    item = stalled.pop(0)
    stack.cli("flow", "signal", item['process'], "--type", "payment.receipt", "--message-id", uuid.uuid4().hex,
              "--payload", "/dev/stdin", payload={"externalRequestId": item['external']})
    jobs = sql.boundary(db, "workflow_worker", "(SELECT jsonb_agg(to_jsonb(j)) FROM workflow.claim_jobs('stalled-failure',1,2000) j)")
    require(len(jobs) == 1 and jobs[0]['process_id'] == item['process'], "Unexpected failure fixture job")
    sql.ok(sql.boundary(db, "workflow_worker", "workflow.fail_job(" + sql_text(jobs[0]['job_id'])
                        + "::uuid,'stalled-failure'," + str(jobs[0]['lease_version'])
                        + ",'stalled.fixture_failure',false)"), "Terminal workflow failure")
    require(db.json("SELECT jsonb_build_array(o.status,p.state,d.state) FROM payment.operations o "
                    "JOIN workflow.process_instances p ON p.process_id=o.process_id "
                    "JOIN payment.external_requests x ON x.operation_id=o.operation_id "
                    "JOIN delivery.outbox d USING(external_request_id) WHERE o.operation_id="
                    + sql_text(item['operation']) + "::uuid;", "failed chain") == ['PROCESSING', 'FAILED', 'DEAD'],
            "Failure fixture does not isolate the workflow predicate")
    read_stalled(stack, db, [])


def invalid_calls(stack: Stack, db: PsqlSession) -> None:
    before = snapshot(db)
    for payload, token, status, code in (({}, None, 401, None), ({}, "invalid-token", 401, None),
                                       ({}, stack.other_token, 403, "access.denied"),
                                       ({"unexpected": True}, stack.token, 422, "payload.invalid")):
        actual, envelope, _ = stack.stalled(payload, token)
        require(actual == status and isinstance(envelope, dict) and envelope.get('status') == 'error',
                "Access/payload HTTP status differs (body withheld)")
        require(code is None or envelope.get('code') == code, "Access/payload error code differs")
    require(snapshot(db) == before, "Denied diagnostics calls changed domain/history evidence")


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
        with tempfile.TemporaryDirectory(prefix="week4-stalled-") as directory:
            stack = Stack(Path(directory))
            try:
                print("PROJECT | " + stack.project, flush=True)
                stack.start()
                db = PsqlSession(stack, "stalled-observer")
                stalled = []
                for name in SCENARIOS:
                    print("RUN | " + name, flush=True)
                    if name == "registration-acl-repeat":
                        registration(stack, db)
                    elif name == "empty-read-only":
                        read_stalled(stack, db, [])
                    elif name == "single-exhausted-delivery":
                        stalled.append(exhaust(db))
                        read_stalled(stack, db, [expected_item(i) for i in stalled])
                    elif name == "multiple-unique-ordered":
                        stalled.extend(exhaust(db) for _ in range(2))
                        read_stalled(stack, db, [expected_item(i) for i in stalled])
                    elif name == "exclude-non-dead-and-unrelated":
                        non_dead(stack, db, [expected_item(i) for i in stalled])
                    elif name == "late-receipt-and-terminal-chains":
                        late_receipts(stack, db, stalled)
                    elif name == "payload-and-access":
                        invalid_calls(stack, db)
                    else:
                        item = exhaust(db)
                        expected = [expected_item(item)]
                        read_stalled(stack, db, expected)
                        before = snapshot(db)
                        history = db.json("SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) "
                                          "FROM public.course_schema_migrations m;", "migration history")
                        stack.compose("stop", "gateway", "api", "postgres", timeout=120)
                        stack.compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120",
                                      "postgres", timeout=150)
                        stack.compose("up", "-d", "--no-build", "--no-deps", "api", "gateway", timeout=120)
                        binding = stack.compose("port", "gateway", "8080")
                        require(re.fullmatch(r"127\.0\.0\.1:[0-9]+", binding) is not None,
                                "Restarted gateway has no isolated loopback port")
                        print("RESTART | gateway port changed=" + str(stack.url != "http://" + binding), flush=True)
                        stack.url = "http://" + binding
                        stack.wait_ready()
                        restarted = PsqlSession(stack, "stalled-restarted-observer")
                        require(snapshot(restarted) == before
                                and restarted.json("SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) "
                                                   "FROM public.course_schema_migrations m;", "migration history")
                                == history, "Restart changed durable diagnostics evidence")
                        read_stalled(stack, restarted, expected)
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
                        sql.report_failure(error)
                finally:
                    signal.signal(signal.SIGINT, previous_int)
                    signal.signal(signal.SIGTERM, interrupted)
    except KeyboardInterrupt:
        failed = True
        print("FAIL | interrupted", flush=True)
    except Exception as error:
        failed = True
        sql.report_failure(error)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    result = "PASSED" if passed == len(SCENARIOS) and not failed else "FAILED"
    print(f"SUMMARY | passed={passed} total={len(SCENARIOS)} result={result}", flush=True)
    return 0 if result == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
