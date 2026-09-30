"""Isolated diagnostics.trace SQL/HTTP regression. --list never invokes Docker.

Runtime uses production CLI, API and SQL boundaries in a private Compose project.
All containers, images and volumes owned by that project are removed in finally.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import signal
import tempfile
import time
import uuid

import week4_sql_regression as sql
from week4_stalled import Stack as BaseStack, snapshot
from signal_process_concurrency import CheckFailed, PsqlSession, compact, interrupted, require, sql_text
from week3_e2e import make_token

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "manifests/diagnostics-trace-v1.action.json"
SCHEMA = ROOT / "contracts/course-1/diagnostics-trace.result.schema.json"
SCENARIOS = (
    "registration-acl-repeat", "provider-and-manual-evidence", "eleven-identifiers-and-matched-by",
    "exact-schema-ordering-privacy-read-only", "null-projections-and-ambiguity", "payload-and-access",
    "restart-preserves-trace",
)


class Stack(BaseStack):
    def __init__(self, directory: Path) -> None:
        super().__init__(directory)
        self.manual_token = make_token(self.env, "reviewer", "workflow:manual")

    def trace(self, payload: object, token: str | None = None) -> tuple[int, object, str]:
        headers = {"Content-Type": "application/json", "X-Action-Version": "1"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        return self.http("POST", "/api/diagnostics/trace", compact(payload).encode(), headers)

    def manual(self, process: str, step: str, reason: str) -> None:
        payload = {"processId": process, "stepInstanceId": step,
                   "decision": "APPROVED", "reason": reason}
        status, envelope, _ = self.http("POST", "/api/workflow/manual", compact(payload).encode(), {
            "Content-Type": "application/json", "X-Action-Version": "1",
            "Authorization": "Bearer " + self.manual_token, "Idempotency-Key": uuid.uuid4().hex})
        require(status == 200, "Manual decision HTTP failed (body withheld)")
        sql.ok(envelope, "Manual decision")

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.http("GET", "/health/ready")[0] == 200:
                return
            time.sleep(0.1)
        raise CheckFailed("Gateway readiness deadline exceeded after restart")


def validate(value: object, schema: dict, root: dict) -> None:
    """Validate the official schema features used by this exact result contract."""
    if "$ref" in schema:
        require(schema["$ref"].startswith("#/$defs/"), "Unexpected schema reference")
        validate(value, root["$defs"][schema["$ref"].split("/")[-1]], root)
        return
    if "oneOf" in schema:
        passed = 0
        for option in schema["oneOf"]:
            try:
                validate(value, option, root)
                passed += 1
            except CheckFailed:
                pass
        require(passed == 1, "Result oneOf differs")
        return
    if "const" in schema:
        require(value == schema["const"], "Result const differs")
    if "enum" in schema:
        require(value in schema["enum"], "Result enum differs")
    kinds = schema.get("type")
    if kinds is not None:
        kinds = kinds if isinstance(kinds, list) else [kinds]
        valid = (("null" in kinds and value is None)
                 or ("object" in kinds and isinstance(value, dict))
                 or ("array" in kinds and isinstance(value, list))
                 or ("string" in kinds and isinstance(value, str))
                 or ("integer" in kinds and type(value) is int))
        require(valid, "Result type differs")
    if isinstance(value, dict):
        require(set(schema.get("required", [])) <= set(value), "Required result field is absent")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            require(set(value) <= set(properties), "Result contains additional fields")
        for key, item in value.items():
            if key in properties:
                validate(item, properties[key], root)
    if isinstance(value, list):
        require(len(value) >= schema.get("minItems", 0), "Result array is too short")
        if schema.get("uniqueItems"):
            require(len({compact(item) for item in value}) == len(value), "Result array is not unique")
        for item in value:
            validate(item, schema.get("items", {}), root)
    if isinstance(value, str):
        require(len(value) >= schema.get("minLength", 0)
                and len(value) <= schema.get("maxLength", 10**9), "Result string length differs")
        if "pattern" in schema:
            require(re.search(schema["pattern"], value) is not None, "Result pattern differs")
        if schema.get("format") == "uuid":
            try:
                require(str(uuid.UUID(value)) == value, "Result UUID is not canonical")
            except ValueError:
                raise CheckFailed("Result UUID is invalid") from None
        if schema.get("format") == "date-time":
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise CheckFailed("Result timestamp is invalid") from None
            require(value.endswith("Z") and parsed.tzinfo == timezone.utc,
                    "Result timestamp is not UTC")
    if type(value) is int and "minimum" in schema:
        require(value >= schema["minimum"], "Result integer is too small")


def trace_ok(stack: Stack, identifier: str, schema: dict) -> dict:
    status, envelope, content_type = stack.trace({"identifier": identifier}, stack.token)
    require(status == 200 and content_type.split(";", 1)[0] == "application/json",
            "Trace HTTP response differs: status=" + str(status) + " code="
            + str(envelope.get("code") if isinstance(envelope, dict) else None))
    require(isinstance(envelope, dict) and set(envelope) == {"status", "outcome", "result", "meta"}
            and envelope["status"] == "ok" and envelope["outcome"] == "FOUND",
            "Trace success envelope differs")
    require(isinstance(envelope["meta"], dict)
            and isinstance(envelope["meta"].get("correlationId"), str)
            and envelope["meta"].get("actionVersion") == 1, "Trace metadata differs")
    result = envelope["result"]
    validate(result, schema, schema)
    require(result["query"]["identifier"] == identifier, "Trace echoed another identifier")
    return result


def canonical(result: dict) -> dict:
    return {key: value for key, value in result.items() if key != "query"}


def provider(stack: Stack, db: PsqlSession) -> dict:
    item = sql.delivery(db)
    stack.receipt(item, "COMPLETED")
    require(sql.boundary(db, "inbox_reconciler", "delivery.reconcile_inbox(100)") == 1,
            "Provider receipt was not reconciled")
    sql.finish_payment(db, item)
    return item


def manual(stack: Stack, db: PsqlSession) -> dict:
    operation = sql.invoke(db, "payment", "request", {
        "operationKind": "PAYMENT_APPROVAL", "amount": "100001.00", "currency": "RUB"})["operationId"]
    process = sql.invoke(db, "payment", "submit", {"operationId": operation})["processId"]
    for action in ("validate", "check_limit"):
        sql.run_job(db, operation, process, action)
    step = db.json("SELECT to_jsonb(step_instance_id::text) FROM workflow.step_instances "
                   "WHERE process_id=" + sql_text(process) + "::uuid AND step_type='MANUAL' "
                   "AND state='WAITING';", "waiting manual step")
    require(isinstance(step, str), "Manual review step is missing")
    stack.manual(process, step, "private-manual-reason-" + uuid.uuid4().hex)
    sql.run_job(db, operation, process, "approve")
    return {"operation": operation, "process": process}


def registration(stack: Stack, db: PsqlSession) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    published = db.json("SELECT to_jsonb(a) FROM catalog.action_definitions a WHERE module='diagnostics' "
                        "AND action='trace' AND version=1;", "trace registration")
    require(isinstance(published, dict)
            and all(published.get(k) == v for k, v in manifest.items()), "Trace publication differs")
    rows = db.json("SELECT jsonb_agg(jsonb_build_object('name',p.proname,'owner',"
                   "pg_get_userbyid(p.proowner),'definer',p.prosecdef,'config',p.proconfig,"
                   "'public',EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,"
                   "acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE'),"
                   "'runtime',has_function_privilege('course_runtime',p.oid,'EXECUTE'),"
                   "'reader',has_function_privilege('autocheck_reader',p.oid,'EXECUTE')) "
                   "ORDER BY p.proname) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                   "WHERE n.nspname='diagnostics' AND p.proname IN ('_utc','_trace_matches','trace_v1');",
                   "trace function ACL")
    require(len(rows) == 3 and all(row['owner'] == 'course_owner' and not row['public']
            and not row['runtime'] and not row['reader'] for row in rows),
            "Trace function owner or EXECUTE ACL differs")
    require(all(row['config'] == ['search_path=pg_catalog'] for row in rows)
            and all(row['definer'] for row in rows if row['name'] != '_utc'),
            "Trace SECURITY DEFINER/search_path differs")
    view = db.json("SELECT jsonb_build_object('owner',pg_get_userbyid(c.relowner),"
                   "'reader',has_table_privilege('autocheck_reader',c.oid,'SELECT'),"
                   "'runtime',has_table_privilege('course_runtime',c.oid,'SELECT')) "
                   "FROM pg_class c WHERE c.oid='diagnostics._trace_dispatch_links'::regclass;",
                   "trace helper view ACL")
    require(view == {'owner': 'course_owner', 'reader': False, 'runtime': False},
            "Trace view has direct reader/runtime access")
    sql.reader_privileges(stack, db)
    before = snapshot(db)
    stack.bootstrap(0, len(sql.forward_files()))
    applied = stack.cli("migration", "apply", "/app/db/migrations/week4")["result"]
    require(applied == {'resource': 'migration', 'operation': 'applied', 'applied': 0,
                        'skipped': len(sql.forward_files())}, "Repeat trace migration was not skipped")
    history = db.json("SELECT jsonb_agg(jsonb_build_object('file_name',file_name,'checksum',checksum) "
                      "ORDER BY file_name) FROM public.course_schema_migrations;", "trace migration history")
    require(history == [{'file_name': p.name, 'checksum': hashlib.sha256(p.read_bytes()).hexdigest()}
                        for p in sql.forward_files()], "Forward migration checksums differ")
    require(snapshot(db) == before, "Repeat bootstrap modified business/history evidence")


def identifier_cases(provider_trace: dict, manual_trace: dict) -> tuple[dict, dict]:
    require(provider_trace['dispatches'] and provider_trace['steps'] and provider_trace['jobs']
            and provider_trace['attempts'] and provider_trace['outbox'] and provider_trace['inbox']
            and provider_trace['receipts'] and manual_trace['decisions'],
            "Production fixtures lack required trace evidence")
    provider_ids = {
        'correlationId': provider_trace['dispatches'][0]['correlationId'],
        'requestId': provider_trace['operation']['requestId'],
        'operationId': provider_trace['operation']['operationId'],
        'processId': provider_trace['process']['processId'],
        'stepInstanceId': provider_trace['steps'][0]['stepInstanceId'],
        'jobId': provider_trace['jobs'][0]['jobId'],
        'executionId': provider_trace['jobs'][0]['executionId'],
        'attemptId': provider_trace['attempts'][0]['attemptId'],
        'externalRequestId': provider_trace['outbox'][0]['externalRequestId'],
        'messageId': provider_trace['inbox'][0]['messageId'],
    }
    manual_ids = {'decisionId': manual_trace['decisions'][0]['decisionId']}
    return provider_ids, manual_ids


def ordering(result: dict, db: PsqlSession) -> None:
    fields = {'dispatches': ('occurredAt', 'correlationId'),
              'operationEvents': ('occurredAt', 'eventId'), 'steps': ('enteredAt', 'stepInstanceId'),
              'attempts': ('startedAt', 'attemptId'), 'outbox': ('createdAt', 'outboxId'),
              'inbox': ('receivedAt', 'messageId'), 'receipts': ('receivedAt', 'messageId'),
              'decisions': ('createdAt', 'decisionId')}
    for name, (moment, identity) in fields.items():
        rows = result[name]
        require(rows == sorted(rows, key=lambda row: (row[moment], row[identity])),
                name + " ordering differs")
    process = result['process']
    if process is not None:
        expected_jobs = db.json("SELECT COALESCE(jsonb_agg(j.job_id::text ORDER BY j.created_at,"
                                "j.job_id::text COLLATE \"C\"),'[]'::jsonb) FROM workflow.workflow_jobs j "
                                "WHERE j.process_id=" + sql_text(process['processId']) + "::uuid;", "job ordering")
        require([job['jobId'] for job in result['jobs']] == expected_jobs, "Jobs are not ordered")


def privacy(result: dict, stack: Stack) -> None:
    forbidden_keys = {'payload', 'body', 'headers', 'jwt', 'token', 'hmac', 'signature',
                      'secret', 'exception', 'stacktrace', 'reason', 'reasonhash', 'metadata'}
    secrets = [value for name, value in stack.env.items()
               if len(value) >= 8 and any(word in name.upper()
                                          for word in ('PASSWORD', 'TOKEN', 'SECRET', 'CAPABILITY'))]
    secrets.append(stack.manual_token)

    def visit(value: object) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                require(key == 'signatureValid'
                        or not any(word in key.lower() for word in forbidden_keys),
                        "Trace contains a forbidden key")
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, str):
            require(not any(secret in value for secret in secrets)
                    and 'private-manual-reason-' not in value, "Trace exposes a private value")

    visit(result)


def ambiguity(db: PsqlSession, stack: Stack, schema: dict) -> None:
    shared = "trace-shared-" + uuid.uuid4().hex
    for index, principal in enumerate(('trace-a', 'trace-b')):
        ctx = sql.context()
        ctx.update({'principal': principal, 'requestId': shared})
        result = sql.boundary(db, 'course_runtime', "api.invoke('payment','request',1,"
                              + sql.sql_json(ctx) + "," + sql.sql_json({
                                  'operationKind': 'PAYMENT_EXECUTION', 'amount': '10.00',
                                  'currency': 'RUB'}) + ")")
        operation = sql.ok(result, "Ambiguity fixture")['result']['operationId']
        if index == 0:
            partial = trace_ok(stack, operation, schema)
            require(partial['operation'] is not None and partial['process'] is None
                    and partial['steps'] == [] and partial['jobs'] == [],
                    "Unsubmitted operation null projection differs")
    stack.publish_flow()
    independent = stack.start_process()
    process_only = trace_ok(stack, independent, schema)
    require(process_only['operation'] is None and process_only['process']['processId'] == independent,
            "Independent workflow null projection differs")
    require(db.json("SELECT count(*) FROM payment.operations WHERE request_id="
                    + sql_text(shared) + ";", "ambiguity fixture count") == 2,
            "Two canonical chains were not created")
    status, envelope, _ = stack.trace({'identifier': shared}, stack.token)
    require(status == 409 and isinstance(envelope, dict)
            and envelope.get('code') == 'diagnostics.trace_ambiguous'
            and envelope.get('status') == 'error' and envelope.get('retryable') is False
            and envelope.get('details') == {} and set(envelope) == {
                'status', 'code', 'message', 'retryable', 'details', 'meta'},
            "Ambiguity did not produce controlled HTTP 409")


def invalid_calls(stack: Stack, db: PsqlSession) -> None:
    before = snapshot(db)
    cases = (({'identifier': 'unknown-' + uuid.uuid4().hex}, stack.token, 404,
              'diagnostics.trace_not_found'),
             ({'identifier': 'x'}, None, 401, None),
             ({'identifier': 'x'}, 'invalid-token', 401, None),
             ({'identifier': 'x'}, stack.other_token, 403, 'access.denied'),
             ({}, stack.token, 422, 'payload.invalid'),
             ({'identifier': 'x', 'extra': True}, stack.token, 422, 'payload.invalid'),
             ({'identifier': ''}, stack.token, 422, 'payload.invalid'),
             ({'identifier': 'x\n'}, stack.token, 422, 'payload.invalid'),
             ({'identifier': 'x' * 201}, stack.token, 422, 'payload.invalid'))
    for payload, token, expected_status, code in cases:
        status, envelope, _ = stack.trace(payload, token)
        require(status == expected_status and isinstance(envelope, dict)
                and envelope.get('status') == 'error'
                and envelope.get('retryable') is False and envelope.get('details') == {}
                and set(envelope) == {'status', 'code', 'message', 'retryable', 'details', 'meta'}
                and (code is None or envelope.get('code') == code),
                "Trace access/payload/error mapping differs")
    require(snapshot(db) == before, "Invalid trace calls modified business/history evidence")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true', help='List scenarios without Docker')
    args = parser.parse_args()
    if args.list:
        print('\n'.join(SCENARIOS))
        return 0
    passed, failed = 0, False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix='week4-trace-') as directory:
            stack = Stack(Path(directory))
            try:
                print('PROJECT | ' + stack.project, flush=True)
                stack.start()
                db = PsqlSession(stack, 'trace-observer')
                schema = json.loads(SCHEMA.read_text(encoding='utf-8'))
                for name in SCENARIOS:
                    print('RUN | ' + name, flush=True)
                    if name == 'registration-acl-repeat':
                        registration(stack, db)
                    elif name == 'provider-and-manual-evidence':
                        provider_item = provider(stack, db)
                        manual_item = manual(stack, db)
                        provider_trace = trace_ok(stack, provider_item['operation'], schema)
                        manual_trace = trace_ok(stack, manual_item['operation'], schema)
                        require(provider_trace['operation']['operationId'] == provider_item['operation']
                                and provider_trace['process']['processId'] == provider_item['process']
                                and manual_trace['operation']['operationId'] == manual_item['operation']
                                and manual_trace['process']['processId'] == manual_item['process']
                                and not manual_trace['outbox'] and not manual_trace['receipts'],
                                'Provider/manual evidence crossed chains')
                    elif name == 'eleven-identifiers-and-matched-by':
                        provider_ids, manual_ids = identifier_cases(provider_trace, manual_trace)
                        for kind, value in provider_ids.items():
                            result = trace_ok(stack, value, schema)
                            require(kind in result['query']['matchedBy']
                                    and canonical(result) == canonical(provider_trace),
                                    kind + ' did not resolve the provider chain')
                        for kind, value in manual_ids.items():
                            result = trace_ok(stack, value, schema)
                            require(kind in result['query']['matchedBy']
                                    and canonical(result) == canonical(manual_trace),
                                    kind + ' did not resolve the manual chain')
                        overlap = trace_ok(stack, provider_ids['executionId'], schema)
                        require(overlap['query']['matchedBy'] == ['requestId', 'executionId'],
                                'Same-chain identifier types were not combined')
                    elif name == 'exact-schema-ordering-privacy-read-only':
                        before = snapshot(db)
                        for result in (trace_ok(stack, provider_item['external'], schema),
                                       trace_ok(stack, manual_item['operation'], schema)):
                            ordering(result, db)
                            privacy(result, stack)
                        require(snapshot(db) == before, 'Trace changed business/history evidence')
                    elif name == 'null-projections-and-ambiguity':
                        ambiguity(db, stack, schema)
                    elif name == 'payload-and-access':
                        invalid_calls(stack, db)
                    else:
                        before = snapshot(db)
                        stable = trace_ok(stack, provider_item['operation'], schema)
                        history = db.json("SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) "
                                          "FROM public.course_schema_migrations m;", "migration history")
                        stack.compose('stop', 'gateway', 'api', 'postgres', timeout=120)
                        stack.compose('up', '-d', '--no-build', '--wait', '--wait-timeout', '120',
                                      'postgres', timeout=150)
                        stack.compose('up', '-d', '--no-build', '--no-deps', 'api', 'gateway', timeout=120)
                        binding = stack.compose('port', 'gateway', '8080')
                        require(re.fullmatch(r'127\.0\.0\.1:[0-9]+', binding) is not None,
                                'Restarted gateway has no isolated loopback port')
                        stack.url = 'http://' + binding
                        stack.wait_ready()
                        restarted = PsqlSession(stack, 'trace-restarted-observer')
                        require(trace_ok(stack, provider_item['operation'], schema) == stable
                                and snapshot(restarted) == before
                                and restarted.json("SELECT jsonb_agg(to_jsonb(m) ORDER BY file_name) "
                                                   "FROM public.course_schema_migrations m;", "migration history")
                                == history, 'Restart changed canonical trace or durable evidence')
                    passed += 1
                    print('PASS | ' + name, flush=True)
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
        print('FAIL | interrupted', flush=True)
    except Exception as error:
        failed = True
        sql.report_failure(error)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    result = 'PASSED' if passed == len(SCENARIOS) and not failed else 'FAILED'
    print(f'SUMMARY | passed={passed} total={len(SCENARIOS)} result={result}', flush=True)
    return 0 if result == 'PASSED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
