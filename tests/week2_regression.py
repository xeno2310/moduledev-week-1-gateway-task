from __future__ import annotations

import concurrent.futures
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "autocheck" / "fixtures"

FIXTURE = json.loads(
    (FIXTURES / "fixture.json").read_text(encoding="utf-8")
)

FLOW = str(FIXTURE["flowName"])

DATA = "/audit/" + str(
    FIXTURE["files"]["processData"]["signal"]
)

CHANGED_DATA = "/audit/" + str(
    FIXTURE["files"]["processData"]["changed"]
)

TARGET_SCHEMA = str(FIXTURE["targetSchema"])
TARGET_FUNCTION = str(FIXTURE["targetFunction"])
EFFECT_TABLE = str(FIXTURE["effectTable"])

AUTOMATIC_STEP = str(FIXTURE["steps"]["automatic"])
WAIT_STEP = str(FIXTURE["steps"]["wait"])

RETRY_DATA = "/audit/" + str(
    FIXTURE["files"]["processData"]["retry"]
)

SIGNAL_PAYLOAD = "/audit/" + str(
    FIXTURE["files"]["signalData"]
)

SIGNAL_TYPE = str(FIXTURE["signalType"])
RETRY_ERROR = str(FIXTURE["errorCodes"]["retry"])

STACK_PREFIX: list[str] | None = None
STACK_ENV: dict[str, str] | None = None
STACK_TEMP: tempfile.TemporaryDirectory[str] | None = None
STACK_WORKER_IMAGE = ""


def prepare_stack() -> None:
    global STACK_PREFIX, STACK_ENV, STACK_TEMP, STACK_WORKER_IMAGE
    STACK_TEMP = tempfile.TemporaryDirectory(prefix="week2-regression-")
    project = "week2-regression-" + uuid.uuid4().hex
    STACK_WORKER_IMAGE = project + "-worker:local"
    directory = Path(STACK_TEMP.name)
    env_file = directory / "empty.env"
    env_file.write_text("", encoding="utf-8")
    override = directory / "images.json"
    override.write_text(json.dumps({"services": {
        name: {"image": STACK_WORKER_IMAGE}
        for name in ("worker-a", "worker-b")
    }}), encoding="utf-8")
    STACK_PREFIX = [
        "docker", "compose", "--project-name", project,
        "--project-directory", str(ROOT), "--env-file", str(env_file),
        "-f", str(ROOT / "compose.yaml"), "-f", str(override),
    ]
    STACK_ENV = {
        key: value for key, value in os.environ.items()
        if not key.upper().startswith((
            "COMPOSE_", "COURSE_", "PROVIDER_", "OUTBOX_", "RECEIPT_", "WORKFLOW_",
        ))
    }
    STACK_ENV.update({
        f"COURSE_{name}_PASSWORD": secrets.token_hex(24)
        for name in (
            "POSTGRES", "MIGRATOR", "PUBLISHER", "RUNTIME", "WORKER",
            "OUTBOX", "INBOX", "AUTOCHECK",
        )
    })
    STACK_ENV.update({
        "COURSE_JWT_ISSUER": "week2-regression",
        "COURSE_JWT_AUDIENCE": "week2-regression",
        "COURSE_JWT_SIGNING_KEY": secrets.token_hex(32),
        "PROVIDER_HMAC_SECRET": secrets.token_hex(32),
        "PROVIDER_CALLBACK_CAPABILITY": secrets.token_hex(24),
        "PROVIDER_CALLBACK_TOKEN": secrets.token_hex(32),
        "PROVIDER_AUDIT_TOKEN": secrets.token_hex(24),
        "COURSE_TEST_PROFILE": "1",
        "COURSE_GATEWAY_PORT": "0",
        "COMPOSE_DISABLE_ENV_FILE": "1",
    })


def cleanup_stack() -> None:
    global STACK_PREFIX, STACK_ENV, STACK_TEMP, STACK_WORKER_IMAGE
    if STACK_PREFIX is None:
        return
    try:
        result = compose("down", "--volumes", "--remove-orphans", "--rmi", "local")
        if result.returncode != 0:
            raise RuntimeError("Week 2 isolated Docker cleanup failed (output withheld)")
    finally:
        if STACK_WORKER_IMAGE:
            run(["docker", "image", "rm", STACK_WORKER_IMAGE], timeout=60)
        if STACK_TEMP is not None:
            STACK_TEMP.cleanup()
        STACK_PREFIX = None
        STACK_ENV = None
        STACK_TEMP = None
        STACK_WORKER_IMAGE = ""


def run(
    command: list[str],
    timeout: int = 120,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    if STACK_PREFIX is not None and command[:2] == ["docker", "compose"]:
        command = [*STACK_PREFIX, *command[2:]]
    return subprocess.run(
        command,
        cwd=ROOT,
        env=STACK_ENV,
        input=input_text,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return run(["docker", "compose", *args])


def cli(
    *args: str,
    input_text: str | None = None,
) -> tuple[int, dict | None, str]:
    result = run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "-T",
            "--no-deps",
            "-v",
            f"{FIXTURES}:/audit:ro",
            "cli",
            *args,
        ],
        input_text=input_text,
    )

    body = None

    for line in reversed(
        result.stdout.splitlines() + result.stderr.splitlines()
    ):
        line = line.strip()

        if not line.startswith("{"):
            continue

        try:
            body = json.loads(line)
            break
        except json.JSONDecodeError:
            pass

    return (
        result.returncode,
        body,
        result.stdout + "\n" + result.stderr,
    )


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def db(sql: str) -> str:
    result = run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-X",
            "-q",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "course",
            "-c",
            sql,
        ]
    )

    if result.returncode != 0:
        raise AssertionError(
            "PostgreSQL command failed:\n"
            + result.stdout
            + result.stderr
        )

    return result.stdout.strip()


def claim(owner: str, lease_ms: int = 2000) -> dict[str, int]:
    text = db(
        "SET ROLE workflow_worker; "
        "SELECT job_id::text || '|' || lease_version::text "
        "FROM workflow.claim_jobs("
        + sql_quote(owner)
        + ", 50, "
        + str(lease_ms)
        + ");"
    )

    claimed: dict[str, int] = {}

    for line in text.splitlines():
        line = line.strip()

        if "|" not in line:
            continue

        job_id, lease_version = line.split("|", 1)
        claimed[job_id] = int(lease_version)

    return claimed


def start_test_worker(
    name: str,
    instance_id: str,
    scopes: str,
    failpoint: str = "",
) -> None:
    result = compose(
        "run",
        "-d",
        "--no-deps",
        "--name",
        name,
        "-e",
        f"WORKER_INSTANCE_ID={instance_id}",
        "-e",
        "COURSE_TEST_PROFILE=1",
        "-e",
        "COURSE_JOB_LEASE_MS=2000",
        "-e",
        f"COURSE_FAILPOINT={failpoint}",
        "-e",
        f"WORKFLOW_WORKER_SCOPES={scopes}",
        "worker-a",
    )

    if result.returncode != 0:
        raise AssertionError(
            "Could not start regression worker:\n"
            + result.stdout
            + result.stderr
        )


def remove_container(name: str) -> None:
    run(
        [
            "docker",
            "rm",
            "-f",
            name,
        ],
        timeout=30,
    )


def wait_for_no_claimable_jobs(
    timeout_seconds: float = 10.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        count = int(
            db(
                "SELECT count(*) "
                "FROM workflow.workflow_jobs "
                "WHERE "
                "state = 'READY' "
                "OR ("
                "state = 'RETRY_WAIT' "
                "AND next_attempt_at <= clock_timestamp()"
                ") "
                "OR ("
                "state = 'LEASED' "
                "AND lease_until <= clock_timestamp()"
                ");"
            )
        )

        if count == 0:
            return

        time.sleep(0.1)

    raise AssertionError(
        "Claimable jobs did not drain before regression test"
    )


class WorkflowRegressionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        prepare_stack()
        try:
            for stage, arguments in (
                ("build", ("build", "postgres", "cli", "api", "worker-a")),
                ("startup", ("up", "-d", "--no-build", "api")),
            ):
                result = compose(*arguments)
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Week 2 isolated {stage} failed (output withheld)"
                    )
            for arguments in (
                ("migration", "apply", "/audit/migrations"),
                ("action", "publish", "/audit/" + FIXTURE["files"]["actionManifest"]),
                ("action", "publish", "/audit/" + FIXTURE["files"]["disabledActionManifest"]),
                ("action", "activate", FIXTURE["module"] + "." + FIXTURE["action"],
                 "--version", "1"),
                ("flow", "publish", "/audit/" + FIXTURE["files"]["flowV1Json"]),
                ("flow", "activate", FLOW, "--version", "1"),
            ):
                code, envelope, _ = cli(*arguments)
                if code != 0 or not isinstance(envelope, dict) or envelope.get("status") != "ok":
                    raise RuntimeError("Week 2 fixture setup failed (output withheld)")
        except BaseException:
            cleanup_stack()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        cleanup_stack()

    def finish_signal_process(self, process_id: str) -> None:
        rc, envelope, _ = cli("flow", "signal", process_id, "--type", SIGNAL_TYPE,
                              "--message-id", uuid.uuid4().hex, "--payload", SIGNAL_PAYLOAD)
        self.assertEqual(rc, 0, "Completion signal failed (output withheld)")
        self.assertIsNotNone(envelope)
        self.assertEqual(envelope['status'], 'ok')
        self.assertEqual(db("SELECT state FROM workflow.process_instances WHERE process_id="
                            + sql_quote(process_id) + "::uuid;"), "COMPLETED")

    def publish_crash_flow(self) -> str:
        document = json.loads((FIXTURES / FIXTURE['files']['flowV1Json']).read_text(encoding='utf-8'))
        document['flow_name'] = 'lease-crash-' + uuid.uuid4().hex
        for step in document['steps']:
            if step['type'] == 'automatic':
                step['task']['timeout_ms'] = 30000
        rc, envelope, _ = cli('flow', 'publish', '/dev/stdin', input_text=json.dumps(document))
        self.assertEqual(rc, 0, 'Crash fixture publication failed (output withheld)')
        self.assertEqual(envelope['status'], 'ok')
        rc, envelope, _ = cli('flow', 'activate', document['flow_name'], '--version', '1')
        self.assertEqual(rc, 0, 'Crash fixture activation failed (output withheld)')
        self.assertEqual(envelope['status'], 'ok')
        return document['flow_name']

    def test_concurrent_start_is_idempotent(self) -> None:
        business_key = "regression-concurrent-" + uuid.uuid4().hex

        def start() -> tuple[int, dict | None, str]:
            return cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                business_key,
                "--data",
                DATA,
            )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=8
        ) as pool:
            results = list(pool.map(lambda _: start(), range(8)))

        process_ids = set()

        for rc, body, raw in results:
            self.assertEqual(rc, 0, raw)
            self.assertIsNotNone(body)
            self.assertEqual(body["status"], "ok")
            process_ids.add(body["result"]["processId"])

        self.assertEqual(len(process_ids), 1)

        process_id = next(iter(process_ids))

        row_count = int(
            db(
                "SELECT count(*) "
                "FROM workflow.process_instances "
                "WHERE flow_name = "
                + sql_quote(FLOW)
                + " AND business_key = "
                + sql_quote(business_key)
                + ";"
            )
        )

        started_count = int(
            db(
                "SELECT count(*) "
                "FROM autocheck.workflow_events "
                "WHERE process_id = "
                + sql_quote(process_id)
                + "::uuid "
                "AND event_type = 'ProcessStarted';"
            )
        )

        self.assertEqual(row_count, 1)
        self.assertEqual(started_count, 1)

    def test_start_changed_data_conflicts(self) -> None:
        business_key = "regression-conflict-" + uuid.uuid4().hex

        first = cli(
            "flow",
            "start",
            FLOW,
            "--business-key",
            business_key,
            "--data",
            DATA,
        )

        second = cli(
            "flow",
            "start",
            FLOW,
            "--business-key",
            business_key,
            "--data",
            CHANGED_DATA,
        )

        self.assertEqual(first[0], 0, first[2])
        self.assertNotEqual(second[0], 0, second[2])
        self.assertIsNotNone(second[1])
        self.assertIn(
            "conflict",
            str(second[1].get("code", "")),
        )

    def test_worker_database_boundary(self) -> None:
        values = db(
            "SELECT "
            "has_table_privilege("
            "'workflow_worker',"
            "'workflow.workflow_jobs','SELECT'"
            ")::text || '|' || "
            "has_table_privilege("
            "'workflow_worker',"
            "'workflow.workflow_jobs','INSERT'"
            ")::text || '|' || "
            "has_table_privilege("
            "'workflow_worker',"
            "'workflow.workflow_jobs','UPDATE'"
            ")::text || '|' || "
            "has_table_privilege("
            "'workflow_worker',"
            "'workflow.workflow_jobs','DELETE'"
            ")::text || '|' || "
            "has_function_privilege("
            "'workflow_worker',"
            "'workflow.claim_jobs(text,integer,integer)',"
            "'EXECUTE'"
            ")::text || '|' || "
            "has_function_privilege("
            "'workflow_worker',"
            "'api.invoke(text,text,integer,jsonb,jsonb)',"
            "'EXECUTE'"
            ")::text || '|' || "
            "has_function_privilege("
            "'workflow_worker',"
            "'workflow.finish_job(uuid,text,bigint,text,jsonb)',"
            "'EXECUTE'"
            ")::text || '|' || "
            "has_function_privilege("
            "'workflow_worker',"
            "'workflow.fail_job(uuid,text,bigint,text,boolean)',"
            "'EXECUTE'"
            ")::text || '|' || "
            "has_function_privilege("
            "'workflow_worker',"
            "'workflow.start_process(text,text,jsonb,text)',"
            "'EXECUTE'"
            ")::text;"
        )

        self.assertEqual(
            values,
            "false|false|false|false|true|true|true|true|false",
        )

    def test_stale_lease_is_fenced(self) -> None:
        stopped = compose("stop", "worker-a", "worker-b")
        self.assertEqual(stopped.returncode, 0, stopped.stderr)

        try:
            business_key = "regression-fence-" + uuid.uuid4().hex

            rc, body, raw = cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                business_key,
                "--data",
                DATA,
            )

            self.assertEqual(rc, 0, raw)

            process_id = body["result"]["processId"]

            job_id = db(
                "SELECT job_id::text "
                "FROM autocheck.jobs "
                "WHERE process_id = "
                + sql_quote(process_id)
                + "::uuid "
                "ORDER BY job_id "
                "LIMIT 1;"
            )

            self.assertTrue(job_id)

            owner_a = "regression-a-" + uuid.uuid4().hex
            owner_b = "regression-b-" + uuid.uuid4().hex

            first = claim(owner_a)

            self.assertIn(
                job_id,
                first,
                "first worker did not claim regression job",
            )

            old_lease = first[job_id]

            remaining_ms = int(
                db(
                    "SELECT greatest("
                    "0,"
                    "ceil(extract(epoch FROM ("
                    "lease_until - clock_timestamp()"
                    ")) * 1000)"
                    ")::bigint "
                    "FROM workflow.workflow_jobs "
                    "WHERE job_id = "
                    + sql_quote(job_id)
                    + "::uuid;"
                )
            )

            time.sleep(
                (remaining_ms + 300) / 1000.0
            )

            # This fencing test performs many host-side SQL observations;
            # short-lease timing is asserted separately by the crash/long tests.
            second = claim(owner_b, lease_ms=10000)

            self.assertIn(
                job_id,
                second,
                "second worker did not reclaim expired job",
            )

            self.assertGreater(
                second[job_id],
                old_lease,
            )

            snapshot_sql = ("SELECT to_jsonb(j)::text FROM workflow.workflow_jobs j WHERE job_id="
                            + sql_quote(job_id) + "::uuid;")
            stable = db(snapshot_sql)
            for stale_owner, stale_version in ((owner_a, old_lease), (owner_b, old_lease),
                                                (owner_a, second[job_id])):
                renewed = db("SET ROLE workflow_worker; SELECT workflow.renew_job_lease("
                             + sql_quote(job_id) + "::uuid," + sql_quote(stale_owner) + ","
                             + str(stale_version) + ",2000)::text;")
                self.assertEqual(renewed, 'false')
                self.assertEqual(db(snapshot_sql), stable)

            response = db(
                "SET ROLE workflow_worker; "
                "SELECT workflow.finish_job("
                + sql_quote(job_id)
                + "::uuid,"
                + sql_quote(owner_a)
                + ","
                + str(old_lease)
                + ","
                + sql_quote("__stale_regression__")
                + ",'{}'::jsonb)::text;"
            )

            envelope = json.loads(response)

            self.assertEqual(
                envelope["status"],
                "error",
            )

            self.assertEqual(
                envelope["code"],
                "workflow.lease_stale",
            )

            self.assertEqual(db(snapshot_sql), stable)

            attempts = db(
                "SELECT string_agg("
                "status, ',' ORDER BY attempt_number"
                ") "
                "FROM autocheck.attempts "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid;"
            )

            self.assertEqual(
                attempts,
                "STALE,RUNNING",
            )

            job_execution_id = db(
                "SELECT execution_id::text "
                "FROM workflow.workflow_jobs "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid;"
            )

            attempt_rows = db(
                "SELECT "
                "attempt_id::text || '|' || "
                "execution_id::text || '|' || "
                "status "
                "FROM workflow.task_attempts "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid "
                "ORDER BY attempt_number;"
            ).splitlines()

            self.assertEqual(
                len(attempt_rows),
                2,
            )

            parsed_attempts = [
                row.split("|")
                for row in attempt_rows
            ]

            attempt_ids = {
                row[0]
                for row in parsed_attempts
            }

            execution_ids = {
                row[1]
                for row in parsed_attempts
            }

            self.assertEqual(
                len(attempt_ids),
                2,
            )

            self.assertEqual(
                execution_ids,
                {job_execution_id},
            )

            cleanup = json.loads(
                db(
                    "SET ROLE workflow_worker; "
                    "SELECT workflow.fail_job("
                    + sql_quote(job_id)
                    + "::uuid,"
                    + sql_quote(owner_b)
                    + ","
                    + str(second[job_id])
                    + ","
                    + sql_quote("regression.cleanup")
                    + ",false)::text;"
                )
            )

            self.assertEqual(
                cleanup["status"],
                "ok",
            )

        finally:
            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )


    def test_z_worker_scope_is_independent_from_action_policy(
        self,
    ) -> None:
        normal = compose(
            "up",
            "-d",
            "--no-build",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            normal.returncode,
            0,
            normal.stderr,
        )

        wait_for_no_claimable_jobs()

        stopped = compose(
            "stop",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            stopped.returncode,
            0,
            stopped.stderr,
        )

        container = (
            "week2-regression-policy-"
            + uuid.uuid4().hex
        )

        try:
            start_test_worker(
                container,
                "regression-policy-denied",
                "payment:read,payment:write",
            )

            time.sleep(0.5)

            business_key = (
                "regression-policy-"
                + uuid.uuid4().hex
            )

            rc, body, raw = cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                business_key,
                "--data",
                DATA,
            )

            self.assertEqual(
                rc,
                0,
                raw,
            )

            self.assertIsNotNone(body)

            process_id = body["result"]["processId"]

            deadline = time.monotonic() + 10.0
            observed = ""

            while time.monotonic() < deadline:
                observed = db(
                    "SELECT "
                    "j.job_id::text || '|' || "
                    "j.execution_id::text || '|' || "
                    "j.state || '|' || "
                    "coalesce(a.attempt_id::text,'') || '|' || "
                    "coalesce(a.status,'') || '|' || "
                    "coalesce(a.error_code,'') "
                    "FROM workflow.workflow_jobs AS j "
                    "LEFT JOIN LATERAL ("
                    "SELECT "
                    "attempt_id,"
                    "status,"
                    "error_code,"
                    "attempt_number "
                    "FROM workflow.task_attempts "
                    "WHERE job_id = j.job_id "
                    "ORDER BY attempt_number DESC "
                    "LIMIT 1"
                    ") AS a ON true "
                    "WHERE j.process_id = "
                    + sql_quote(process_id)
                    + "::uuid "
                    "ORDER BY j.created_at DESC "
                    "LIMIT 1;"
                )

                if observed.endswith(
                    "|access.denied"
                ):
                    break

                time.sleep(0.1)

            self.assertTrue(
                observed.endswith(
                    "|access.denied"
                ),
                observed,
            )

            parts = observed.split("|")

            self.assertEqual(
                parts[2],
                "DEAD",
            )

            self.assertEqual(
                parts[4],
                "FAILED",
            )

            self.assertEqual(
                parts[5],
                "access.denied",
            )

            execution_id = parts[1]

            effect_count = int(
                db(
                    "SELECT count(*) "
                    f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                    "WHERE execution_id = "
                    + sql_quote(execution_id)
                    + ";"
                )
            )

            self.assertEqual(
                effect_count,
                0,
            )

        finally:
            remove_container(container)

            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )


    def test_zz_long_action_survives_old_standard_lease(
        self,
    ) -> None:
        normal = compose(
            "up",
            "-d",
            "--no-build",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            normal.returncode,
            0,
            normal.stderr,
        )

        wait_for_no_claimable_jobs()

        stopped = compose(
            "stop",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            stopped.returncode,
            0,
            stopped.stderr,
        )

        worker_a = (
            "week2-regression-long-a-"
            + uuid.uuid4().hex
        )

        worker_b = (
            "week2-regression-long-b-"
            + uuid.uuid4().hex
        )

        original_function = (
            TARGET_FUNCTION
            + "_regression_original"
        )

        slow_installed = False
        original_timeout = None
        active_version = None

        try:
            active_version = int(
                db(
                    "SELECT flow_version "
                    "FROM workflow.flow_versions "
                    "WHERE flow_name = "
                    + sql_quote(FLOW)
                    + " "
                    "AND is_active = true;"
                )
            )

            original_timeout = int(
                db(
                    "SELECT timeout_ms "
                    "FROM workflow.task_definitions "
                    "WHERE flow_name = "
                    + sql_quote(FLOW)
                    + " "
                    "AND flow_version = "
                    + str(active_version)
                    + " "
                    "AND step_key = "
                    + sql_quote(AUTOMATIC_STEP)
                    + ";"
                )
            )

            leftover = int(
                db(
                    "SELECT count(*) "
                    "FROM pg_proc "
                    "WHERE pronamespace = "
                    + sql_quote(TARGET_SCHEMA)
                    + "::regnamespace "
                    "AND proname = "
                    + sql_quote(original_function)
                    + ";"
                )
            )

            self.assertEqual(
                leftover,
                0,
                "Previous slow-action fixture was not restored",
            )

            install_sql = f"""
BEGIN;

ALTER FUNCTION
    {TARGET_SCHEMA}.{TARGET_FUNCTION}(jsonb,jsonb)
RENAME TO
    {original_function};

CREATE FUNCTION
    {TARGET_SCHEMA}.{TARGET_FUNCTION}(
        p_context jsonb,
        p_payload jsonb
    )
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, {TARGET_SCHEMA}
AS $regression_slow$
BEGIN
    PERFORM pg_sleep(2.6);

    RETURN {TARGET_SCHEMA}.{original_function}(
        p_context,
        p_payload
    );
END;
$regression_slow$;

UPDATE workflow.task_definitions
SET timeout_ms = 5000
WHERE flow_name = {sql_quote(FLOW)}
  AND flow_version = {active_version}
  AND step_key = {sql_quote(AUTOMATIC_STEP)};

COMMIT;
"""

            db(install_sql)

            slow_installed = True

            start_test_worker(
                worker_a,
                "regression-long-a",
                "payment:read,"
                "payment:write,"
                "workflow:execute",
            )

            start_test_worker(
                worker_b,
                "regression-long-b",
                "payment:read,"
                "payment:write,"
                "workflow:execute",
            )

            time.sleep(0.5)

            business_key = (
                "regression-long-"
                + uuid.uuid4().hex
            )

            rc, body, raw = cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                business_key,
                "--data",
                DATA,
            )

            self.assertEqual(
                rc,
                0,
                raw,
            )

            self.assertIsNotNone(body)

            process_id = body["result"]["processId"]

            deadline = time.monotonic() + 15.0
            process_row = ""
            saw_heartbeat = False

            while time.monotonic() < deadline:
                process_row = db(
                    "SELECT "
                    "state || '|' || "
                    "coalesce(current_step_key,'') "
                    "FROM workflow.process_instances "
                    "WHERE process_id = "
                    + sql_quote(process_id)
                    + "::uuid;"
                )

                saw_heartbeat = saw_heartbeat or db(
                    "SELECT EXISTS (SELECT 1 FROM workflow.workflow_jobs j "
                    "JOIN workflow.task_attempts a USING(job_id) WHERE j.process_id="
                    + sql_quote(process_id) + "::uuid AND j.state='LEASED' AND j.attempt_count=1 "
                    "AND j.lease_until-j.updated_at=interval '2000 milliseconds' "
                    "AND j.updated_at >= a.started_at + interval '500 milliseconds')::text;") == 'true'

                if process_row == (
                    "WAITING_SIGNAL|"
                    + WAIT_STEP
                ):
                    break

                time.sleep(0.1)

            self.assertEqual(
                process_row,
                "WAITING_SIGNAL|" + WAIT_STEP,
            )

            job_row = db(
                "SELECT "
                "j.job_id::text || '|' || "
                "j.execution_id::text || '|' || "
                "j.state || '|' || "
                "j.attempt_count::text "
                "FROM workflow.workflow_jobs AS j "
                "JOIN workflow.step_instances AS si "
                "ON si.step_instance_id = j.step_instance_id "
                "WHERE j.process_id = "
                + sql_quote(process_id)
                + "::uuid "
                "AND si.step_key = "
                + sql_quote(AUTOMATIC_STEP)
                + " "
                "ORDER BY j.created_at "
                "LIMIT 1;"
            )

            parts = job_row.split("|")

            self.assertEqual(
                len(parts),
                4,
                job_row,
            )

            job_id = parts[0]
            execution_id = parts[1]

            self.assertEqual(
                parts[2],
                "SUCCEEDED",
            )

            self.assertEqual(
                int(parts[3]),
                1,
            )

            attempt_summary = db(
                "SELECT "
                "count(*)::text || '|' || "
                "string_agg("
                "status,',' ORDER BY attempt_number"
                ") "
                "FROM workflow.task_attempts "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid;"
            )

            self.assertEqual(
                attempt_summary,
                "1|SUCCEEDED",
            )

            duration_ms = int(
                db(
                    "SELECT round("
                    "extract(epoch FROM ("
                    "finished_at - started_at"
                    ")) * 1000"
                    ")::bigint "
                    "FROM workflow.task_attempts "
                    "WHERE job_id = "
                    + sql_quote(job_id)
                    + "::uuid "
                    "ORDER BY attempt_number "
                    "LIMIT 1;"
                )
            )

            self.assertGreater(
                duration_ms,
                2000,
            )

            effect_count = int(
                db(
                    "SELECT count(*) "
                    f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                    "WHERE execution_id = "
                    + sql_quote(execution_id)
                    + ";"
                )
            )

            self.assertEqual(
                effect_count,
                1,
            )

            claim_events = int(
                db(
                    "SELECT count(*) "
                    "FROM workflow.workflow_events "
                    "WHERE process_id = "
                    + sql_quote(process_id)
                    + "::uuid "
                    "AND event_type = 'TaskClaimed';"
                )
            )

            self.assertEqual(
                claim_events,
                1,
            )

            self.assertTrue(saw_heartbeat, "Long action did not expose independent heartbeat evidence")
            self.finish_signal_process(process_id)

        finally:
            remove_container(worker_a)
            remove_container(worker_b)

            if (
                slow_installed
                and original_timeout is not None
                and active_version is not None
            ):
                restore_sql = f"""
BEGIN;

UPDATE workflow.task_definitions
SET timeout_ms = {original_timeout}
WHERE flow_name = {sql_quote(FLOW)}
  AND flow_version = {active_version}
  AND step_key = {sql_quote(AUTOMATIC_STEP)};

DROP FUNCTION IF EXISTS
    {TARGET_SCHEMA}.{TARGET_FUNCTION}(jsonb,jsonb);

ALTER FUNCTION
    {TARGET_SCHEMA}.{original_function}(jsonb,jsonb)
RENAME TO
    {TARGET_FUNCTION};

COMMIT;
"""

                db(restore_sql)

            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )


    def test_zzz_crash_after_action_rolls_back_and_recovers(self) -> None:
        self.assert_crash_recovery("after_action_before_finish")

    def test_zzz_crash_after_claim_uses_short_lease(self) -> None:
        self.assert_crash_recovery("after_job_claim")

    def assert_crash_recovery(self, failpoint: str) -> None:
        normal = compose(
            "up",
            "-d",
            "--no-build",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            normal.returncode,
            0,
            normal.stderr,
        )

        wait_for_no_claimable_jobs()

        stopped = compose(
            "stop",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            stopped.returncode,
            0,
            stopped.stderr,
        )

        crash_worker = (
            "week2-regression-crash-"
            + uuid.uuid4().hex
        )

        recovery_worker = (
            "week2-regression-recovery-"
            + uuid.uuid4().hex
        )

        crash_instance = (
            "regression-crash-"
            + uuid.uuid4().hex
        )

        recovery_instance = (
            "regression-recovery-"
            + uuid.uuid4().hex
        )

        try:
            flow = self.publish_crash_flow()
            start_test_worker(
                crash_worker,
                crash_instance,
                "payment:read,"
                "payment:write,"
                "workflow:execute",
                failpoint=failpoint,
            )

            business_key = (
                "regression-crash-after-action-"
                + uuid.uuid4().hex
            )

            rc, body, raw = cli(
                "flow",
                "start",
                flow,
                "--business-key",
                business_key,
                "--data",
                DATA,
            )

            self.assertEqual(
                rc,
                0,
                raw,
            )

            self.assertIsNotNone(body)

            process_id = body["result"]["processId"]

            # ------------------------------------------------
            # Exact acknowledgement, while the replacement worker is stopped.
            # ------------------------------------------------

            deadline = time.monotonic() + 12.0
            acknowledged = False

            while time.monotonic() < deadline:
                logs = run(
                    [
                        "docker",
                        "logs",
                        crash_worker,
                    ],
                    timeout=15,
                )

                combined = (
                    logs.stdout
                    + "\n"
                    + logs.stderr
                )

                try:
                    entries = [
                        json.loads(line)
                        for line in combined.splitlines()
                        if line.strip().startswith("{")
                    ]
                except json.JSONDecodeError:
                    entries = []

                acknowledged = any(
                    item.get("event")
                    == "failpoint.reached"
                    and item.get("name")
                    == failpoint
                    and item.get("instanceId")
                    == crash_instance
                    for item in entries
                )

                if acknowledged:
                    break

                time.sleep(0.1)

            self.assertTrue(
                acknowledged,
                failpoint + " was not reached",
            )

            # ------------------------------------------------
            # Claim transaction was committed earlier, so job
            # and attempt are visible. No committed action effect is visible
            # at either crash boundary.
            # ------------------------------------------------

            job_row = db(
                "SELECT "
                "j.job_id::text || '|' || "
                "j.execution_id::text || '|' || "
                "j.state || '|' || "
                "j.lease_version::text || '|' || "
                "j.attempt_count::text "
                "FROM workflow.workflow_jobs AS j "
                "JOIN workflow.step_instances AS si "
                "ON si.step_instance_id = j.step_instance_id "
                "WHERE j.process_id = "
                + sql_quote(process_id)
                + "::uuid "
                "AND si.step_key = "
                + sql_quote(AUTOMATIC_STEP)
                + " "
                "ORDER BY j.created_at "
                "LIMIT 1;"
            )

            job_parts = job_row.split("|")

            self.assertEqual(
                len(job_parts),
                5,
                job_row,
            )

            job_id = job_parts[0]
            execution_id = job_parts[1]
            first_lease_version = int(job_parts[3])

            self.assertEqual(
                job_parts[2],
                "LEASED",
            )

            self.assertEqual(
                int(job_parts[4]),
                1,
            )

            attempt_before = db(
                "SELECT "
                "attempt_id::text || '|' || "
                "execution_id::text || '|' || "
                "lease_version::text || '|' || "
                "attempt_number::text || '|' || "
                "status "
                "FROM workflow.task_attempts "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid "
                "ORDER BY attempt_number;"
            )

            before_parts = attempt_before.split("|")

            self.assertEqual(
                len(before_parts),
                5,
                attempt_before,
            )

            first_attempt_id = before_parts[0]

            self.assertEqual(
                before_parts[1],
                execution_id,
            )

            self.assertEqual(
                before_parts[4],
                "RUNNING",
            )

            effect_before = int(
                db(
                    "SELECT count(*) "
                    f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                    "WHERE execution_id = "
                    + sql_quote(execution_id)
                    + ";"
                )
            )

            self.assertEqual(
                effect_before,
                0,
            )

            # Observe actual renewals for >2s while the acknowledged failpoint
            # remains held. This is a database barrier, not a timing sleep.
            deadline = time.monotonic() + 8
            renewed = False
            while time.monotonic() < deadline:
                lease_fact = json.loads(db(
                    "SELECT jsonb_build_object('duration',extract(epoch FROM (j.lease_until-j.updated_at))*1000,"
                    "'renewed',j.updated_at >= a.started_at + interval '2100 milliseconds',"
                    "'count',j.attempt_count,'version',j.lease_version,'failure',j.failure_count)::text "
                    "FROM workflow.workflow_jobs j JOIN workflow.task_attempts a USING(job_id) WHERE j.job_id="
                    + sql_quote(job_id) + "::uuid AND a.attempt_number=1;"))
                self.assertEqual(lease_fact['duration'], 2000)
                self.assertEqual(lease_fact['count'], 1)
                self.assertEqual(lease_fact['version'], first_lease_version)
                self.assertEqual(lease_fact['failure'], 0)
                if lease_fact['renewed']:
                    renewed = True
                    break
            self.assertTrue(renewed, 'Live failpoint did not retain a short lease with heartbeat')

            # ------------------------------------------------
            # Real process crash: connection disappears and
            # uncommitted action transaction must rollback.
            # ------------------------------------------------

            killed_at = time.monotonic()
            killed_db_at = db("SELECT clock_timestamp()::text;")
            killed = run(["docker", "rm", "-f", crash_worker], timeout=10)
            self.assertEqual(killed.returncode, 0, "Crash worker kill failed (output withheld)")
            last_lease = db("SELECT lease_until::text FROM workflow.workflow_jobs WHERE job_id="
                            + sql_quote(job_id) + "::uuid;")

            effect_after_crash = int(
                db(
                    "SELECT count(*) "
                    f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                    "WHERE execution_id = "
                    + sql_quote(execution_id)
                    + ";"
                )
            )

            self.assertEqual(
                effect_after_crash,
                0,
            )

            # ------------------------------------------------
            # New real worker must reclaim after lease expiry.
            # ------------------------------------------------

            start_test_worker(
                recovery_worker,
                recovery_instance,
                "payment:read,"
                "payment:write,"
                "workflow:execute",
            )

            deadline = killed_at + 12.0
            process_row = ""

            while time.monotonic() < deadline:
                process_row = db(
                    "SELECT "
                    "state || '|' || "
                    "coalesce(current_step_key,'') "
                    "FROM workflow.process_instances "
                    "WHERE process_id = "
                    + sql_quote(process_id)
                    + "::uuid;"
                )

                if process_row == (
                    "WAITING_SIGNAL|"
                    + WAIT_STEP
                ):
                    break

                time.sleep(0.15)

            self.assertEqual(
                process_row,
                "WAITING_SIGNAL|" + WAIT_STEP,
            )

            self.assertLess(time.monotonic() - killed_at, 12.0,
                            'Recovery must be bounded well below the 30s task timeout')
            # started_at is transaction-time now(), slightly before the claim
            # predicate uses clock_timestamp(); allow only 50ms for that query.
            timing = json.loads(db(
                "SELECT jsonb_build_object('afterLease',started_at >= " + sql_quote(last_lease)
                + "::timestamptz - interval '50 milliseconds','afterKillMs',extract(epoch FROM (started_at-"
                + sql_quote(killed_db_at) + "::timestamptz))*1000)::text "
                "FROM workflow.task_attempts WHERE job_id=" + sql_quote(job_id)
                + "::uuid AND attempt_number=2;"))
            self.assertTrue(timing['afterLease'], 'Reclaimed before the last committed lease expired')
            self.assertGreaterEqual(timing['afterKillMs'], 0)
            self.assertLess(timing['afterKillMs'], 12000)
            print(f"EVIDENCE | {failpoint} leaseMs=2000 taskTimeoutMs=30000 "
                  f"reclaimAfterKillMs={timing['afterKillMs']:.1f}", flush=True)

            # ------------------------------------------------
            # Stable jobId/executionId, newer leaseVersion,
            # new attemptId and exact append-only history.
            # ------------------------------------------------

            recovered_job = db(
                "SELECT "
                "job_id::text || '|' || "
                "execution_id::text || '|' || "
                "state || '|' || "
                "lease_version::text || '|' || "
                "attempt_count::text "
                "FROM workflow.workflow_jobs "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid;"
            )

            recovered_parts = recovered_job.split("|")

            self.assertEqual(
                recovered_parts[0],
                job_id,
            )

            self.assertEqual(
                recovered_parts[1],
                execution_id,
            )

            self.assertEqual(
                recovered_parts[2],
                "SUCCEEDED",
            )

            self.assertGreater(
                int(recovered_parts[3]),
                first_lease_version,
            )

            self.assertEqual(
                int(recovered_parts[4]),
                2,
            )

            attempt_rows = db(
                "SELECT "
                "attempt_id::text || '|' || "
                "execution_id::text || '|' || "
                "lease_version::text || '|' || "
                "attempt_number::text || '|' || "
                "status || '|' || "
                "coalesce(error_code,'') "
                "FROM workflow.task_attempts "
                "WHERE job_id = "
                + sql_quote(job_id)
                + "::uuid "
                "ORDER BY attempt_number;"
            ).splitlines()

            self.assertEqual(
                len(attempt_rows),
                2,
            )

            first = attempt_rows[0].split("|")
            second = attempt_rows[1].split("|")

            self.assertEqual(
                first[0],
                first_attempt_id,
            )

            self.assertNotEqual(
                second[0],
                first_attempt_id,
            )

            self.assertEqual(
                first[1],
                execution_id,
            )

            self.assertEqual(
                second[1],
                execution_id,
            )

            self.assertEqual(
                first[4],
                "STALE",
            )

            self.assertEqual(
                first[5],
                "workflow.lease_expired",
            )

            self.assertEqual(
                second[4],
                "SUCCEEDED",
            )

            self.assertEqual(
                second[5],
                "",
            )

            effect_final = int(
                db(
                    "SELECT count(*) "
                    f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                    "WHERE execution_id = "
                    + sql_quote(execution_id)
                    + ";"
                )
            )

            self.assertEqual(
                effect_final,
                1,
            )

            claim_events = int(
                db(
                    "SELECT count(*) "
                    "FROM workflow.workflow_events "
                    "WHERE process_id = "
                    + sql_quote(process_id)
                    + "::uuid "
                    "AND event_type = 'TaskClaimed';"
                )
            )

            self.assertEqual(
                claim_events,
                2,
            )

            success_metadata = db(
                "SELECT "
                "(metadata ->> 'processId') || '|' || "
                "(metadata ->> 'jobId') || '|' || "
                "(metadata ->> 'executionId') || '|' || "
                "(metadata ->> 'attemptId') || '|' || "
                "(metadata ->> 'leaseVersion') "
                "FROM workflow.workflow_events "
                "WHERE process_id = "
                + sql_quote(process_id)
                + "::uuid "
                "AND event_type = 'TaskSucceeded' "
                "ORDER BY occurred_at DESC, event_id DESC "
                "LIMIT 1;"
            ).split("|")

            self.assertEqual(
                success_metadata,
                [
                    process_id,
                    job_id,
                    execution_id,
                    second[0],
                    second[2],
                ],
            )

            self.assertEqual(db("SELECT failure_count::text FROM workflow.workflow_jobs WHERE job_id="
                                + sql_quote(job_id) + "::uuid;"), "0")
            self.finish_signal_process(process_id)

        finally:
            remove_container(crash_worker)
            remove_container(recovery_worker)

            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )


    def test_zzzz_bounded_retry_preserves_identity_and_history(
        self,
    ) -> None:
        normal = compose(
            "up",
            "-d",
            "--no-build",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            normal.returncode,
            0,
            normal.stderr,
        )

        wait_for_no_claimable_jobs()

        business_key = (
            "regression-retry-"
            + uuid.uuid4().hex
        )

        rc, body, raw = cli(
            "flow",
            "start",
            FLOW,
            "--business-key",
            business_key,
            "--data",
            RETRY_DATA,
        )

        self.assertEqual(
            rc,
            0,
            raw,
        )

        self.assertIsNotNone(body)

        process_id = body["result"]["processId"]

        deadline = time.monotonic() + 15.0
        process_state = ""

        while time.monotonic() < deadline:
            process_state = db(
                "SELECT state "
                "FROM workflow.process_instances "
                "WHERE process_id = "
                + sql_quote(process_id)
                + "::uuid;"
            )

            if process_state == "FAILED":
                break

            time.sleep(0.1)

        self.assertEqual(
            process_state,
            "FAILED",
        )

        job_row = db(
            "SELECT "
            "job_id::text || '|' || "
            "execution_id::text || '|' || "
            "state || '|' || "
            "attempt_count::text || '|' || "
            "failure_count::text "
            "FROM workflow.workflow_jobs "
            "WHERE process_id = "
            + sql_quote(process_id)
            + "::uuid;"
        )

        parts = job_row.split("|")

        self.assertEqual(
            len(parts),
            5,
            job_row,
        )

        job_id = parts[0]
        execution_id = parts[1]

        self.assertEqual(
            parts[2],
            "DEAD",
        )

        self.assertEqual(
            int(parts[3]),
            3,
        )

        self.assertEqual(
            int(parts[4]),
            3,
        )

        attempt_rows = db(
            "SELECT "
            "attempt_id::text || '|' || "
            "job_id::text || '|' || "
            "execution_id::text || '|' || "
            "attempt_number::text || '|' || "
            "status || '|' || "
            "coalesce(error_code,'') "
            "FROM workflow.task_attempts "
            "WHERE job_id = "
            + sql_quote(job_id)
            + "::uuid "
            "ORDER BY attempt_number;"
        ).splitlines()

        self.assertEqual(
            len(attempt_rows),
            3,
        )

        attempts = [
            row.split("|")
            for row in attempt_rows
        ]

        self.assertEqual(
            [
                int(row[3])
                for row in attempts
            ],
            [1, 2, 3],
        )

        self.assertEqual(
            len(
                {
                    row[0]
                    for row in attempts
                }
            ),
            3,
        )

        self.assertEqual(
            {
                row[1]
                for row in attempts
            },
            {job_id},
        )

        self.assertEqual(
            {
                row[2]
                for row in attempts
            },
            {execution_id},
        )

        self.assertEqual(
            {
                row[4]
                for row in attempts
            },
            {"FAILED"},
        )

        self.assertEqual(
            {
                row[5]
                for row in attempts
            },
            {RETRY_ERROR},
        )

        effect_count = int(
            db(
                "SELECT count(*) "
                f"FROM {TARGET_SCHEMA}.{EFFECT_TABLE} "
                "WHERE execution_id = "
                + sql_quote(execution_id)
                + ";"
            )
        )

        self.assertEqual(
            effect_count,
            0,
        )

        event_summary = db(
            "SELECT "
            "count(*) FILTER ("
            "WHERE event_type = 'TaskClaimed'"
            ")::text || '|' || "
            "count(*) FILTER ("
            "WHERE event_type = 'TaskRetryScheduled'"
            ")::text || '|' || "
            "count(*) FILTER ("
            "WHERE event_type = 'TaskFailed'"
            ")::text "
            "FROM workflow.workflow_events "
            "WHERE process_id = "
            + sql_quote(process_id)
            + "::uuid;"
        )

        self.assertEqual(
            event_summary,
            "3|2|1",
        )

        event_link_summary = db(
            "WITH checked AS ("
            "SELECT "
            "e.event_type, "
            "("
            "a.attempt_id IS NULL "
            "OR e.metadata ->> 'processId' "
            "IS DISTINCT FROM e.process_id::text "
            "OR e.metadata ->> 'jobId' "
            "IS DISTINCT FROM a.job_id::text "
            "OR e.metadata ->> 'executionId' "
            "IS DISTINCT FROM a.execution_id::text "
            "OR e.metadata ->> 'attemptId' "
            "IS DISTINCT FROM a.attempt_id::text "
            "OR e.metadata ->> 'leaseVersion' "
            "IS DISTINCT FROM a.lease_version::text"
            ") AS bad "
            "FROM workflow.workflow_events AS e "
            "LEFT JOIN workflow.task_attempts AS a "
            "ON a.attempt_id = "
            "NULLIF(e.metadata ->> 'attemptId','')::uuid "
            "WHERE e.process_id = "
            + sql_quote(process_id)
            + "::uuid "
            "AND e.event_type IN ("
            "'TaskRetryScheduled',"
            "'TaskFailed'"
            ")"
            ") "
            "SELECT "
            "count(*)::text || '|' || "
            "count(*) FILTER (WHERE bad)::text "
            "FROM checked;"
        )

        self.assertEqual(
            event_link_summary,
            "3|0",
        )


    def test_zzzzz_signal_duplicate_and_conflict_preserve_history(
        self,
    ) -> None:
        normal = compose(
            "up",
            "-d",
            "--no-build",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            normal.returncode,
            0,
            normal.stderr,
        )

        wait_for_no_claimable_jobs()

        stopped = compose(
            "stop",
            "worker-a",
            "worker-b",
        )

        self.assertEqual(
            stopped.returncode,
            0,
            stopped.stderr,
        )

        try:
            key_a = (
                "regression-signal-a-"
                + uuid.uuid4().hex
            )

            key_b = (
                "regression-signal-b-"
                + uuid.uuid4().hex
            )

            rc_a, body_a, raw_a = cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                key_a,
                "--data",
                DATA,
            )

            self.assertEqual(
                rc_a,
                0,
                raw_a,
            )

            self.assertIsNotNone(body_a)

            process_a = body_a["result"]["processId"]

            rc_b, body_b, raw_b = cli(
                "flow",
                "start",
                FLOW,
                "--business-key",
                key_b,
                "--data",
                DATA,
            )

            self.assertEqual(
                rc_b,
                0,
                raw_b,
            )

            self.assertIsNotNone(body_b)

            process_b = body_b["result"]["processId"]

            message_id = (
                "regression-message-"
                + uuid.uuid4().hex
            )

            accepted_rc, accepted, accepted_raw = cli(
                "flow",
                "signal",
                process_a,
                "--type",
                SIGNAL_TYPE,
                "--message-id",
                message_id,
                "--payload",
                SIGNAL_PAYLOAD,
            )

            self.assertEqual(
                accepted_rc,
                0,
                accepted_raw,
            )

            self.assertIsNotNone(accepted)

            self.assertEqual(
                accepted["result"]["status"],
                "accepted",
            )

            signal_after_accept = db(
                "SELECT "
                "process_id::text || '|' || "
                "signal_type || '|' || "
                "status || '|' || "
                "body_hash "
                "FROM workflow.workflow_signals "
                "WHERE message_id = "
                + sql_quote(message_id)
                + ";"
            )

            events_after_accept = int(
                db(
                    "SELECT count(*) "
                    "FROM workflow.workflow_events "
                    "WHERE process_id = "
                    + sql_quote(process_a)
                    + "::uuid;"
                )
            )

            duplicate_rc, duplicate, duplicate_raw = cli(
                "flow",
                "signal",
                process_a,
                "--type",
                SIGNAL_TYPE,
                "--message-id",
                message_id,
                "--payload",
                SIGNAL_PAYLOAD,
            )

            self.assertEqual(
                duplicate_rc,
                0,
                duplicate_raw,
            )

            self.assertIsNotNone(duplicate)

            self.assertEqual(
                duplicate["result"]["status"],
                "duplicate",
            )

            self.assertEqual(
                db(
                    "SELECT "
                    "process_id::text || '|' || "
                    "signal_type || '|' || "
                    "status || '|' || "
                    "body_hash "
                    "FROM workflow.workflow_signals "
                    "WHERE message_id = "
                    + sql_quote(message_id)
                    + ";"
                ),
                signal_after_accept,
            )

            self.assertEqual(
                int(
                    db(
                        "SELECT count(*) "
                        "FROM workflow.workflow_events "
                        "WHERE process_id = "
                        + sql_quote(process_a)
                        + "::uuid;"
                    )
                ),
                events_after_accept,
            )

            original_signal = json.loads(
                (
                    FIXTURES
                    / FIXTURE["files"]["signalData"]
                ).read_text(encoding="utf-8")
            )

            self.assertEqual(
                len(original_signal),
                1,
            )

            signal_key = next(iter(original_signal))

            changed_signal = {
                signal_key:
                    str(original_signal[signal_key])
                    + "-changed"
            }

            conflict_rc, conflict, conflict_raw = cli(
                "flow",
                "signal",
                process_a,
                "--type",
                SIGNAL_TYPE,
                "--message-id",
                message_id,
                "--payload",
                "/dev/stdin",
                input_text=json.dumps(
                    changed_signal,
                    ensure_ascii=False,
                ),
            )

            self.assertNotEqual(
                conflict_rc,
                0,
                conflict_raw,
            )

            self.assertIsNotNone(conflict)

            self.assertIn(
                "conflict",
                str(conflict.get("code", "")),
            )

            self.assertEqual(
                db(
                    "SELECT "
                    "process_id::text || '|' || "
                    "signal_type || '|' || "
                    "status || '|' || "
                    "body_hash "
                    "FROM workflow.workflow_signals "
                    "WHERE message_id = "
                    + sql_quote(message_id)
                    + ";"
                ),
                signal_after_accept,
            )

            self.assertEqual(
                int(
                    db(
                        "SELECT count(*) "
                        "FROM workflow.workflow_events "
                        "WHERE process_id = "
                        + sql_quote(process_a)
                        + "::uuid;"
                    )
                ),
                events_after_accept,
            )

            global_rc, global_conflict, global_raw = cli(
                "flow",
                "signal",
                process_b,
                "--type",
                SIGNAL_TYPE,
                "--message-id",
                message_id,
                "--payload",
                SIGNAL_PAYLOAD,
            )

            self.assertNotEqual(
                global_rc,
                0,
                global_raw,
            )

            self.assertIsNotNone(global_conflict)

            self.assertIn(
                "conflict",
                str(global_conflict.get("code", "")),
            )

            self.assertEqual(
                int(
                    db(
                        "SELECT count(*) "
                        "FROM workflow.workflow_signals "
                        "WHERE message_id = "
                        + sql_quote(message_id)
                        + ";"
                    )
                ),
                1,
            )

            self.assertEqual(
                int(
                    db(
                        "SELECT count(*) "
                        "FROM workflow.workflow_signals "
                        "WHERE process_id = "
                        + sql_quote(process_b)
                        + "::uuid;"
                    )
                ),
                0,
            )

            self.assertEqual(
                int(
                    db(
                        "SELECT count(*) "
                        "FROM workflow.workflow_events "
                        "WHERE process_id = "
                        + sql_quote(process_a)
                        + "::uuid "
                        "AND event_type = 'SignalAccepted';"
                    )
                ),
                1,
            )

        finally:
            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )

        deadline = time.monotonic() + 10.0
        process_a_state = ""
        process_b_state = ""

        while time.monotonic() < deadline:
            process_a_state = db(
                "SELECT state "
                "FROM workflow.process_instances "
                "WHERE process_id = "
                + sql_quote(process_a)
                + "::uuid;"
            )

            process_b_state = db(
                "SELECT state "
                "FROM workflow.process_instances "
                "WHERE process_id = "
                + sql_quote(process_b)
                + "::uuid;"
            )

            if (
                process_a_state == "COMPLETED"
                and process_b_state == "WAITING_SIGNAL"
            ):
                break

            time.sleep(0.1)

        self.assertEqual(
            process_a_state,
            "COMPLETED",
        )

        self.assertEqual(
            process_b_state,
            "WAITING_SIGNAL",
        )

        self.assertEqual(
            db(
                "SELECT status "
                "FROM workflow.workflow_signals "
                "WHERE message_id = "
                + sql_quote(message_id)
                + ";"
            ),
            "APPLIED",
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["--list"]:
        names = unittest.defaultTestLoader.getTestCaseNames(WorkflowRegressionTests)
        print("\n".join(names))
        print(f"total={len(names)} (list only; Docker not invoked)")
    else:
        unittest.main(verbosity=2)
