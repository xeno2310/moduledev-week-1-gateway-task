from __future__ import annotations

import concurrent.futures
import json
import subprocess
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


def run(
    command: list[str],
    timeout: int = 120,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
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
        result = compose("up", "-d", "--no-build", "postgres")

        if result.returncode != 0:
            raise RuntimeError(
                "PostgreSQL is not available:\n"
                + result.stdout
                + result.stderr
            )

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

            second = claim(owner_b)

            self.assertIn(
                job_id,
                second,
                "second worker did not reclaim expired job",
            )

            self.assertGreater(
                second[job_id],
                old_lease,
            )

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


    def test_zzz_crash_after_action_rolls_back_and_recovers(
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
            start_test_worker(
                crash_worker,
                crash_instance,
                "payment:read,"
                "payment:write,"
                "workflow:execute",
                failpoint="after_action_before_finish",
            )

            business_key = (
                "regression-crash-after-action-"
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

            # ------------------------------------------------
            # Wait until action executed but finish_job has
            # deliberately not been called yet.
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
                    == "after_action_before_finish"
                    and item.get("instanceId")
                    == crash_instance
                    for item in entries
                )

                if acknowledged:
                    break

                time.sleep(0.1)

            self.assertTrue(
                acknowledged,
                "after_action_before_finish was not reached",
            )

            # ------------------------------------------------
            # Claim transaction was committed earlier, so job
            # and attempt are visible. Action transaction is
            # still open and its effect must not be visible.
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

            # ------------------------------------------------
            # Real process crash: connection disappears and
            # uncommitted action transaction must rollback.
            # ------------------------------------------------

            remove_container(crash_worker)

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

            deadline = time.monotonic() + 15.0
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
    unittest.main(verbosity=2)