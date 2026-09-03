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


def run(command: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
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


def cli(*args: str) -> tuple[int, dict | None, str]:
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
        ]
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

            time.sleep(2.3)

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

        finally:
            compose(
                "up",
                "-d",
                "--no-build",
                "worker-a",
                "worker-b",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)