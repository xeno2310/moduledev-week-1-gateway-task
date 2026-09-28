"""Four deterministic READ COMMITTED races against the production SQL boundary.

Run from the repository root. Only PostgreSQL and CLI images are built; each run
owns a fresh Compose project/volume. --list and --help never invoke Docker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import signal
import subprocess
import tempfile
import threading
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db/migrations/020_fix_signal_process_message_id_race.sql"
SIGNAL_A = "concurrency.a"
SIGNAL_B = "concurrency.b"
STATEMENT_SECONDS = 20
REQUEST_SECONDS = 30
BLOCK_SECONDS = 10
SCENARIOS = (
    ("same-signal-commit", False, False, False, "duplicate"),
    ("other-process-commit", True, False, False, "conflict"),
    ("other-body-commit", False, True, False, "conflict"),
    ("other-process-rollback", True, False, True, "accepted"),
)


class CheckFailed(RuntimeError):
    """Only locally authored, non-sensitive messages may be reported."""


def require(condition: bool, label: str) -> None:
    if not condition:
        raise CheckFailed(label)


def compact(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sql_text(value: str) -> str:
    # All sessions explicitly enable standard_conforming_strings.
    return "'" + value.replace("'", "''") + "'"


def decode(raw: str, label: str):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise CheckFailed(label + ": invalid JSON (output withheld)") from None


def synthetic_environment() -> dict[str, str]:
    env = {f"COURSE_{name}_PASSWORD": secrets.token_hex(24)
           for name in ("POSTGRES", "MIGRATOR", "PUBLISHER", "RUNTIME", "WORKER", "OUTBOX", "INBOX")}
    # Unused HTTP services still have variables interpolated by Compose. Supply
    # synthetic values, never inherit credentials or an implicit local .env.
    env.update({
        "COURSE_JWT_ISSUER": "signal-concurrency",
        "COURSE_JWT_AUDIENCE": "signal-concurrency",
        "COURSE_JWT_SIGNING_KEY": secrets.token_hex(32),
        "PROVIDER_HMAC_SECRET": secrets.token_hex(32),
        "PROVIDER_CALLBACK_CAPABILITY": secrets.token_hex(24),
        "PROVIDER_CALLBACK_TOKEN": secrets.token_hex(32),
        "PROVIDER_AUDIT_TOKEN": secrets.token_hex(24),
        "COURSE_TEST_PROFILE": "1",
        "COURSE_FAILPOINT": "",
        "COURSE_GATEWAY_PORT": "0",
    })
    return env


class PsqlSession:
    """One persistent backend, with framed stdout and bounded asynchronous reads.

    ON_ERROR_STOP makes every SQL error fatal to this psql process. stderr is
    discarded: an unexpected EOF fails the test without exposing server detail.
    """

    def __init__(self, stack: Stack, label: str) -> None:
        self.label = label
        self.pending: tuple[str, str, float] | None = None
        self.lines: queue.Queue[str | None] = queue.Queue()
        command = stack.prefix + [
            "exec", "-T", "-e", "PGAPPNAME=" + stack.project + "-" + label,
            "postgres", "psql", "-X", "-q", "-A", "-t",
            "-v", "ON_ERROR_STOP=1", "-P", "pager=off", "-U", "postgres", "-d", "course",
        ]
        try:
            self.process = subprocess.Popen(
                command, cwd=ROOT, env=stack.env, shell=False,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
            )
        except OSError:
            raise CheckFailed(label + ": could not start psql") from None
        stack.sessions.append(self)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.execute(
            f"SET statement_timeout = '{STATEMENT_SECONDS}s';\n"
            "SET lock_timeout = '15s';\n"
            "SET idle_in_transaction_session_timeout = '60s';\n"
            "SET standard_conforming_strings = on;",
            "session configuration",
        )
        info = self.json(
            "SELECT jsonb_build_object('pid', pg_backend_pid(), "
            "'isolation', current_setting('transaction_isolation'));",
            "backend identity",
        )
        require(isinstance(info, dict) and type(info.get("pid")) is int,
                label + ": invalid backend identity")
        require(info.get("isolation") == "read committed", label + ": READ COMMITTED required")
        self.pid = info["pid"]

    def _read(self) -> None:
        try:
            assert self.process.stdout is not None
            for line in self.process.stdout:
                self.lines.put(line.rstrip("\r\n"))
        except (OSError, ValueError):
            pass
        finally:
            self.lines.put(None)

    def send(self, sql: str, label: str, timeout: float = REQUEST_SECONDS) -> None:
        require(self.pending is None, self.label + ": previous request still pending")
        require(self.process.poll() is None, self.label + ": psql is not running")
        marker = "signal_test_done_" + uuid.uuid4().hex
        self.pending = (marker, self.label + " " + label, time.monotonic() + timeout)
        try:
            assert self.process.stdin is not None
            self.process.stdin.write(sql.rstrip() + "\n\\echo " + marker + "\n")
            self.process.stdin.flush()
        except (OSError, ValueError):
            raise CheckFailed(self.label + ": psql input failed") from None

    def finish(self) -> str:
        require(self.pending is not None, self.label + ": no pending request")
        marker, label, deadline = self.pending
        result: list[str] = []
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0, label + ": response deadline exceeded")
            try:
                line = self.lines.get(timeout=remaining)
            except queue.Empty:
                raise CheckFailed(label + ": response deadline exceeded") from None
            require(line is not None, label + ": psql ended before completion (SQL/process failure)")
            if line == marker:
                self.pending = None
                return "\n".join(result)
            if line:
                result.append(line)

    def execute(self, sql: str, label: str, timeout: float = REQUEST_SECONDS) -> str:
        self.send(sql, label, timeout)
        return self.finish()

    def json(self, sql: str, label: str, timeout: float = REQUEST_SECONDS):
        return decode(self.execute(sql, label, timeout), self.label + " " + label)

    def close(self) -> None:
        try:
            if self.process.poll() is None:
                try:
                    assert self.process.stdin is not None
                    self.process.stdin.write("\\q\n")
                    self.process.stdin.flush()
                except (OSError, ValueError):
                    pass
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait(timeout=3)
        finally:
            if self.process.poll() is not None:
                if self.process.stdin is not None:
                    self.process.stdin.close()
                if hasattr(self, "reader"):
                    self.reader.join(timeout=1)
                if self.process.stdout is not None:
                    self.process.stdout.close()


class Stack:
    def __init__(self, directory: Path) -> None:
        self.project = "signal-race-" + uuid.uuid4().hex
        self.flow = "signal-race-" + uuid.uuid4().hex
        self.sessions: list[PsqlSession] = []
        self.cleanup_armed = False
        self.env = {key: value for key, value in os.environ.items()
                    if not key.upper().startswith(("COMPOSE_", "COURSE_", "PROVIDER_"))}
        self.env.update(synthetic_environment())
        env_file = directory / "empty.env"
        env_file.write_text("", encoding="utf-8")
        self.prefix = [
            "docker", "compose", "--project-name", self.project,
            "--project-directory", str(ROOT), "--env-file", str(env_file),
            "-f", str(ROOT / "compose.yaml"),
        ]

    def run(self, command: list[str], label: str, *, stdin: str | None = None,
            timeout: float = 120) -> str:
        try:
            result = subprocess.run(
                command, cwd=ROOT, env=self.env, shell=False, input=stdin,
                text=True, encoding="utf-8", errors="replace", stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, timeout=timeout, check=False,
            )
        except subprocess.TimeoutExpired:
            raise CheckFailed(label + ": subprocess deadline exceeded (output withheld)") from None
        except OSError:
            raise CheckFailed(label + ": could not start subprocess") from None
        require(result.returncode == 0, label + f": exit={result.returncode} (output withheld)")
        return result.stdout.strip()

    def compose(self, *args: str, stdin: str | None = None, timeout: float = 120) -> str:
        return self.run(self.prefix + list(args), "Compose " + args[0], stdin=stdin, timeout=timeout)

    def assert_no_resources(self, label: str) -> None:
        project_filter = "label=com.docker.compose.project=" + self.project
        for kind, command in (
            ("containers", ["docker", "ps", "-aq", "--filter", project_filter]),
            ("volumes", ["docker", "volume", "ls", "-q", "--filter", project_filter]),
            ("networks", ["docker", "network", "ls", "-q", "--filter", project_filter]),
        ):
            require(not self.run(command, label, timeout=30), label + ": project " + kind + " exist")

    def start(self) -> None:
        self.assert_no_resources("Isolation preflight")
        self.cleanup_armed = True
        print("SETUP | building PostgreSQL and CLI for an isolated fresh volume", flush=True)
        self.compose("build", "postgres", "cli", timeout=1200)
        # Both images have project-local implicit tags. No shared worker/Python
        # image is built, no other service starts, and init SQL includes 020.
        self.compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "120", "postgres", timeout=150)

    def cli(self, *args: str, payload: object | None = None) -> dict:
        raw = self.compose("run", "--rm", "-T", "--no-deps", "cli", *args,
                           stdin=None if payload is None else compact(payload) + "\n")
        body = decode(raw, "CLI envelope")
        require(isinstance(body, dict) and body.get("status") == "ok", "CLI did not return success")
        return body

    def publish_flow(self) -> None:
        document = {
            "contract_version": "course-1", "flow_name": self.flow, "version": 1,
            "start_step": "wait-a",
            "steps": [
                {"key": "wait-a", "type": "wait_signal", "signal_type": SIGNAL_A, "outcome": "A"},
                {"key": "wait-b", "type": "wait_signal", "signal_type": SIGNAL_B, "outcome": "B"},
                {"key": "done", "type": "end", "outcome": "COMPLETED"},
            ],
            "transitions": [
                {"from": "wait-a", "outcome": "A", "to": "wait-b"},
                {"from": "wait-b", "outcome": "B", "to": "done"},
            ],
        }
        self.cli("flow", "publish", "/dev/stdin", payload=document)
        self.cli("flow", "activate", self.flow, "--version", "1")

    def start_process(self) -> str:
        body = self.cli("flow", "start", self.flow, "--business-key", uuid.uuid4().hex,
                        "--data", "/dev/stdin", payload={})
        process_id = body.get("result", {}).get("processId")
        try:
            return str(uuid.UUID(process_id))
        except (ValueError, TypeError, AttributeError):
            raise CheckFailed("CLI returned an invalid process identity") from None

    def cleanup(self) -> None:
        session_failed = False
        try:
            # Release T1 first, so a blocked T2 can also quit. Closing either
            # connection rolls back any transaction the scenario left open.
            for session in self.sessions:
                try:
                    session.close()
                except Exception:
                    session_failed = True
        finally:
            if self.cleanup_armed:
                self.compose("down", "--volumes", "--remove-orphans", "--timeout", "5", timeout=120)
                self.assert_no_resources("Cleanup verification")
                print("CLEANUP | isolated project, volume and network removed", flush=True)
        require(not session_failed, "One or more psql clients failed to close cleanly")


def verify_migration(observer: PsqlSession) -> None:
    metadata = observer.json(
        "SELECT jsonb_build_object('body', p.prosrc, 'owner', pg_get_userbyid(p.proowner), "
        "'definer', p.prosecdef, 'config', p.proconfig, 'volatility', p.provolatile, "
        "'language', l.lanname, 'publisher', "
        "has_function_privilege('course_publication', p.oid, 'EXECUTE'), "
        "'public', EXISTS (SELECT 1 FROM aclexplode(COALESCE(p.proacl, "
        "acldefault('f', p.proowner))) AS a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')) "
        "FROM pg_proc AS p JOIN pg_language AS l ON l.oid = p.prolang "
        "WHERE p.oid = to_regprocedure('workflow.signal_process(uuid,text,text,jsonb,text)');",
        "migration definition",
    )
    expected_body = MIGRATION.read_text(encoding="utf-8").split("AS $$\n", 1)[1].split("\n$$;", 1)[0]
    require(isinstance(metadata, dict)
            and metadata.get("body", "").replace("\r\n", "\n").strip() == expected_body.strip(),
            "Migration 020 function body is not installed")
    require({key: value for key, value in metadata.items() if key != "body"} == {
        "owner": "course_owner", "definer": True, "config": ["search_path=pg_catalog, workflow"],
        "volatility": "v", "language": "plpgsql", "publisher": True, "public": False,
    }, "Migration 020 function security or volatility differs from the expected boundary")


def signal_envelope(process_id: str, message_id: str, outcome: str) -> dict:
    if outcome == "conflict":
        return {"status": "error", "code": "workflow.signal_conflict",
                "message": "message id already belongs to another signal", "retryable": False,
                "meta": {"contractVersion": "course-1"}}
    return {"status": "ok", "result": {"resource": "signal", "processId": process_id,
            "messageId": message_id, "signalType": SIGNAL_B, "status": outcome},
            "meta": {"contractVersion": "course-1"}}


def signal_sql(process_id: str, message_id: str, body: dict) -> str:
    encoded = compact(body)
    body_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return ("SELECT workflow.signal_process(" + sql_text(process_id) + "::uuid, "
            + sql_text(SIGNAL_B) + ", " + sql_text(message_id) + ", " + sql_text(encoded)
            + "::jsonb, " + sql_text(body_hash) + ");")


def snapshot_sql(process_ids: list[str], message_id: str) -> str:
    processes = ", ".join(sql_text(value) + "::uuid" for value in process_ids)
    message = sql_text(message_id)
    fields = []
    for key, table, predicate, order in (
        ("signals", "workflow_signals", "message_id = " + message, "message_id"),
        ("events", "workflow_events", "metadata ->> 'messageId' = " + message, "event_id"),
        ("processes", "process_instances", "process_id IN (" + processes + ")", "process_id"),
        ("steps", "step_instances", "process_id IN (" + processes + ")", "step_instance_id"),
        ("history", "workflow_events", "process_id IN (" + processes + ")", "event_id"),
        ("jobs", "workflow_jobs", "process_id IN (" + processes + ")", "job_id"),
    ):
        fields.append(sql_text(key) + ", (SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY "
                      + order + "), '[]'::jsonb) FROM workflow." + table + " AS r WHERE " + predicate + ")")
    return "SELECT jsonb_build_object(" + ", ".join(fields) + ");"


def verify_evidence(baseline: dict, actual: dict, process_id: str, message_id: str, body: dict) -> None:
    require(len(actual["signals"]) == 1, "Expected exactly one global signal row")
    row = actual["signals"][0]
    expected = {"message_id": message_id, "process_id": process_id, "signal_type": SIGNAL_B,
                "body": body, "body_hash": hashlib.sha256(compact(body).encode("utf-8")).hexdigest(),
                "status": "ACCEPTED", "applied_at": None}
    require({key: row.get(key) for key in expected} == expected, "Signal evidence differs from the winner")
    require(len(actual["events"]) == 1, "Expected exactly one event for the messageId")
    event = actual["events"][0]
    require(event["event_type"] == "SignalAccepted" and event["process_id"] == process_id
            and event["step_instance_id"] is None
            and event["metadata"] == {"messageId": message_id, "signalType": SIGNAL_B},
            "SignalAccepted evidence differs from the winner")
    for key in ("processes", "steps", "jobs"):
        require(actual[key] == baseline[key], "Early signal changed " + key)
    history = sorted(baseline["history"] + [event], key=lambda item: item["event_id"])
    require(actual["history"] == history, "A transaction added or changed unexpected workflow history")


def wait_until_blocked(observer: PsqlSession, t1: PsqlSession, t2: PsqlSession) -> None:
    deadline = time.monotonic() + BLOCK_SECONDS
    while time.monotonic() < deadline:
        blocked = observer.json(
            f"SELECT to_jsonb({t1.pid} = ANY(pg_blocking_pids({t2.pid})));", "blocking barrier",
            timeout=max(0.001, deadline - time.monotonic()))
        if blocked is True and time.monotonic() < deadline:
            return
        require(t2.process.poll() is None, "T2 psql exited before a blocking relationship was observed")
        # No timing assumption: only pg_blocking_pids may release the barrier.
    raise CheckFailed("Blocking barrier deadline exceeded; T1 was not observed blocking T2")


def run_scenario(stack: Stack, t1: PsqlSession, t2: PsqlSession, observer: PsqlSession,
                 other_process: bool, other_body: bool, rollback: bool, outcome: str) -> None:
    process_a = stack.start_process()
    process_b = stack.start_process() if other_process else process_a
    process_ids = sorted({process_a, process_b})
    message_id = "signal-race-" + uuid.uuid4().hex
    body_a = {"value": "first"}
    body_b = {"value": "second"} if other_body else body_a
    snapshot = snapshot_sql(process_ids, message_id)
    baseline = observer.json(snapshot, "baseline evidence")
    require(not baseline["signals"] and not baseline["events"], "Message identity was not fresh")
    require(len(baseline["processes"]) == len(process_ids) and not baseline["jobs"],
            "Unexpected process setup")
    require(all(row["state"] == "WAITING_SIGNAL" and row["current_step_key"] == "wait-a"
                for row in baseline["processes"]), "A process is not waiting for signal A")
    require(len(baseline["steps"]) == len(process_ids)
            and all(row["step_key"] == "wait-a" and row["state"] == "WAITING"
                    for row in baseline["steps"]), "Unexpected initial step instances")

    begin = "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL ROLE course_publication;"
    t1.execute(begin, "begin")
    accepted = t1.json(signal_sql(process_a, message_id, body_a), "signal")
    require(accepted == signal_envelope(process_a, message_id, "accepted"), "T1 accepted envelope differs")
    # Inspect uncommitted evidence as the original observer-capable session user.
    # The signal call itself above runs through the publication role boundary.
    staged = t1.json("RESET ROLE;\n" + snapshot, "uncommitted evidence")
    verify_evidence(baseline, staged, process_a, message_id, body_a)
    require(observer.json(snapshot, "uncommitted invisibility") == baseline,
            "T1 evidence became visible before commit")

    t2.execute(begin, "begin")
    t2.send(signal_sql(process_b, message_id, body_b), "signal")
    wait_until_blocked(observer, t1, t2)
    # With one process T2 waits on its process row; with two different
    # processes T2 reaches the uncommitted global unique-key conflict.
    t1.execute("ROLLBACK;" if rollback else "COMMIT;", "release blocker")
    result = decode(t2.finish(), "T2 signal envelope")
    require(result == signal_envelope(process_b, message_id, outcome), "T2 envelope differs")

    visible = observer.json(snapshot, "evidence before T2 commit")
    require(visible == (baseline if rollback else staged),
            "T1 commit/rollback evidence differs before T2 commit")
    t2.execute("COMMIT;", "commit")
    final = observer.json(snapshot, "durable evidence")
    winner = process_b if rollback else process_a
    winning_body = body_b if rollback else body_a
    verify_evidence(baseline, final, winner, message_id, winning_body)
    if rollback:
        require(final["events"][0]["event_id"] != staged["events"][0]["event_id"],
                "Rolled-back T1 event survived")
    else:
        require(final == staged, "T2 changed the committed signal or workflow history")


def interrupted(_signum, _frame) -> None:
    raise KeyboardInterrupt


def report_failure(error: Exception) -> None:
    # Never print subprocess exceptions: their messages can contain arguments,
    # SQL, credentials, or raw captured output.
    message = str(error) if isinstance(error, CheckFailed) else type(error).__name__ + " (details withheld)"
    print("FAIL | " + message, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List four scenarios without invoking Docker")
    args = parser.parse_args()
    if args.list:
        for name, _, _, _, outcome in SCENARIOS:
            print(name + " | T2=" + outcome)
        return 0

    passed = 0
    failed = False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory(prefix="signal-concurrency-") as directory:
            stack = Stack(Path(directory))
            try:
                stack.start()
                t1 = PsqlSession(stack, "T1")
                t2 = PsqlSession(stack, "T2")
                observer = PsqlSession(stack, "observer")
                require(len({t1.pid, t2.pid, observer.pid}) == 3, "Three independent backends required")
                verify_migration(observer)
                stack.publish_flow()
                for name, other_process, other_body, rollback, outcome in SCENARIOS:
                    print("RUN | " + name, flush=True)
                    run_scenario(stack, t1, t2, observer, other_process, other_body, rollback, outcome)
                    passed += 1
                    print("PASS | " + name, flush=True)
            finally:
                # Cleanup still runs on assertion, SQL error, timeout, Ctrl-C,
                # or SIGTERM. A second interrupt must not leave the volume behind.
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
