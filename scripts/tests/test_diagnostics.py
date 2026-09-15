"""离线验收工具回归：真实临时文件/数据库，进程测试使用受控本机子进程。"""
from contextlib import closing
import select
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import diagnostics as d
import evidence as e


def event(name, **fields):
    return {"run_id": "one", "fields": {"event": name, **fields}}


def start():
    return event("application_start", **d.VERSIONS, concurrency=4)


def finals():
    return [event("session_shutdown", success=True),
            event("collector_summary", final_snapshot=True, running_workers=0),
            *[event("attempt_summary", final_snapshot=True, scope=scope) for scope in ("total", "interval")],
            *[event("logging_queue", final_snapshot=True, sink=sink, dropped_total=0) for sink in ("file", "stderr")]]


class EvidenceTests(unittest.TestCase):
    def test_atomic_write_does_not_overwrite_or_publish_partial_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "report.json"
            e.write_json(target, {"status": "passed"})
            with self.assertRaises(FileExistsError):
                e.write_json(target, {"status": "failed"})
            self.assertEqual(json.loads(target.read_text())["status"], "passed")
            with self.assertRaises(ValueError):
                e.write_json(root / "bad.json", {"value": float("nan")})
            self.assertEqual(list(root.iterdir()), [target])

    def test_source_scope_includes_tests_and_excludes_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "tests").mkdir()
            test = root / "tests/check.rs"
            test.write_text("first")
            before = e.source_identity(root)
            (root / "logs").mkdir()
            (root / "logs/run.jsonl").write_text("ignored")
            self.assertEqual(e.source_identity(root), before)
            test.write_text("second")
            self.assertNotEqual(e.source_identity(root)["source_sha256"], before["source_sha256"])

    def test_build_failure_and_source_changes_never_publish_prepared_binary(self):
        for failure in ("build", "changed"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "src"
                source.mkdir()
                (source / "main.rs").write_text("first")
                run = root / "run"
                run.mkdir()
                # 陈旧产物不在独立 target 目录中，不能被复制。
                (root / "target/release").mkdir(parents=True)
                (root / "target/release/bt-sniffer").write_text("stale")

                def build(command, **kwargs):
                    if failure == "build":
                        raise subprocess.CalledProcessError(1, command)
                    (source / "main.rs").write_text("changed")
                    return subprocess.CompletedProcess(command, 0)

                with patch.object(d, "command_text", return_value="fixture"), patch.object(d.subprocess, "check_output", return_value=b""), patch.object(d.subprocess, "run", side_effect=build):
                    with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                        d.prepare(root, run)
                self.assertFalse((run / "bt-sniffer").exists())
                self.assertFalse((run / "preparation.json").exists())


class LogTests(unittest.TestCase):
    def test_partial_utf8_rotation_and_final_partial_line(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "bt-sniffer.1.jsonl"
            data = json.dumps(event("说明"), ensure_ascii=False).encode() + b"\n"
            split = data.index("说".encode()) + 1
            path.write_bytes(data[:split])
            reader = d.LogReader(root)
            self.assertEqual(list(reader.records()), [])
            with self.assertRaises(ValueError):
                reader.finish()
            with path.open("ab") as output:
                output.write(data[split:])
            (root / "bt-sniffer.2.jsonl").write_text(json.dumps(start()) + "\n")
            self.assertEqual([x["fields"]["event"] for x in reader.records()], ["说明", "application_start"])
            reader.finish()
            self.assertEqual(list(reader.records()), [])

    def test_bad_lines_and_truncation_are_errors(self):
        for contents in (b"not json\n", b"[]\n", b'{"fields":{}}\n', b'\xff\n'):
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "bt-sniffer.1.jsonl").write_bytes(contents)
                with self.assertRaises((ValueError, UnicodeError)):
                    list(d.LogReader(root).records())
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bt-sniffer.1.jsonl"
            path.write_text(json.dumps(start()) + "\n")
            reader = d.LogReader(path.parent)
            list(reader.records())
            path.write_text("")
            with self.assertRaises(ValueError):
                list(reader.records())

    def test_versions_run_identity_types_and_missing_start(self):
        cases = [[event("collector_status", sample_hashes=1)],
                 [start(), {"run_id": "two", "fields": {}}],
                 [start(), start()],
                 [event("application_start", **{**d.VERSIONS, "log_contract_version": 1}, concurrency=4)],
                 [start(), event("collector_status", sample_hashes=True)]]
        for events in cases:
            with self.subTest(events=events), self.assertRaises(ValueError):
                d.Observation({}).consume(events)

    def test_observation_window_and_missing_final_evidence(self):
        clock = [0]
        record = {}
        observer = d.Observation(record, lambda: clock[0])
        observer.consume([start()])
        clock[0] = 1799
        self.assertIsNone(observer.stop_reason())
        clock[0] = 1800
        self.assertEqual(observer.stop_reason(), "no_effective_sampling")
        observer.consume([event("collector_status", sample_hashes=1)])
        clock[0] += 2099
        self.assertIsNone(observer.stop_reason())
        clock[0] += 1
        self.assertEqual(observer.stop_reason(), "complete")
        self.assertFalse(observer.final_checks())
        observer.consume(finals())
        self.assertTrue(observer.final_checks())
        observer.consume([event("logging_queue", final_snapshot=True, sink="file", dropped_total=1)])
        self.assertFalse(observer.final_checks())


class ProcessTests(unittest.TestCase):
    def test_sigterm_is_joined_and_early_exit_is_not_passed(self):
        for behavior in ("wait", "exit", "ignore", "interrupt", "bad_log", "wait_signals", "ignore_signals"):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                # FIFO 确认子进程已安装信号处理并写完启动日志，无需固定 sleep 猜测就绪。
                ready = root / "ready"
                os.mkfifo(ready)
                child = root / "child.py"
                child.write_text('''import json, pathlib, signal, sys
root=pathlib.Path.cwd()
(root/'logs').mkdir()
out=(root/'logs/bt-sniffer.1.jsonl').open('w')
def emit(events):
    for event in events: out.write(json.dumps(event)+'\\n')
    out.flush()
def stop(*args):
    emit(FINALS)
    sys.exit(0)
signal.signal(signal.SIGTERM, signal.SIG_IGN if BEHAVIOR.startswith('ignore') else stop)
emit(INITIAL)
with (root/'ready').open('w') as ready: ready.write('ready')
if BEHAVIOR=='exit': sys.exit(0)
while True: signal.pause()
'''.replace("FINALS", repr(finals())).replace("INITIAL", repr([start(), event("collector_status", sample_hashes=1)])).replace("BEHAVIOR", repr(behavior)))
                fake_time = [0]
                first_wait = [True]
                ready_fd = os.open(ready, os.O_RDWR | os.O_NONBLOCK)
                children = []
                spawn = subprocess.Popen
                wait_timeouts = []
                previous_signals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}

                def own_child(*args, **kwargs):
                    child = spawn(*args, **kwargs)
                    children.append(child)
                    if behavior.endswith("_signals"):
                        original_wait = child.wait

                        def wait(timeout=None):
                            # 在真实子进程的退出等待入口注入信号；原实现会从这里抛出并丢失报告。
                            first = not wait_timeouts
                            wait_timeouts.append(timeout)
                            if first:
                                for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGINT):
                                    os.kill(os.getpid(), sig)
                            return original_wait(timeout=timeout)

                        child.wait = wait
                    return child

                def advance(_):
                    if first_wait[0]:
                        self.assertTrue(select.select([ready_fd], [], [], 3)[0], "子进程未就绪")
                        self.assertEqual(os.read(ready_fd, 5), b"ready")
                        first_wait[0] = False
                    if behavior == "interrupt":
                        raise KeyboardInterrupt
                    if behavior == "bad_log":
                        with (root / "logs/bt-sniffer.1.jsonl").open("ab") as output:
                            output.write(b"not json\n")
                    fake_time[0] += 2100

                record = {"observed_for_35m": False}
                try:
                    with patch.object(d.subprocess, "Popen", side_effect=own_child):
                        result = d.supervise([sys.executable, str(child)], root, record,
                                             clock=lambda: fake_time[0], sleep=advance, shutdown_timeout=0.5)
                    self.assertEqual(json.loads((root / "observation.json").read_text()), result)
                    for sig, previous in previous_signals.items():
                        self.assertEqual(signal.getsignal(sig), previous)
                    if behavior.endswith("_signals"):
                        self.assertEqual(wait_timeouts, [0.5], "重复信号不得重新开始退出等待")
                        self.assertTrue(result["interrupted"])
                        self.assertEqual(result["status"], "failed")
                    if behavior == "wait":
                        self.assertEqual(result["status"], "observed", result)
                        self.assertEqual(result["exit_code"], 0)
                    elif behavior == "exit":
                        self.assertEqual(result["status"], "failed")
                    elif behavior in ("interrupt", "bad_log", "wait_signals"):
                        self.assertEqual(result["status"], "failed")
                        self.assertEqual(result["incomplete_reason"], "log_read_failed" if behavior == "bad_log" else "interrupted")
                        self.assertEqual(result["exit_code"], 0)
                        self.assertNotIn("still_running_pid", result)
                    else:
                        self.assertEqual(result["incomplete_reason"], "shutdown_timeout")
                        self.assertIn("still_running_pid", result)
                finally:
                    os.close(ready_fd)
                    for process in children:
                        # 只回收测试拥有的子进程；生产工具超时不强杀。
                        if process.poll() is None:
                            process.kill()
                        process.wait(timeout=3)



class DatabaseTests(unittest.TestCase):
    def fixture(self, root):
        (root / "state").mkdir()
        path = root / "state/state.sqlite3"
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.executescript('''PRAGMA user_version=2;
                CREATE TABLE infohashes(hash BLOB PRIMARY KEY, first_seen INTEGER);
                CREATE TABLE fetch_jobs(hash BLOB, generation INTEGER, state TEXT, due_at INTEGER);
                CREATE INDEX fetch_claim_due ON fetch_jobs(due_at);
                CREATE TABLE metadata(hash BLOB, info BLOB);''')
            connection.execute("INSERT INTO metadata VALUES(?,?)", (hashlib.sha1(b"de").digest(), b"de"))
        e.write_json(root / "observation.json", {"status": "observed", "exit_code": 0, "ended_at": e.utc()})
        return path

    def test_verification_is_read_only_and_detects_corrupt_metadata(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                path = self.fixture(root)
                if corrupt:
                    with closing(sqlite3.connect(path)) as connection, connection:
                        connection.execute("UPDATE metadata SET info=x'00'")
                before = path.read_bytes()
                result = d.verify(root)
                self.assertEqual(path.read_bytes(), before)
                self.assertEqual(result["status"], "failed" if corrupt else "passed")
                self.assertTrue(result["manual_review_required"])

    def test_cli_report_publication_failure_returns_nonzero(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            with patch.object(sys, "argv", ["diagnostics.py", "verify", str(root)]), patch.object(d, "write_json", side_effect=OSError("injected report failure")):
                self.assertEqual(d.main(), 1)
            self.assertFalse((root / "database-verification.json").exists())

    def test_live_process_record_prevents_database_open(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            e.write_json(root / "observation.json", {"still_running_pid": 1, "exit_code": None})
            with patch.object(d.sqlite3, "connect") as connect, self.assertRaises(ValueError):
                d.verify(root)
            connect.assert_not_called()
