"""隔离的服务器复测：prepare 构建，observe 监督进程，verify 只读核对。

仅 observe 会启动公共 DHT。导入模块、帮助和默认工具测试不启动采集。
"""
import argparse
import datetime
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from evidence import artifact_identity, command_text, source_identity, utc, write_json

REPO = Path(__file__).resolve().parents[1]
VERSIONS = {"log_contract_version": 3, "scheduling_policy_version": 2,
            "extension_handshake_policy_version": 2, "admission_policy_version": 2}


def prepare(repo, run):
    """独立 target 目录消除陈旧产物歧义；源码前后不一致时不发布准备记录。"""
    before = source_identity(repo)
    identity = {"report_version": 2, **before, "prepared_at": utc(),
                "git_head": command_text(repo, ["git", "rev-parse", "HEAD"]).strip(),
                "git_status": command_text(repo, ["git", "status", "--porcelain=v1"]),
                "toolchain": command_text(repo, ["rustc", "-Vv"]),
                "cargo": command_text(repo, ["cargo", "-V"])}
    (run / "source.diff").write_bytes(subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=repo))
    with tempfile.TemporaryDirectory(prefix="bt-sniffer-build-") as target:
        command = ["cargo", "build", "--release", "--locked", "--bin", "bt-sniffer", "--target-dir", target]
        identity["command"] = command
        with (run / "build.log").open("xb") as output:
            subprocess.run(command, cwd=repo, stdout=output, stderr=subprocess.STDOUT, check=True)
        if source_identity(repo) != before:
            raise ValueError("构建期间源码变化，须重新准备")
        # 已选定 Linux 服务器工具，不推断其他目标的文件后缀或目录布局。
        binary = run / "bt-sniffer"
        with (Path(target) / "release/bt-sniffer").open("rb") as source, binary.open("xb") as destination:
            import shutil
            shutil.copyfileobj(source, destination)
        binary.chmod(0o700)
        identity["artifact"] = artifact_identity(binary, "application")
    identity["build_source_consistency"] = "checked_before_and_after_build"
    write_json(run / "preparation.json", identity)
    return identity


class LogReader:
    """有界读取完整 JSONL 行；半行跨 poll 保留，不吞掉坏行或文件截断。"""
    def __init__(self, directory):
        self.directory = directory
        self.positions = {}
        self.pending = {}

    def records(self):
        for path in sorted(self.directory.glob("bt-sniffer.*.jsonl")):
            offset = self.positions.get(path, 0)
            if path.stat().st_size < offset:
                raise ValueError(f"日志被截断：{path}")
            with path.open("rb") as source:
                source.seek(offset)
                while chunk := source.read(65536):
                    self.positions[path] = source.tell()
                    lines = (self.pending.get(path, b"") + chunk).split(b"\n")
                    self.pending[path] = lines.pop()
                    if len(self.pending[path]) > 1024 * 1024:
                        raise ValueError("JSONL 单行超过 1 MiB 消费预算")
                    for line in lines:
                        if len(line) > 1024 * 1024:
                            raise ValueError("JSONL 单行超过 1 MiB 消费预算")
                        event = json.loads(line)
                        if not isinstance(event, dict) or not isinstance(event.get("run_id"), str) or not event["run_id"] or not isinstance(event.get("fields"), dict):
                            raise ValueError("日志对象缺少 run_id 或 fields")
                        yield event

    def finish(self):
        if any(self.pending.values()):
            raise ValueError("进程退出后存在未完成 JSONL 末行")


def unsigned(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} 必须是非负整数")
    return value


class Observation:
    """只保存固定的验收状态和首条采样证据，不积累全部日志。"""
    def __init__(self, record, clock=time.monotonic):
        self.record = record
        self.clock = clock
        self.started = clock()
        self.first_sample = None
        self.run_id = None
        self.session_closed = False
        self.workers_closed = False
        self.final_scopes = set()
        self.queues = {}

    def consume(self, events):
        for event in events:
            fields = event["fields"]
            name = fields.get("event")
            if self.run_id is None:
                self.run_id = event["run_id"]
            if event["run_id"] != self.run_id:
                raise ValueError("验收目录包含多个 run_id")
            if name == "application_start":
                if "application_start" in self.record:
                    raise ValueError("重复 application_start")
                if any(type(fields.get(key)) is not int or fields[key] != version for key, version in VERSIONS.items()):
                    raise ValueError("日志或业务策略版本不匹配")
                if type(fields.get("concurrency")) is not int or fields["concurrency"] != 4:
                    raise ValueError("服务器复测要求并发 4")
                self.record["application_start"] = event
                self.record["run_id"] = self.run_id
            elif name == "collector_status":
                count = unsigned(fields.get("sample_hashes"), "sample_hashes")
                if count and self.first_sample is None:
                    if "application_start" not in self.record:
                        raise ValueError("有效采样之前缺少 application_start")
                    self.first_sample = self.clock()
                    self.record["first_sample_observed_at"] = utc()
                    self.record["first_sample_log"] = event
            elif name == "session_shutdown":
                if fields.get("success") is not True:
                    raise ValueError("会话关闭失败")
                self.session_closed = True
            elif name == "collector_summary" and fields.get("final_snapshot") is True:
                self.workers_closed = unsigned(fields.get("running_workers"), "running_workers") == 0
            elif name == "attempt_summary" and fields.get("final_snapshot") is True:
                self.final_scopes.add(fields.get("scope"))
            elif name == "logging_queue" and fields.get("final_snapshot") is True:
                sink = fields.get("sink")
                if sink not in ("file", "stderr"):
                    raise ValueError("未知日志输出端")
                self.queues[sink] = unsigned(fields.get("dropped_total"), "dropped_total")

    def stop_reason(self):
        if self.first_sample is not None:
            if self.clock() - self.first_sample >= 35 * 60:
                self.record["observed_for_35m"] = True
                return "complete"
        elif self.clock() - self.started >= 30 * 60:
            return "no_effective_sampling"
        return None

    def final_checks(self):
        checks = {"application_start": "application_start" in self.record,
                  "session_shutdown": self.session_closed,
                  "running_workers_zero": self.workers_closed,
                  "attempt_total_and_interval": {"total", "interval"} <= self.final_scopes,
                  "queue_observations": set(self.queues) == {"file", "stderr"},
                  "no_observed_drops": bool(self.queues) and not any(self.queues.values())}
        self.record["checks"] = checks
        self.record["logging_dropped_total"] = self.queues
        return all(checks.values())


def supervise(command, run, record, *, clock=time.monotonic, sleep=time.sleep, shutdown_timeout=40):
    """信号只记录停止请求；收尾和报告发布完成后恢复调用者的信号处理。"""
    def interrupted(signum, frame):
        record["interrupted"] = True
        record.setdefault("incomplete_reason", "interrupted")
        record["status"] = "failed"

    previous = {}
    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, interrupted)
        return supervise_process(command, run, record, clock=clock, sleep=sleep,
                                 shutdown_timeout=shutdown_timeout)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def supervise_process(command, run, record, *, clock, sleep, shutdown_timeout):
    """持有子进程至 wait 确认；超时保留 PID，调用者负责后续人工处理。"""
    observer = Observation(record, clock)
    reader = LogReader(run / "logs")
    env = dict(os.environ, RUST_LOG="warn,bt_sniffer=info")
    process = None
    try:
        with (run / "stdout.log").open("xb") as stdout, (run / "stderr.log").open("xb") as stderr:
            process = subprocess.Popen(command, cwd=run, env=env, stdout=stdout, stderr=stderr)
            record["pid"] = process.pid
            try:
                while process.poll() is None:
                    if record.get("interrupted"):
                        break
                    observer.consume(reader.records())
                    reason = observer.stop_reason()
                    if reason:
                        if reason != "complete":
                            record["incomplete_reason"] = reason
                        break
                    sleep(1)
            except KeyboardInterrupt:
                record["incomplete_reason"] = "interrupted"
            except (ValueError, OSError) as error:
                record["incomplete_reason"] = "log_read_failed"
                record["error"] = str(error)
            finally:
                if process.poll() is None:
                    record["sigterm_at"] = utc()
                    try:
                        process.send_signal(signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                try:
                    record["exit_code"] = process.wait(timeout=shutdown_timeout)
                except subprocess.TimeoutExpired:
                    record["incomplete_reason"] = "shutdown_timeout"
                    record["still_running_pid"] = process.pid
                if "still_running_pid" not in record:
                    try:
                        observer.consume(reader.records())
                        reader.finish()
                    except (ValueError, OSError) as error:
                        record["incomplete_reason"] = "log_read_failed"
                        record["error"] = str(error)
    except OSError as error:
        record["incomplete_reason"] = "process_start_failed"
        record["error"] = str(error)
    record["ended_at"] = utc()
    record["runtime_seconds"] = clock() - observer.started
    complete = observer.final_checks()
    record["checks"]["stdout_empty"] = (run / "stdout.log").is_file() and (run / "stdout.log").stat().st_size == 0
    record["status"] = "observed" if (complete and record.get("observed_for_35m") is True
        and record.get("exit_code") == 0 and "incomplete_reason" not in record
        and record["checks"]["stdout_empty"]) else "failed"
    write_json(run / "observation.json", record)
    return record


def observe(run):
    prepared = json.loads((run / "preparation.json").read_text())
    binary = run / "bt-sniffer"
    artifact = artifact_identity(binary, "application")
    if artifact["sha256"] != prepared["artifact"]["sha256"]:
        raise ValueError("准备后的二进制已变化")
    command = [str(binary), "--state-dir", str(run / "state"), "--sample", "--fetch",
               "--fetch-concurrency", "4", "--listen-v4", "0.0.0.0:0", "--listen-v6", "[::]:0"]
    record = {"report_version": 2, "command": command, "cwd": str(run), "artifact": artifact,
              "log_filter": "warn,bt_sniffer=info", "started_at": utc(), "observed_for_35m": False}
    # 排他占用单次运行目录，避免重复 observe 混入旧日志或启动第二个进程。
    if any((run / name).exists() for name in ("logs", "state", "stdout.log", "observation.json")):
        raise ValueError("运行目录已有状态或日志，须重新 prepare")
    write_json(run / "command.json", record)
    return supervise(command, run, record)


def verify(run):
    observation = json.loads((run / "observation.json").read_text())
    if "still_running_pid" in observation or observation.get("exit_code") is None:
        raise ValueError("缺少进程退出确认，禁止复核数据库")
    now_ms = int(datetime.datetime.fromisoformat(observation["ended_at"]).timestamp() * 1000)
    connection = sqlite3.connect((run / "state/state.sqlite3").as_uri() + "?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        schema = connection.execute("PRAGMA user_version").fetchone()[0]
        states = dict(connection.execute("SELECT state, count(*) FROM fetch_jobs GROUP BY state"))
        backlog = connection.execute('''
            SELECT count(*), coalesce(sum(i.first_seen < ?1 - 1800000), 0),
                   CASE WHEN count(*)=0 THEN NULL ELSE max(0, ?1-min(i.first_seen)) END,
                   CASE WHEN count(*)=0 THEN NULL ELSE max(0, ?1-min(j.due_at)) END,
                   coalesce(sum(j.due_at > ?1), 0)
            FROM fetch_jobs j INDEXED BY fetch_claim_due
            JOIN infohashes i ON i.hash=j.hash
            WHERE j.generation=0 AND j.state IN ('pending','retry_wait')
        ''', (now_ms,)).fetchone()
        count = failures = 0
        for expected, info in connection.execute("SELECT hash, info FROM metadata"):
            count += 1
            failures += hashlib.sha1(info).digest() != expected
        catalog_count = connection.execute("SELECT count(*) FROM torrent_catalog").fetchone()[0]
        fts_count = connection.execute("SELECT count(*) FROM torrent_catalog_fts").fetchone()[0]
        indexed, catalog_total = connection.execute(
            "SELECT indexed,total FROM torrent_catalog_state WHERE singleton=1").fetchone()
        missing_catalog = connection.execute('''
            SELECT count(*) FROM metadata m
            WHERE NOT EXISTS(SELECT 1 FROM torrent_catalog c WHERE c.hash=m.hash)
        ''').fetchone()[0]
    finally:
        connection.close()
    catalog_consistent = (catalog_total == count and indexed == catalog_count == fts_count
                          and missing_catalog == count - catalog_count)
    passed = (observation.get("status") == "observed" and integrity == ["ok"]
              and not foreign_keys and not failures and states.get("running", 0) == 0
              and schema == 3 and catalog_consistent and indexed == catalog_total)
    result = {"report_version": 2, "status": "passed" if passed else "failed",
              "scope": "automated_shutdown_and_database_checks", "manual_review_required": True,
              "checked_at": utc(), "snapshot_time_ms": now_ms, "integrity_check": integrity,
              "schema_version": schema, "foreign_key_violation_count": len(foreign_keys),
              "states": states, "metadata_count": count, "sha1_failure_count": failures,
              "catalog": {"count": catalog_count, "fts_count": fts_count,
                          "indexed": indexed, "total": catalog_total,
                          "missing": missing_catalog, "consistent": catalog_consistent},
              "first_attempt_backlog": dict(zip(["waiting", "older_than_30m", "oldest_discovery_age_ms", "oldest_due_wait_ms", "not_due"], backlog))}
    write_json(run / "database-verification.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("prepare", help="构建并输出新的隔离目录路径")
    for name in ("observe", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("run", type=Path)
    args = parser.parse_args()
    try:
        if args.action == "prepare":
            run = Path(tempfile.mkdtemp(prefix="bt-sniffer-diagnostics-"))
            print(f"准备目录：{run}", file=sys.stderr)
            prepare(REPO, run)
            print(run)
        else:
            result = (observe if args.action == "observe" else verify)(args.run.resolve())
            print(json.dumps(result, ensure_ascii=False, indent=2))
            if result["status"] not in ("observed", "passed"):
                return 1
    except (OSError, ValueError, subprocess.SubprocessError, sqlite3.Error, KeyError, KeyboardInterrupt) as error:
        print(f"验收工具失败：{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
