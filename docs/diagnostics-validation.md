# 采集诊断服务器手动复测

本步骤由操作者在服务器显式执行，使用隔离状态目录。目标是验证诊断准确、资源有界，以及调度版本 2 和扩展握手版本 2 的预期行为；不设置吞吐提升门槛，不进行并发 4/8 对比。运行配置使用默认并发 4 和日志契约中定义的网络期限。

## 本地交付检查

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --offline
cargo clippy --locked --workspace --all-targets --offline -- -D warnings
cargo test --locked --workspace --all-targets --offline
```

默认测试使用 loopback 和临时数据库，不启用 ignored。若环境禁止本机 socket，须在允许 loopback 的环境运行，不能把绑定失败当成业务回归。积压成本测试使用实际生产 SQL 的 SQLite VM 指令数，在不扩大已接纳任务集时增加 20,000 个未接纳 hash，避免机器负载干扰耗时断言。本地结果不证明公网性能改善。

本地核对也可使用 `cargo test --locked --workspace`，默认不运行 ignored。多候选、最多 8 个地址和失败切换测试位于 `collection::worker_tests`；单 peer 编解码与期限测试位于 `collection::peer`。诊断回归检查完整握手时长、Task 超时的 Timeout/Task 结果、合并桶后的分位数，以及提示维度汇总一致性。

专项入口包括 `collection::jobs::transitions::tests`（重试边界）、`collection::tcp_limits`（同 IP 许可释放）、`collection::ingest::tests`（分段失败与取消恢复）及 `app::logging::event_tests::dht_and_collection_histogram_contracts_match`（直方图输出）。使用精确名称前通过 `cargo test --locked --workspace -- --list` 核对名称，并确认实际执行数量。

## 固定输入与运行身份

下面使用 Bash、Python 3 和 Cargo；从待验证源码的仓库根目录执行。服务器需有足够磁盘、可用的 IPv4/IPv6 和公网 UDP/TCP。绑定独立临时端口隔离已有实例；若网络不支持某个地址族，应记录环境失败并另行注明实际覆盖范围，不能默默换配置后混用报告。

```bash
export BT_REPO="$PWD"
export BT_RUN="$(mktemp -d /tmp/bt-sniffer-diagnostics-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX)"
git rev-parse HEAD > "$BT_RUN/git-head.txt"
git status --porcelain=v1 > "$BT_RUN/git-status.txt"
git diff HEAD --binary > "$BT_RUN/source.diff"
rustc -Vv > "$BT_RUN/rustc.txt"
cargo -V > "$BT_RUN/cargo.txt"
date -u +%FT%TZ > "$BT_RUN/prepared-at.txt"
python3 - <<'PY'
import hashlib, json, os, pathlib, subprocess
repo = pathlib.Path(os.environ['BT_REPO'])
names = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=repo).split(b'\0')
manifest = {}
for name in names:
    if not name:
        continue
    path = repo / os.fsdecode(name)
    if path.is_file():
        manifest[os.fsdecode(name)] = hashlib.sha256(path.read_bytes()).hexdigest()
encoded = json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2).encode()
run = pathlib.Path(os.environ['BT_RUN'])
(run / 'source-manifest.json').write_bytes(encoded)
(run / 'source-manifest.sha256').write_text(hashlib.sha256(encoded).hexdigest() + '\n')
PY
cargo build --release --locked > "$BT_RUN/build.log" 2>&1
# 仅在构建成功、期间源码未变动后执行后续步骤。
cp target/release/bt-sniffer "$BT_RUN/bt-sniffer"
sha256sum "$BT_RUN/bt-sniffer" > "$BT_RUN/binary.sha256"
```

源码清单包含实际工作区文件，Git HEAD 不能代替未提交源码指纹。构建期间若源码发生变化，重新准备并构建。记录完整命令、二进制、清单与 UTC 时间；保留原目录中的日志、数据库和历史报告，不用它们作为新运行的输出目录。

## 有效采样后至少运行 35 分钟

启动后核对 `application_start`：`log_contract_version=3`，`scheduling_policy_version=2`、`extension_handshake_policy_version=2`、`admission_policy_version=2`；数据库 schema 为 2。并发、候选数、毫秒期限与字节上限从具名字段读取，并与运行命令及[当前日志契约](logging.md#日志契约版本-3)对照。使用真实模块 target 和 event 筛选，任务成本看 attempt 系列，完整握手看 `peer_handshake_diagnostic`。

在同一终端执行下面脚本。它保存完整命令及退出状态；并发明确为 4，保留默认 freshness、网络期限及 DHT 配额。日志目录是隔离工作目录下的 logs，状态目录为该目录的 state。脚本显式设置默认 RUST_LOG，逐个完整 JSONL 行校验；半行等待追加，完整坏行报告失败并进入收尾。

脚本以首次**观察到日志中** `sample_hashes > 0` 的时刻开始 35 分钟计时，记录对应日志行和 UTC。运行 30 分钟仍未观察到有效采样时发送 SIGTERM，并标记未完成有效采样验收；这只是外部观察上限，不修改程序期限。有效采样前的等待不计入 35 分钟。提前退出、手动中断、关闭超时均不能标为通过。

```bash
python3 - <<'PY'
import datetime, json, os, pathlib, signal, subprocess, time
run = pathlib.Path(os.environ['BT_RUN'])
command = [str(run / 'bt-sniffer'), '--state-dir', str(run / 'state'),
           '--sample', '--fetch', '--fetch-concurrency', '4',
           '--listen-v4', '0.0.0.0:0', '--listen-v6', '[::]:0']
def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
environment = os.environ.copy()
environment['RUST_LOG'] = 'warn,bt_sniffer=info'
record = {'command': command, 'cwd': str(run), 'log_filter': environment['RUST_LOG'], 'started_at': utc(),
          'first_sample_observed_at': None, 'observed_for_35m': False}
(run / 'command.json').write_text(json.dumps(record, indent=2))
# 按文件保存字节偏移和未完成末行；跨日时从新文件开始读取。
positions = {}
pending = {}
def read_records(directory):
    for path in sorted(directory.glob('bt-sniffer.*.jsonl')):
        offset = positions.get(path, 0)
        with path.open('rb') as source:
            source.seek(offset)
            chunk = source.read()
            positions[path] = source.tell()
        lines = (pending.get(path, b'') + chunk).split(b'\n')
        pending[path] = lines.pop()
        for line in lines:
            try:
                event = json.loads(line)
                if not isinstance(event, dict) or not isinstance(event.get('run_id'), str) or not isinstance(event.get('fields'), dict):
                    raise ValueError('日志对象缺少 run_id 或 fields')
            except (ValueError, UnicodeError) as error:
                raise ValueError(f'{path}: 完整 JSONL 行无效：{error}') from error
            yield event

def observe_events(events, record):
    found_sample = False
    for event in events:
        fields = event['fields']
        if fields.get('event') == 'application_start':
            if fields.get('log_contract_version') != 3:
                raise ValueError('不支持的日志契约版本')
            if record.get('run_id') not in (None, event['run_id']):
                raise ValueError('验收目录包含多次运行，须使用新目录')
            record['run_id'] = event['run_id']
        if event['run_id'] != record.get('run_id'):
            continue
        if fields.get('event') == 'collector_status':
            count = fields.get('sample_hashes')
            if type(count) is not int or count < 0:
                raise ValueError('sample_hashes 必须是非负整数')
            if count > 0 and record['first_sample_observed_at'] is None:
                record['first_sample_observed_at'] = utc()
                record['first_sample_log'] = event
                found_sample = True
    return found_sample

with (run / 'stdout.log').open('w') as stdout, (run / 'stderr.log').open('w') as stderr:
    process = subprocess.Popen(command, cwd=run, env=environment, stdout=stdout, stderr=stderr)
    started = time.monotonic()
    first_sample = None
    try:
        while process.poll() is None:
            # 即使已经发现样本，也继续校验后续完整行；不把坏行当作缺失样本。
            events = list(read_records(run / 'logs'))
            if observe_events(events, record):
                first_sample = time.monotonic()
            if first_sample is not None and time.monotonic() - first_sample >= 35 * 60:
                record['observed_for_35m'] = True
                break
            if first_sample is None and time.monotonic() - started >= 30 * 60:
                record['incomplete_reason'] = 'no_effective_sampling'
                break
            time.sleep(1)
    except KeyboardInterrupt:
        record['incomplete_reason'] = 'interrupted'
    except (ValueError, OSError) as error:
        record['incomplete_reason'] = 'log_read_failed'
        record['log_error'] = str(error)
    finally:
        if process.poll() is None:
            record['sigterm_at'] = utc()
            process.send_signal(signal.SIGTERM)
        try:
            record['exit_code'] = process.wait(timeout=40)
        except subprocess.TimeoutExpired:
            record['incomplete_reason'] = 'shutdown_timeout'
            # 不自动 SIGKILL，不对仍运行的数据库使用 immutable；交给操作者排查。
            record['still_running_pid'] = process.pid
        if 'still_running_pid' not in record:
            try:
                observe_events(list(read_records(run / 'logs')), record)
                if any(pending.values()):
                    raise ValueError('进程退出后存在未完成 JSONL 末行')
            except (ValueError, OSError) as error:
                record['incomplete_reason'] = 'log_read_failed'
                record['log_error'] = str(error)
        record['ended_at'] = utc()
        record['runtime_seconds'] = time.monotonic() - started
        (run / 'observation.json').write_text(json.dumps(record, indent=2))
print(json.dumps(record, indent=2))
PY
```

检查 observation.json 没有 still_running_pid、incomplete_reason 或 log_error，退出码为 0，日志有 `fields.event=session_shutdown` 且 `fields.success=true`、最终 `fields.event=collector_summary` 且 `fields.running_workers=0`、累计和区间尾段。外部超时结束或 stderr 中无报错都不能替代正常收尾证据。stdout 应为空。只有实际等待完成 35 分钟、正常关闭且后续复核成功，才完成该次运行验收。

## 退出后只读数据库复核

确认进程已经退出后执行。不清理 WAL/SHM，不把仍运行的库当作 immutable。下面用 `mode=ro` 读取最终数据库；时间固定为运行记录的 ended_at，同时在报告中保留复核时间。查询范围和生产 `first_attempt_backlog` 相同。

```bash
python3 - <<'PY'
import datetime, hashlib, json, os, pathlib, sqlite3
run = pathlib.Path(os.environ['BT_RUN'])
observation = json.loads((run / 'observation.json').read_text())
assert 'still_running_pid' not in observation, '先确认进程已退出'
assert observation.get('exit_code') is not None, '缺少进程退出确认'
now_ms = int(datetime.datetime.fromisoformat(observation['ended_at']).timestamp() * 1000)
connection = sqlite3.connect((run / 'state/state.sqlite3').as_uri() + '?mode=ro', uri=True)
connection.execute('PRAGMA query_only=ON')
integrity = [row[0] for row in connection.execute('PRAGMA integrity_check')]
foreign_keys = connection.execute('PRAGMA foreign_key_check').fetchall()
states = dict(connection.execute('SELECT state, count(*) FROM fetch_jobs GROUP BY state'))
backlog = connection.execute('''
    SELECT count(*), coalesce(sum(i.first_seen < ?1 - 1800000), 0),
           CASE WHEN count(*)=0 THEN NULL ELSE max(0, ?1-min(i.first_seen)) END,
           CASE WHEN count(*)=0 THEN NULL ELSE max(0, ?1-min(j.due_at)) END,
           coalesce(sum(j.due_at > ?1), 0)
    FROM fetch_jobs j INDEXED BY fetch_claim_due
    JOIN infohashes i ON i.hash=j.hash
    WHERE j.generation=0 AND j.state IN ('pending','retry_wait')
''', (now_ms,)).fetchone()
metadata_count = 0
sha1_failures = 0
for expected, info in connection.execute('SELECT hash, info FROM metadata'):
    metadata_count += 1
    sha1_failures += hashlib.sha1(info).digest() != expected
connection.close()
result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'snapshot_time_ms': now_ms, 'integrity_check': integrity,
          'foreign_key_violation_count': len(foreign_keys), 'states': states,
          'metadata_count': metadata_count, 'sha1_failure_count': sha1_failures,
          'first_attempt_backlog': dict(zip(
              ['waiting', 'older_than_30m', 'oldest_discovery_age_ms', 'oldest_due_wait_ms', 'not_due'], backlog))}
(run / 'database-verification.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
assert integrity == ['ok'] and not foreign_keys and not sha1_failures
assert states.get('running', 0) == 0
PY
```

## 报告填写与判断

按 application_start 的顶层 run_id 筛选 **JSONL 文件日志一份**，业务字段读取 fields 对象，不要把 stderr 副本再相加，也不要把每分钟累计值求和。最后一个 `scope=total final_snapshot=true` 是本次累计；区间和尾段可交叉验证。字段完整性仍受日志丢弃边界影响，缺失必须明示，不补猜。完整字段说明见 [日志契约](logging.md#bencode-样本与领取历史诊断)。

报告至少包含：

| 项目 | 必填证据 |
| --- | --- |
| 运行身份 | 源码清单及 SHA-256、Git 状态、二进制 SHA-256、工具链、完整命令、有效配置、run_id、开始/首次有效采样/SIGTERM/退出 UTC |
| Bencode | 按阶段、来源、地址族、固定 reason 摘录样本和 truncated；对应 `peer_failure_detail` 原聚合频数；`bencode_sample_summary` emitted/suppressed。没有目标错误时写“未复现”，不能写解析已修复 |
| First/Repeat | 分别列 claims、executions、downloaded、committed、execution_sum_ms/1000；执行秒数除以 committed 得每次提交成本，零提交为缺失值；再按 failed_attempts_before 和 RemoteFailure.reason 分组列数及耗时 |
| 积压和暂停 | admission_status 的 Q、高低水位；first_attempt_backlog 的等待数、超龄数、最老年龄；sampler_diagnostic、sampling_backpressure 与暂停/恢复事件的时间线。分钟快照与精确事件时间分开，缺事件不猜时间 |
| 收尾 | 35 分钟有效观察、退出码 0、session_shutdown、running_workers=0、数据库 running=0、integrity、外键、逐份 metadata SHA-1；最终未首试积压用只读复核补足 |
| 局限 | 未复现的错误、缺失日志、未验证地址族、无提交类别和任何未满足验收项 |

First/Repeat 是领取历史，不是“第几次 TCP”；最终任务失败不涵盖全部 peer 失败；Downloaded 不能替代 Committed。分钟 Q 与全部未首试积压口径不同，超龄任务离开 Q 后仍出现在未首试积压快照中。无样本不能推断没有错误，应同时看原频数与 suppressed。

判断目标是证据可信、行为保持和资源有界，不要求吞吐改善。并发对比、退避调整和解析兼容性修改须另行制定计划。验证输出保存在隔离运行目录，历史运行报告及原 state 目录保持只读。

诊断交叉核对：记录键序样本的三个可选检查字段，不能将不完整检查中的 false 解释为不存在问题；按 claim_class 汇总联合统计，验证与 Attempt 聚合一致；汇总 connect_history_diagnostic 中重复失败的连接数与耗时，同时报告历史容量丢弃数。端点历史只有内存 TTL 范围内的覆盖，不据此统计独立 peer 总数。最终数据库复核同时展示最老发现年龄、最老到期等待和未到期数，两个最大值不能相减。这些字段的完整契约见 [诊断字段](logging.md#键序领取与连接历史诊断)。

按提示维度汇总 `attempt_hint_summary` 应回到 `attempt_summary`；带提示 Repeat 使用该汇总，不查 Hint × Repeat。总体成本使用最后累计值，零提交为缺失值。若进行独立版本比较，每个版本分别保存二进制和源码指纹；调度与兼容同时变化时不能将结果归因于单项修改。

兼容结果列出 attempted_frames、accepted_frames、rejected_frames、sessions、downloaded、committed，核对 attempted=accepted+rejected、各 interval 之和等于最终 total。未触发兼容注明未复现，不能用 InvalidDictionary 减少量代替提交收益。保留最终拒绝样本、原失败频数和 suppressed；严格 metadata 校验、数据库完整性、外键和原始 info SHA-1 都须复核。缺少完成时间、退出尾段或提交确认时记录缺口，不补猜计数。

## 历史验证记录

接口与状态收敛、日志契约清理的本地结果见[历史验证记录摘录](reports/extracted-validation-records.md)。这些记录不代表本次运行结果，也不能替代服务器观察。
