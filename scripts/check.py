"""开发检查公开入口：固定范围、顺序执行、独立日志和明确完成状态。"""
import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCOPES = ('rust', 'docs', 'tools', 'examples', 'all')
LABELS = {'passed': '通过', 'failed': '失败', 'blocked': '环境阻塞',
          'interrupted': '中止', 'not_run': '未运行'}


def utc():
    return datetime.now(timezone.utc).isoformat()


def stages(scopes):
    """按固定顺序取并集；全部工具测试已经包含检查工具专项测试。"""
    selected = set(scopes)
    if 'all' in selected:
        selected = set(SCOPES) - {'all'}
    python = sys.executable
    result = [('environment', [python, 'scripts/check_environment.py', *sorted(selected)])]
    if 'rust' in selected:
        result.extend([
            ('loopback', [python, 'scripts/check_loopback.py']),
            ('rust-format', ['cargo', 'fmt', '--all', '--', '--check']),
            ('rust-check', ['cargo', 'check', '--locked', '--workspace', '--all-targets']),
            ('rust-clippy', ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--', '-D', 'warnings']),
            ('rust-tests', ['cargo', 'test', '--locked', '--workspace', '--all-targets']),
        ])
    if 'docs' in selected:
        result.append(('documentation', [python, 'scripts/check_docs.py']))
    if 'tools' in selected:
        result.append(('tool-tests', [python, '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-v']))
    elif 'docs' in selected:
        result.append(('check-tests', [python, '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_checks*.py', '-v']))
    if 'examples' in selected:
        manifest = '.agents/skills/rust-async-patterns/examples/Cargo.toml'
        result.extend([
            ('example-format', ['cargo', 'fmt', '--manifest-path', manifest, '--', '--check']),
            ('example-clippy', ['cargo', 'clippy', '--locked', '--all-targets', '--manifest-path', manifest, '--', '-D', 'warnings']),
            ('example-tests', ['cargo', 'test', '--locked', '--manifest-path', manifest]),
        ])
    return result


def signal_group(process, number):
    """组长退出不代表子孙退出；始终向创建时的整个进程组发信号。"""
    try:
        os.killpg(process.pid, number)
    except ProcessLookupError:
        pass


def stop_group(process, number):
    signal_group(process, number)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        process.poll()  # 回收组长，但继续观察可能仍在运行的组成员。
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    signal_group(process, signal.SIGKILL)
    process.wait()


def run_stage(stage, root, log, interrupted):
    """输出直接写文件；仅父进程保留中止状态，子进程在独立组运行。"""
    started = time.monotonic()
    stage['started_at'] = utc()
    env = dict(os.environ, RUSTUP_AUTO_INSTALL='0')
    if stage['name'].startswith('example-'):
        env['CARGO_TARGET_DIR'] = str(root / 'target/skill-examples')
    process = None
    try:
        with log.open('wb') as output:
            process = subprocess.Popen(stage['command'], cwd=root, env=env,
                                       stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            while True:
                if interrupted[0]:
                    stop_group(process, interrupted[0])
                    stage['status'] = 'interrupted'
                    break
                try:
                    process.wait(timeout=0.1)
                    stage['status'] = 'passed' if process.returncode == 0 else 'failed'
                    # 只有已知的前提检查可以将退出码解释为环境问题。
                    if (stage['name'] == 'environment' and process.returncode == 2
                            or stage['name'] == 'loopback' and process.returncode == 2):
                        stage['status'] = 'blocked'
                    break
                except subprocess.TimeoutExpired:
                    continue
            stage['exit_code'] = process.returncode
    except FileNotFoundError as error:
        stage.update(status='blocked', error=str(error))
    except OSError as error:
        stage.update(status='failed', error=str(error))
    finally:
        if process is not None and process.poll() is None:
            stop_group(process, signal.SIGTERM)
        stage.update(ended_at=utc(), elapsed_seconds=round(time.monotonic() - started, 3))


def tail(path):
    """最多读取末尾 8 KiB；截断 UTF-8 时仅替换残缺字符，完整日志不改写。"""
    with path.open('rb') as source:
        source.seek(0, os.SEEK_END)
        source.seek(max(0, source.tell() - 8192))
        return source.read(8192).decode('utf-8', errors='replace')


def write_report(path, report):
    """独立目录内原子发布一次结果；失败由调用者报告，不能计检查成功。"""
    temporary = path.with_suffix('.tmp')
    try:
        with temporary.open('x', encoding='utf-8') as target:
            json.dump(report, target, ensure_ascii=False, indent=2, allow_nan=False)
            target.write('\n')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def check(scopes, root=ROOT):
    root = root.resolve()
    parent = root / 'target/checks'
    parent.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-'), dir=parent))
    report = {'report_type': 'development-check', 'format_version': 1, 'scopes': scopes,
              'cwd': str(root), 'started_at': utc(), 'status': 'not_run', 'stages': []}
    for index, (name, command) in enumerate(stages(scopes)):
        report['stages'].append({'name': name, 'command': command, 'status': 'not_run',
                                 'exit_code': None, 'log': f'{index:02d}-{name}.log'})
    interrupted = [0]
    previous = {}

    def receive(number, frame):
        if not interrupted[0]:
            interrupted[0] = number

    for number in (signal.SIGINT, signal.SIGTERM):
        previous[number] = signal.signal(number, receive)
    code = 1
    try:
        print(f'检查范围：{", ".join(scopes)}；证据目录：{run}', flush=True)
        for stage in report['stages']:
            if interrupted[0]:
                break
            print(f'执行 {stage["name"]}', flush=True)
            log = run / stage['log']
            run_stage(stage, root, log, interrupted)
            print(f'{stage["name"]}：{LABELS[stage["status"]]}，{stage["elapsed_seconds"]:.3f} 秒', flush=True)
            if stage['status'] != 'passed':
                if stage.get('error'):
                    print(stage['error'], file=sys.stderr)
                if log.exists():
                    print(tail(log), file=sys.stderr, end='\n')
                break
        statuses = [stage['status'] for stage in report['stages']]
        if interrupted[0]:
            report.update(status='interrupted', signal=interrupted[0])
            code = 128 + interrupted[0]
        elif 'blocked' in statuses:
            report['status'], code = 'blocked', 2
        elif all(status == 'passed' for status in statuses):
            report['status'], code = 'passed', 0
        else:
            report['status'], code = 'failed', 1
        report.update(ended_at=utc(), exit_code=code)
        write_report(run / 'result.json', report)
        for stage in report['stages']:
            if stage['status'] == 'not_run':
                print(f'{stage["name"]}：未运行')
        print(f'检查{LABELS[report["status"]]}；结果：{run / "result.json"}', flush=True)
        return code
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def main(args=None):
    parser = argparse.ArgumentParser(description='按范围执行开发检查；可指定多个范围取并集。')
    parser.add_argument('scopes', nargs='*', metavar='范围', help='rust / docs / tools / examples / all')
    arguments = parser.parse_args(args)
    if not arguments.scopes:
        parser.print_help()
        return 0
    if any(scope not in SCOPES for scope in arguments.scopes):
        parser.error('范围只能是 rust、docs、tools、examples 或 all')
    try:
        return check(list(dict.fromkeys(arguments.scopes)))
    except (OSError, ValueError) as error:
        print(f'检查执行或报告保存失败：{error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
