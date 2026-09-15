"""公开入口的执行证据：受控子命令、失败分类、去重与进程组收尾。"""
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import check as runner
import check_environment as environment


class RunnerTests(unittest.TestCase):
    def fixture(self, root):
        (root / 'scripts/tests').mkdir(parents=True)
        for name in ('check.py', 'check_environment.py', 'check_docs.py', 'check_loopback.py'):
            shutil.copyfile(ROOT / 'scripts' / name, root / 'scripts' / name)
        shutil.copyfile(ROOT / 'rust-toolchain.toml', root / 'rust-toolchain.toml')
        (root / 'scripts/tests/test_fixture.py').write_text(
            'import unittest\nclass Example(unittest.TestCase):\n    def test_example(self):\n        self.assertEqual(1, 1)\n')
        return [sys.executable, str(root / 'scripts/check.py')]

    def report(self, root):
        paths = list((root / 'target/checks').glob('*/result.json'))
        self.assertEqual(len(paths), 1)
        return paths[0], json.loads(paths[0].read_text())

    def test_help_and_invalid_arguments_do_not_start_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = self.fixture(root)
            for args, code in (([], 0), (['--help'], 0), (['unknown'], 2), (['--shell', 'true'], 2)):
                result = subprocess.run(command + args, cwd='/tmp', capture_output=True)
                self.assertEqual(result.returncode, code)
                self.assertFalse((root / 'target/checks').exists())

    def test_real_entrypoint_from_outside_preserves_inputs_and_records_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = self.fixture(root)
            before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
            result = subprocess.run(command + ['tools', 'tools'], cwd='/tmp', capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            path, report = self.report(root)
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(report['scopes'], ['tools'])
            self.assertEqual([s['name'] for s in report['stages']], ['environment', 'tool-tests'])
            for stage in report['stages']:
                self.assertEqual(stage['exit_code'], 0)
                self.assertGreaterEqual(stage['elapsed_seconds'], 0)
                self.assertTrue((path.parent / stage['log']).is_file())
                self.assertTrue(stage['command'])
            self.assertEqual({p: p.read_bytes() for p in before}, before)

    def test_scope_union_runs_shared_checks_once(self):
        all_stages = runner.stages(['all'])
        self.assertEqual(all_stages, runner.stages(['web', 'examples', 'tools', 'docs', 'rust', 'docs']))
        names = [name for name, _ in all_stages]
        self.assertEqual(len(names), len(set(names)))
        self.assertIn('tool-tests', names)
        self.assertNotIn('check-tests', names)
        self.assertIn('check-tests', [name for name, _ in runner.stages(['docs'])])
        self.assertLess(names.index('loopback'), names.index('rust-tests'))
        self.assertIn('example-tests', names)

    def test_missing_tool_and_wrong_version_are_environment_blocks(self):
        for problem in ('missing', 'version'):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                command = self.fixture(root)
                binary = root / 'bin'
                binary.mkdir()
                if problem == 'version':
                    for name, output in [('git', ''), ('cargo', ''), ('rustc', 'rustc 0.0.0')]:
                        tool = binary / name
                        tool.write_text(f'#!{sys.executable}\nprint({output!r})\n')
                        tool.chmod(0o700)
                result = subprocess.run(command + ['rust'], env=dict(os.environ, PATH=str(binary)),
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                _, report = self.report(root)
                self.assertEqual(report['status'], 'blocked')
                self.assertEqual(report['stages'][0]['status'], 'blocked')
                self.assertTrue(all(s['status'] == 'not_run' for s in report['stages'][1:]))

    def test_failed_command_does_not_run_later_stage_or_become_environment_block(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = self.fixture(root)
            # 仅替代外部工具，入口、范围选择、报告和失败判定均走真实实现。
            binary = root / 'bin'
            binary.mkdir()
            for name, body in {
                'git': 'pass', 'rustc': 'print("rustc 1.98.1 (fixture)")',
                'cargo': 'import sys\nprint("受控命令失败")\nsys.exit(0 if "--version" in sys.argv else 17)',
            }.items():
                tool = binary / name
                tool.write_text(f'#!{sys.executable}\n{body}\n')
                tool.chmod(0o700)
            # 与宿主 socket 权限无关；loopback 自身另有真实检查。
            (root / 'scripts/check_loopback.py').write_text('print("fixture loopback")\n')
            result = subprocess.run(command + ['rust'], env=dict(os.environ, PATH=str(binary)),
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            _, report = self.report(root)
            failed = report['stages'][2]
            self.assertEqual((failed['name'], failed['status'], failed['exit_code']), ('rust-format', 'failed', 17))
            self.assertTrue(all(s['status'] == 'not_run' for s in report['stages'][3:]))

    def test_loopback_failure_is_blocked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            (root / 'scripts/check_loopback.py').write_text('raise SystemExit(2)\n')
            stage = {'name': 'loopback', 'command': [sys.executable, 'scripts/check_loopback.py']}
            runner.run_stage(stage, root, root / 'loopback.log', [0])
            self.assertEqual(stage['status'], 'blocked')

    def test_large_chinese_output_is_complete_in_log_and_bounded_in_terminal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = self.fixture(root)
            (root / 'scripts/tests/test_fixture.py').write_text(
                'import unittest\nclass Example(unittest.TestCase):\n'
                '    def test_failure(self):\n        print("中文输出" * 100000)\n        self.fail("预期失败")\n')
            result = subprocess.run(command + ['tools'], capture_output=True)
            self.assertEqual(result.returncode, 1)
            path, report = self.report(root)
            log = path.parent / report['stages'][-1]['log']
            self.assertIn(('中文输出' * 100000).encode(), log.read_bytes())
            self.assertLess(len(result.stdout) + len(result.stderr), 11000)
            self.assertIn('预期失败'.encode(), result.stderr)

    def test_report_failure_is_not_success_and_does_not_publish_partial_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(runner, 'check', side_effect=OSError('disk full')):
                self.assertEqual(runner.main(['tools']), 1)
            target = root / 'result.json'
            with self.assertRaises(ValueError):
                runner.write_report(target, {'bad': float('nan')})
            self.assertFalse(target.exists())
            self.assertFalse(target.with_suffix('.tmp').exists())
            with patch.object(runner.os, 'replace', side_effect=OSError('disk full')):
                with self.assertRaises(OSError), contextlib.redirect_stdout(io.StringIO()):
                    runner.check(['tools'], root)
            self.assertEqual(list((root / 'target/checks').glob('*/result.json')), [])

    def test_missing_rust_component_is_identified_before_cargo_checks(self):
        with patch.object(environment.shutil, 'which', return_value='/fixture/tool'), \
                patch.object(environment.subprocess, 'check_output', side_effect=['rustc 1.98.1', 'clippy-x86_64-unknown-linux-gnu\n']), \
                patch.object(environment.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'rustfmt'):
                environment.check(['rust'])
            run.assert_not_called()

    def test_environment_unknown_command_failure_stays_failure(self):
        with patch.object(environment, 'check', side_effect=subprocess.CalledProcessError(19, ['node', '--version'])):
            self.assertEqual(environment.main(), 1)

    def test_signal_is_forwarded_and_stubborn_group_is_killed(self):
        # 子进程忽略信号；组长先退出，入口仍需清理余下组成员。
        for number in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=number), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                command = self.fixture(root)
                child = ('import os,signal,time; from pathlib import Path; '
                         'signal.signal(signal.SIGINT, signal.SIG_IGN); '
                         'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                         'Path("child.pid").write_text(str(os.getpid())); time.sleep(60)')
                (root / 'scripts/tests/test_fixture.py').write_text(
                    'import subprocess,sys,time,unittest,signal\nfrom pathlib import Path\n'
                    'def stop(number, frame):\n    Path("forwarded.signal").write_text(str(number))\n    raise SystemExit(0)\n'
                    'signal.signal(signal.SIGINT, stop)\nsignal.signal(signal.SIGTERM, stop)\n'
                    'class Waiting(unittest.TestCase):\n'
                    f'    def test_wait(self):\n        subprocess.Popen([sys.executable, "-c", {child!r}])\n'
                    '        time.sleep(60)\n')
                process = subprocess.Popen(command + ['tools'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                child_pid = None
                try:
                    deadline = time.monotonic() + 10
                    while not (root / 'child.pid').exists() and time.monotonic() < deadline:
                        if process.poll() is not None:
                            self.fail('检查进程在受控子进程就绪前退出')
                        time.sleep(0.02)
                    self.assertTrue((root / 'child.pid').exists())
                    child_pid = int((root / 'child.pid').read_text())
                    process.send_signal(number)
                    stdout, stderr = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 128 + number, (stdout + stderr).decode())
                    _, report = self.report(root)
                    self.assertEqual(int((root / 'forwarded.signal').read_text()), number)
                    self.assertEqual(report['status'], 'interrupted')
                    self.assertEqual(report['stages'][-1]['status'], 'interrupted')
                    # Linux 的孤儿可能等待 PID 1 回收；僵尸已停止，不能把仍运行的进程算成功。
                    stat = Path(f'/proc/{child_pid}/stat')
                    if stat.exists():
                        self.assertEqual(stat.read_text().split()[2], 'Z')
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()
                    if child_pid is not None:
                        try:
                            os.kill(child_pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass


if __name__ == '__main__':
    unittest.main()
