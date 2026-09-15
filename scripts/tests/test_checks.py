"""证明检查入口拒绝失败，链接检查识别仓库使用的 Markdown 语法。"""
import os
import sys
from unittest.mock import patch
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import check_docs as docs


class CheckTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("lychee"), "链接工具未安装；check.py docs 必须安装后执行")
    def test_links_and_fragments_reject_invalid_references(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "target.md").write_text('# 中文标题\n\n<a id="explicit"></a>\n')
            source = directory / "source.md"
            command = ["lychee", "--offline", "--include-fragments", "--no-progress", str(source)]
            source.write_text('[标题](target.md#中文标题)\n\n[引用][id]\n\n[id]: target.md#explicit\n')
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            for link in ['missing.md', 'target.md#missing']:
                source.write_text(f'[错误]({link})\n')
                self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)


class DocumentationTests(unittest.TestCase):
    def fixture(self, root):
        subprocess.run(['git', 'init', '-q', str(root)], check=True)
        (root / 'scripts/tests').mkdir(parents=True)
        (root / 'docs/architecture').mkdir(parents=True)
        (root / 'README.md').write_text('# 入口\n\n[架构](docs/architecture/overview.md)\n')
        (root / '.gitignore').write_text('/docs/plans/\n')
        (root / 'docs/architecture/overview.md').write_text('# 架构概览\n')

    def test_git_scope_includes_new_documents_skips_deleted_and_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            deleted = root / 'docs/deleted.md'
            deleted.write_text('# Deleted\n')
            subprocess.run(['git', 'add', 'README.md', 'docs/deleted.md'], cwd=root, check=True)
            deleted.unlink()
            (root / 'docs/plans').mkdir()
            (root / 'docs/plans/local.md').write_text('local')
            (root / 'docs/architecture/new.md').write_text('# 新报告\n')
            (root / '.agents').mkdir()
            (root / '.agents/reference.md').write_text('upstream text')
            skill = root / '.agents/skills/test/SKILL.md'
            skill.parent.mkdir(parents=True)
            skill.write_text('# Skill\n')
            links, formatting = docs.documents(root)
            self.assertEqual(set(links), {'README.md', 'docs/architecture/overview.md', 'docs/architecture/new.md', '.agents/reference.md', '.agents/skills/test/SKILL.md'})
            self.assertEqual(set(formatting), {'README.md', 'docs/architecture/overview.md', 'docs/architecture/new.md', '.agents/skills/test/SKILL.md'})
            (root / 'docs/linked.md').symlink_to(root / 'README.md')
            with self.assertRaises(ValueError):
                docs.documents(root)

    def test_git_failure_and_empty_selection_are_not_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(subprocess.CalledProcessError):
                docs.documents(root)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            with self.assertRaises(ValueError):
                docs.documents(root)

    def test_command_failure_stops_later_checks_and_main_returns_nonzero(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            with patch.object(docs, 'ROOT', root), patch.object(docs, 'tool_commands', return_value=['formatter']), patch.object(docs.subprocess, 'run', side_effect=subprocess.CalledProcessError(17, 'formatter')) as run:
                self.assertEqual(docs.main(), 1)
                self.assertEqual(run.call_count, 1, '格式失败后不能继续链接检查')
            with patch.object(docs, 'ROOT', root), patch.object(docs, 'tool_commands', side_effect=FileNotFoundError('missing tool')):
                self.assertEqual(docs.main(), 1)

    def test_missing_or_wrong_tool_versions_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(docs.subprocess, 'check_output', return_value='v22.0.0\n'), self.assertRaises(ValueError):
                docs.tool_commands(root)
            with patch.object(docs.subprocess, 'check_output', return_value='v24.0.0\n'), self.assertRaises(ValueError):
                docs.tool_commands(root)
            package = root / 'tools/docs/node_modules/markdownlint-cli2'
            package.mkdir(parents=True)
            (package / 'package.json').write_text('{"version":"0.1.0"}')
            with patch.object(docs.subprocess, 'check_output', return_value='v24.0.0\n'), self.assertRaises(ValueError):
                docs.tool_commands(root)
            (package / 'package.json').write_text('{"version":"0.18.1"}')
            with patch.object(docs.subprocess, 'check_output', side_effect=['v24.0.0\n', 'lychee 0.19.0\n']), self.assertRaises(ValueError):
                docs.tool_commands(root)

    @unittest.skipUnless(shutil.which('lychee') and shutil.which('node') and (ROOT / 'tools/docs/node_modules/markdownlint-cli2/package.json').is_file(), '需先安装文档工具；check.py docs 强制核对依赖')
    def test_real_entrypoint_new_documents_links_formatting_and_missing_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            for name in ('check.py', 'check_environment.py', 'check_docs.py'):
                shutil.copyfile(ROOT / 'scripts' / name, root / 'scripts' / name)
            (root / 'tools/docs').mkdir(parents=True)
            shutil.copyfile(ROOT / 'tools/docs/.markdownlint-cli2.jsonc', root / 'tools/docs/.markdownlint-cli2.jsonc')
            (root / 'tools/docs/node_modules').symlink_to(ROOT / 'tools/docs/node_modules', target_is_directory=True)
            # 最后一步的标记证明入口确实继续执行反例测试；不在夹具里递归调用自身。
            (root / 'scripts/tests/test_checks.py').write_text('import unittest\nfrom pathlib import Path\nclass GateTest(unittest.TestCase):\n    def test_finished(self):\n        Path("gate-completed").touch()\n')
            target = root / 'docs/target.md'
            target.write_text('# 中文标题\n\n<a id="explicit"></a>\n')
            report = root / 'docs/architecture/new.md'
            good = '# 新报告\n\n[标题](../target.md#中文标题)\n\n[引用][id]\n\n[id]: ../target.md#explicit\n'
            report.write_text(good)
            command = [sys.executable, str(root / 'scripts/check.py'), 'docs']
            before = {p: p.read_bytes() for p in root.rglob('*.md')}
            result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((root / 'gate-completed').exists())
            self.assertEqual({p: p.read_bytes() for p in before}, before, '检查不得改写输入')
            cases = [
                ('# 新报告\n\n[坏链接](missing.md)\n', 'missing.md'),
                ('# 新报告\n\n[坏锚点](../target.md#missing)\n', 'missing'),
                ('# 新报告\n\n### 跳级标题\n', 'MD001'),
                ('# 新报告\n\n```\ncommand\n```\n', 'MD040'),
                ('# 新报告\n\n<div>不允许</div>\n', 'MD033'),
            ]
            for content, evidence in cases:
                with self.subTest(evidence=evidence):
                    (root / 'gate-completed').unlink(missing_ok=True)
                    report.write_text(content)
                    result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(evidence, result.stdout + result.stderr)
                    self.assertFalse((root / 'gate-completed').exists(), '失败后应立即退出入口')
            report.write_text(good)
            skill = root / '.agents/skills/new/SKILL.md'
            skill.parent.mkdir(parents=True)
            skill.write_text('# Skill\n\n### Wrong level\n')
            result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('MD001', result.stdout + result.stderr)
            skill.write_text('# Skill\n')
            result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            # 普通被引用目标缺失仍失败；不再依赖历史清单。
            (root / 'docs/architecture/overview.md').unlink()
            result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('overview.md', result.stdout + result.stderr)
            # 去掉失效引用后，已删除文件不再属于当前输入。
            (root / 'README.md').write_text('# 入口\n')
            result = subprocess.run(command, cwd='/tmp', capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
