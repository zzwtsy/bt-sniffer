"""前端检查必须覆盖全部阶段，并将缺少工具识别为环境问题。"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import check
import check_environment


class WebChecks(unittest.TestCase):
    def test_web_order_and_all_inclusion(self):
        names = [name for name, _ in check.stages(['web'])]
        self.assertEqual(names, ['environment', 'web-lint', 'web-types', 'web-tests', 'web-build', 'web-browser'])
        rust_names = [name for name, _ in check.stages(['rust'])]
        self.assertNotIn('rust-binary', rust_names)
        self.assertNotIn('web-rust-smoke', rust_names)
        combined = [name for name, _ in check.stages(['web', 'rust'])]
        self.assertEqual(combined.count('rust-binary'), 1)
        self.assertEqual(combined.count('web-rust-smoke'), 1)
        self.assertLess(combined.index('rust-tests'), combined.index('rust-binary'))
        self.assertLess(combined.index('rust-binary'), combined.index('web-rust-smoke'))
        all_names = [name for name, _ in check.stages(['all', 'web'])]
        for name in names:
            self.assertEqual(all_names.count(name), 1)
        self.assertEqual(all_names.count('rust-binary'), 1)
        self.assertEqual(all_names.count('web-rust-smoke'), 1)
        self.assertTrue(all('install' not in command for _, command in check.stages(['web'])))

    def test_wrong_node_and_missing_dependency_are_blocked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(check_environment.subprocess, 'check_output', return_value='v22.0.0'), self.assertRaisesRegex(ValueError, 'Node 24'):
                check_environment.check_web(root)
            with patch.object(check_environment.subprocess, 'check_output', side_effect=['v24.0.0', '11.22.0']), self.assertRaisesRegex(ValueError, '缺少前端依赖'):
                check_environment.check_web(root)

    def test_missing_browser_does_not_install(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries = root / 'web/node_modules/.bin'
            binaries.mkdir(parents=True)
            for name in ('eslint', 'tsc', 'vitest', 'vite', 'playwright'):
                (binaries / name).touch()
            with patch.object(check_environment.subprocess, 'check_output', side_effect=['v24.0.0', '11.22.0', str(root / 'missing-browser')]) as command:
                with self.assertRaisesRegex(ValueError, '缺少 Chromium'):
                    check_environment.check_web(root)
                self.assertEqual(command.call_count, 3)
