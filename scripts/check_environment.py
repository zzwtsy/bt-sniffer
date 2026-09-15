"""检查入口的内部环境前提；已知缺失返回 2，其他命令失败返回 1。"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import tomllib
from check_docs import tool_commands

ROOT = Path(__file__).resolve().parents[1]


def check(scopes):
    if os.name != 'posix' or sys.version_info < (3, 11):
        raise ValueError('开发检查需要支持进程组的 POSIX 环境和 Python 3.11 以上')
    required = ['git']
    if {'rust', 'examples'} & set(scopes):
        required += ['cargo', 'rustc']
    if 'docs' in scopes:
        required += ['node', 'lychee']
    for name in required:
        if shutil.which(name) is None:
            raise ValueError(f'缺少工具：{name}；请按开发指南安装')
    if {'rust', 'examples'} & set(scopes):
        channel = tomllib.loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        version = subprocess.check_output(['rustc', '--version'], text=True).split()
        if len(version) < 2 or version[1] != channel:
            raise ValueError(f'需要仓库固定 Rust {channel}')
        if shutil.which('rustup') is not None:
            installed = subprocess.check_output(['rustup', 'component', 'list', '--installed'], text=True).splitlines()
            for component in ('rustfmt', 'clippy'):
                if not any(line.split()[0] == component or line.startswith(component + '-') for line in installed):
                    raise ValueError(f'缺少 Rust 组件：{component}；请按开发指南安装')
        for component in ('fmt', 'clippy'):
            subprocess.run(['cargo', component, '--version'], check=True)
    if 'docs' in scopes:
        tool_commands(ROOT)
    print('环境前提通过')


def main():
    try:
        check(sys.argv[1:])
    except (FileNotFoundError, ValueError) as error:
        print(f'环境阻塞：{error}', file=sys.stderr)
        return 2
    except (OSError, subprocess.SubprocessError) as error:
        print(f'环境检查命令失败：{error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
