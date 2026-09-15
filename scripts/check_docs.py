"""文档检查编排：精确文件范围、固定工具；不安装依赖或改写文件。"""
import json
import re
import stat
import subprocess
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]

def documents(root):
    """Git 枚举包含未忽略新文件；工作树删除不再检查，异常输入明确失败。"""
    output = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    )
    names = sorted(set(output.decode("utf-8").split("\0")) - {""})
    links, formatting = [], []
    for name in names:
        if not name.endswith(".md"):
            continue
        path = root / name
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise ValueError(f"文档输入不接受符号链接：{name}")
        try:
            mode = path.stat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(mode):
            raise ValueError(f"文档输入不是普通文件：{name}")
        links.append(name)
        if (name in ("README.md", "AGENTS.md") or name.startswith("docs/")
                or PurePosixPath(name).match(".agents/skills/*/SKILL.md")):
            formatting.append(name)
    if not links or not formatting:
        raise ValueError("没有找到文档，拒绝将空检查视为通过")
    return links, formatting


def tool_commands(root):
    """只使用已安装的固定版本，不通过 npx 隐式下载。"""
    node_version = subprocess.check_output(["node", "--version"], text=True).strip()
    if not re.fullmatch(r"v24\.\d+\.\d+", node_version):
        raise ValueError("文档格式检查需要 Node 24.x")
    package = root / "tools/docs/node_modules/markdownlint-cli2"
    if not (package / "package.json").is_file():
        raise ValueError("缺少格式工具，请先执行 npm ci --prefix tools/docs")
    version = json.loads((package / "package.json").read_text(encoding="utf-8")).get("version")
    if version != "0.18.1":
        raise ValueError("需要 markdownlint-cli2 0.18.1，请执行 npm ci --prefix tools/docs")
    if subprocess.check_output(["lychee", "--version"], text=True).strip() != "lychee 0.20.0":
        raise ValueError("需要 lychee 0.20.0")
    return ["node", str(package / "markdownlint-cli2-bin.mjs"), "--config",
            str(root / "tools/docs/.markdownlint-cli2.jsonc")]


def check(root):
    root = root.resolve()
    links, formatting = documents(root)
    markdownlint = tool_commands(root)
    print(f"格式：{len(formatting)} 份；链接输入：{len(links)} 份", flush=True)
    # CLI2 的冒号前缀把路径作为字面文件名，避免方括号等被解释为 glob。
    subprocess.run(markdownlint + [f":{root / name}" for name in formatting], cwd=root, check=True)
    subprocess.run(["lychee", "--offline", "--include-fragments", "--hidden", "--no-progress",
                    *[f"./{name}" for name in links]], cwd=root, check=True)


def main():
    try:
        check(ROOT)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"文档检查失败：{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
