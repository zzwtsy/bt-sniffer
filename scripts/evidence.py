"""验收身份与报告保存。与 Rust acceptance 使用同一源码范围和清单编码。"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

SOURCE_SCOPE = "cargo-src-tests-scripts-config-v1"


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def source_identity(repo):
    """包含指定范围的未提交文件；日志、文档及编译输出不属于源码清单。"""
    paths = [repo / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "build.rs") if (repo / name).is_file()]
    for folder, suffixes in [("src", {".rs"}), ("tests", {".rs"}), ("scripts", {".py", ".sh"}), (".cargo", {".toml"}), (".github/workflows", {".yml", ".yaml"})]:
        directory = repo / folder
        if directory.is_symlink():
            raise ValueError(f"源码清单不接受符号链接：{directory}")
        for path in directory.rglob("*"):
            if path.is_dir() and path.is_symlink():
                raise ValueError(f"源码清单不接受符号链接：{path}")
            if path.is_file() and path.suffix in suffixes:
                if path.is_symlink():
                    raise ValueError(f"源码清单不接受符号链接：{path}")
                paths.append(path)
    manifest = "".join(f"{sha256(path)}  {path.relative_to(repo).as_posix()}\n" for path in sorted(paths))
    return {"source_scope": SOURCE_SCOPE, "source_manifest": manifest,
            "source_sha256": hashlib.sha256(manifest.encode()).hexdigest()}


def command_text(repo, command):
    return subprocess.check_output(command, cwd=repo, text=True)


def artifact_identity(path, kind):
    return {"kind": kind, "path": str(path), "sha256": sha256(path)}


def write_json(path, value):
    """完整写入同目录临时文件后，以硬链接排他发布；不覆盖已有证据。"""
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".report-", suffix=".tmp", delete=False) as target:
        temporary = Path(target.name)
        try:
            json.dump(value, target, ensure_ascii=False, indent=2, allow_nan=False)
            target.write("\n")
            target.close()
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
