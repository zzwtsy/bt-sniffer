//! 记录源码范围、执行产物与工具链；源码当前内容不构成二进制构建一致性证明。
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{fs, io, path::Path, process::Command};

/// 运行时取得的源码和工具链身份；与 Python 的摘要范围保持一致。
#[derive(Serialize)]
pub(super) struct RunIdentity {
    source_scope: &'static str,
    source_sha256: String,
    source_manifest: String,
    artifact: ArtifactIdentity,
    build_source_consistency: &'static str,
    command: Vec<String>,
    cwd: std::path::PathBuf,
    git_head: String,
    git_status: String,
    toolchain: String,
    cargo: String,
}

#[derive(Serialize)]
struct ArtifactIdentity {
    kind: &'static str,
    path: std::path::PathBuf,
    sha256: String,
}

pub(super) fn collect() -> io::Result<RunIdentity> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (source_sha256, source_manifest) = source_digest(root)?;
    let executable = std::env::current_exe()?;
    Ok(RunIdentity {
        source_scope: "cargo-src-tests-scripts-config-v1",
        source_sha256,
        source_manifest,
        artifact: ArtifactIdentity {
            kind: "test_executable",
            sha256: file_digest(&executable)?,
            path: executable,
        },
        build_source_consistency: "not_verified_runtime_source_snapshot",
        command: std::env::args().collect(),
        cwd: std::env::current_dir()?,
        git_head: command(root, "git", &["rev-parse", "HEAD"])?.trim().into(),
        git_status: command(root, "git", &["status", "--porcelain=v1"])?,
        toolchain: command(root, "rustc", &["-Vv"])?,
        cargo: command(root, "cargo", &["-V"])?,
    })
}

fn command(root: &Path, name: &str, args: &[&str]) -> io::Result<String> {
    let output = Command::new(name).args(args).current_dir(root).output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{name} 失败：{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout).map_err(io::Error::other)
}

fn file_digest(path: &Path) -> io::Result<String> {
    use io::Read;
    let mut input = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let length = input.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
    }
    Ok(hex(&digest.finalize()))
}

/// 与 scripts/evidence.py 保持相同范围和编码：排序相对路径，每行摘要、两空格、路径和 LF。
pub(super) fn source_digest(root: &Path) -> io::Result<(String, String)> {
    let mut paths = Vec::new();
    for name in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "build.rs",
    ] {
        if root.join(name).is_file() {
            paths.push(root.join(name));
        }
    }
    for (folder, suffixes) in [
        ("src", &["rs"][..]),
        ("tests", &["rs"][..]),
        ("scripts", &["py", "sh"][..]),
        (".cargo", &["toml"][..]),
        (".github/workflows", &["yml", "yaml"][..]),
    ] {
        visit(&root.join(folder), suffixes, &mut paths)?;
    }
    paths.sort();
    let mut manifest = String::new();
    for path in paths {
        manifest.push_str(&format!(
            "{}  {}\n",
            file_digest(&path)?,
            path.strip_prefix(root)
                .map_err(io::Error::other)?
                .to_string_lossy()
                .replace('\\', "/")
        ));
    }
    Ok((hex(&Sha256::digest(manifest.as_bytes())), manifest))
}

fn visit(path: &Path, suffixes: &[&str], paths: &mut Vec<std::path::PathBuf>) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_symlink() {
        return Err(io::Error::other(format!(
            "源码清单不接受符号链接：{}",
            path.display()
        )));
    }
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            visit(&path, suffixes, paths)?;
        } else if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| suffixes.contains(&value))
        {
            if path.is_symlink() {
                return Err(io::Error::other("源码清单不接受符号链接"));
            }
            paths.push(path);
        }
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
