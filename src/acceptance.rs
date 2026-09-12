//! 手动验收的独立 JSON 报告；默认测试只验证报告逻辑，不启动长测。
//! 报告在 Drop 执行时尝试写出，普通 unwind 也会经过此路径；强杀、进程 abort
//! 或写入失败都不保证留存，发送取消通知本身也不会立即生成报告。
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// 验收调用者持有的报告；状态先保守记为未验证，完成后由调用者显式判定。
pub(crate) struct Report {
    /// 调用者补充的配置、证据和统计；它不是生产日志的 JSON 编码。
    pub(crate) value: Value,
    path: PathBuf,
    start: std::time::Instant,
}
impl Report {
    /// 采集 rustc 版本与源码指纹，预设 environment_blocked，尚不创建报告文件。
    /// 要求当前目录可读取源码、目标目录已存在；系统时钟或源码读取异常会 panic。
    pub(crate) fn new(kind: &str, directory: &Path, config: Value) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = directory.join(format!("{kind}-{timestamp}.json"));
        let toolchain = Command::new("rustc")
            .arg("-Vv")
            .output()
            .map(|v| String::from_utf8_lossy(&v.stdout).into_owned())
            .unwrap_or_else(|e| e.to_string());
        let (source_sha256, source_manifest) = source_digest();
        Self {
            path,
            start: std::time::Instant::now(),
            value: json!({"kind":kind,"started_unix_ms":timestamp,"status":"environment_blocked","source_sha256":source_sha256,"source_manifest":source_manifest,"toolchain":toolchain,"profile":if cfg!(debug_assertions) {"debug"} else {"release"},"config":config,"verification":[],"statistics":{},"families":{}}),
        }
    }
    /// 已进入验收流程，先标为 failed，防止未走到 finish 的异常退出被误报成功。
    pub(crate) fn running(&mut self) {
        self.value["status"] = json!("failed");
    }
    /// 仅更新内存状态，不写文件；未完成优先记 aborted，完成后再按 passed 判断。
    pub(crate) fn finish(&mut self, completed: bool, passed: bool) {
        self.value["status"] = json!(if !completed {
            "aborted"
        } else if passed {
            "passed"
        } else {
            "failed"
        });
    }
}
impl Drop for Report {
    fn drop(&mut self) {
        self.value["duration_seconds"] = json!(self.start.elapsed().as_secs_f64());
        // create_new 防止覆盖已有报告；失败只打印诊断，Drop 无法向调用者返回写入结果。
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path)?;
            serde_json::to_writer_pretty(file, &self.value)?;
            Ok(())
        })();
        eprintln!("验收报告 {}: {:?}", self.path.display(), result);
    }
}
/// 对 Cargo.toml、Cargo.lock 和 src 下全部 .rs 的排序路径及内容计算指纹。
/// 不包含独立 tests、文档、Git 状态或构建产物，不能据此证明二进制或运行环境一致。
fn source_digest() -> (String, String) {
    fn visit(path: &Path, paths: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path).expect("源码目录") {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, paths);
            } else if path.extension().is_some_and(|e| e == "rs") {
                paths.push(path);
            }
        }
    }
    let mut paths = vec![PathBuf::from("Cargo.toml"), PathBuf::from("Cargo.lock")];
    visit(Path::new("src"), &mut paths);
    paths.sort();
    use sha2::{Digest, Sha256};
    let mut manifest = String::new();
    for path in paths {
        let digest = Sha256::digest(std::fs::read(&path).expect("读取源码"));
        manifest.push_str(&format!("{}  {}\n", hex(&digest), path.display()));
    }
    (hex(&Sha256::digest(manifest.as_bytes())), manifest)
}

#[test]
fn reports_preserve_aborted_status_and_source_fingerprint() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = Report::new(
        "report-regression",
        directory.path(),
        json!({"planned_seconds":7200}),
    );
    report.running();
    report.finish(false, true);
    let path = report.path.clone();
    drop(report);
    let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value["status"], "aborted");
    assert_eq!(value["source_sha256"].as_str().unwrap().len(), 64);
    assert!(value["duration_seconds"].as_f64().unwrap() >= 0.0);
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
