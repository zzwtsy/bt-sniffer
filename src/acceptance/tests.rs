//! 临时文件验证报告发布与失败语义；跨语言对照验证源码清单定义。
use super::*;
use std::{fs, process::Command};

fn report(directory: &Path) -> Report {
    Report {
        value: json!({"status": "environment_blocked"}),
        path: directory.join("report.json"),
        start: std::time::Instant::now(),
        attempted: false,
    }
}

#[test]
fn explicit_save_preserves_aborted_status_and_rejects_repeated_completion() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = report(directory.path());
    report.running();
    report.finish(false, true).unwrap();
    let original = fs::read(&report.path).unwrap();
    let value: Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(value["status"], "aborted");
    assert!(value["duration_seconds"].as_f64().unwrap() >= 0.0);
    assert_eq!(
        report.finish(true, true).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    drop(report);
    assert_eq!(
        fs::read(directory.path().join("report.json")).unwrap(),
        original
    );
}

#[test]
fn failed_save_does_not_overwrite_or_retry_in_drop() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = report(directory.path());
    fs::write(&report.path, b"existing evidence").unwrap();
    assert!(report.finish(true, true).is_err());
    drop(report);
    assert_eq!(
        fs::read(directory.path().join("report.json")).unwrap(),
        b"existing evidence"
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);

    let mut missing = self::report(&directory.path().join("missing"));
    assert!(missing.finish(true, true).is_err());
    fs::create_dir(directory.path().join("missing")).unwrap();
    drop(missing);
    assert!(!directory.path().join("missing/report.json").exists());
}

#[test]
fn unwind_fallback_never_claims_success() {
    let directory = tempfile::tempdir().unwrap();
    let result = std::panic::catch_unwind(|| {
        let mut report = report(directory.path());
        report.value["status"] = json!("passed");
        panic!("验收在完成确认之前中止");
    });
    assert!(result.is_err());
    let value: Value =
        serde_json::from_slice(&fs::read(directory.path().join("report.json")).unwrap()).unwrap();
    assert_eq!(value["status"], "failed");
    assert_eq!(value["completion_confirmation"], "drop_fallback");
}

#[test]
fn source_identity_matches_python_and_tracks_tests_not_logs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (digest, manifest) = identity::source_digest(root).unwrap();
    let output = Command::new("python3")
        .args(["-c", "import sys; from pathlib import Path; sys.path.insert(0,'scripts'); from evidence import source_identity; import json; print(json.dumps(source_identity(Path.cwd())))"])
        .current_dir(root).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let python: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(python["source_sha256"], digest);
    assert_eq!(python["source_manifest"], manifest);

    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("tests")).unwrap();
    let path = directory.path().join("tests/check.rs");
    fs::write(&path, "first").unwrap();
    let first = identity::source_digest(directory.path()).unwrap();
    fs::create_dir(directory.path().join("logs")).unwrap();
    fs::write(directory.path().join("logs/events.jsonl"), "output").unwrap();
    assert_eq!(identity::source_digest(directory.path()).unwrap(), first);
    fs::write(path, "second").unwrap();
    assert_ne!(identity::source_digest(directory.path()).unwrap(), first);
}

#[test]
fn report_records_real_test_artifact_and_source_limitations() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = Report::new("identity", directory.path(), json!({}));
    assert_eq!(report.value["report_version"], 2);
    assert_eq!(report.value["artifact"]["kind"], "test_executable");
    assert_eq!(
        report.value["artifact"]["path"],
        json!(std::env::current_exe().unwrap())
    );
    assert_eq!(
        report.value["artifact"]["sha256"].as_str().unwrap().len(),
        64
    );
    assert_eq!(
        report.value["build_source_consistency"],
        "not_verified_runtime_source_snapshot"
    );
    report.finish(true, true).unwrap();
}
