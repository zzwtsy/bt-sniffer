//! 临时文件验证报告发布与失败语义；跨语言对照验证源码清单定义。
use super::*;
use serde_json::Value;
use std::{fs, process::Command};

fn report(directory: &Path) -> Report {
    let mut report = Report::new("test", directory, json!({})).unwrap();
    report.path = directory.join("report.json");
    report
}

#[test]
fn explicit_save_preserves_aborted_status_and_rejects_repeated_completion() {
    let directory = tempfile::tempdir().unwrap();
    let mut report = report(directory.path());
    report.running().unwrap();
    report.finish(FinishOutcome::Aborted).unwrap();
    let original = fs::read(&report.path).unwrap();
    let value: Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(value["status"], "aborted");
    assert!(value["duration_seconds"].as_f64().unwrap() >= 0.0);
    assert_eq!(
        report.finish(FinishOutcome::Passed).unwrap_err().kind(),
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
    assert!(report.finish(FinishOutcome::Passed).is_err());
    assert!(report.running().is_err());
    assert!(report.set_statistics(json!({})).is_err());
    assert!(report.set_families(json!({})).is_err());
    assert!(report.set_verification(vec![]).is_err());
    assert!(report.set_run_errors(None).is_err());
    assert!(report.finish(FinishOutcome::Failed).is_err());
    drop(report);
    assert_eq!(
        fs::read(directory.path().join("report.json")).unwrap(),
        b"existing evidence"
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);

    let mut missing = self::report(&directory.path().join("missing"));
    assert!(missing.finish(FinishOutcome::Passed).is_err());
    fs::create_dir(directory.path().join("missing")).unwrap();
    drop(missing);
    assert!(!directory.path().join("missing/report.json").exists());
}

#[test]
fn unwind_fallback_never_claims_success() {
    let directory = tempfile::tempdir().unwrap();
    let result = std::panic::catch_unwind(|| {
        let mut report = report(directory.path());
        report.running().unwrap();
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
    let mut report = Report::new("identity", directory.path(), json!({})).unwrap();
    let value = report.to_value().unwrap();
    assert_eq!(value["report_version"], 2);
    assert_eq!(value["artifact"]["kind"], "test_executable");
    assert_eq!(
        value["artifact"]["path"],
        json!(std::env::current_exe().unwrap())
    );
    assert_eq!(value["artifact"]["sha256"].as_str().unwrap().len(), 64);
    assert_eq!(
        value["build_source_consistency"],
        "not_verified_runtime_source_snapshot"
    );
    report.finish(FinishOutcome::Passed).unwrap();
}

#[test]
fn explicit_outcomes_and_optional_fields_preserve_json_contract() {
    for (outcome, name) in [
        (FinishOutcome::Passed, "passed"),
        (FinishOutcome::Failed, "failed"),
        (FinishOutcome::Aborted, "aborted"),
        (FinishOutcome::EnvironmentBlocked, "environment_blocked"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut report = report(directory.path());
        assert!(report.to_value().unwrap().get("run_errors").is_none());
        report.set_run_errors(None).unwrap();
        assert!(report.to_value().unwrap().get("run_errors").is_none());
        report
            .set_run_errors(Some(vec!["已清除的错误".into()]))
            .unwrap();
        report.set_run_errors(None).unwrap();
        assert!(report.to_value().unwrap().get("run_errors").is_none());
        report.set_run_errors(Some(vec!["原因".into()])).unwrap();
        report.set_statistics(json!([1, {"count":2}])).unwrap();
        report.set_families(json!({"ipv4":true})).unwrap();
        report.set_verification(vec!["checked".into()]).unwrap();
        report.running().unwrap();
        report.finish(outcome).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&report.path).unwrap()).unwrap();
        assert_eq!(value["status"], name);
        assert_eq!(value["report_version"], 2);
        assert_eq!(value["statistics"], json!([1, {"count":2}]));
        assert_eq!(value["verification"], json!(["checked"]));
        assert_eq!(value["run_errors"], json!(["原因"]));
        assert!(value.get("completion_confirmation").is_none());
        assert!(report.running().is_err());
        assert!(report.set_statistics(json!({})).is_err());
        assert!(report.set_families(json!({})).is_err());
        assert!(report.set_verification(vec![]).is_err());
        assert!(report.set_run_errors(None).is_err());
    }
}

#[test]
fn preparation_drop_and_identity_failure_cannot_publish_success() {
    let directory = tempfile::tempdir().unwrap();
    let report = report(directory.path());
    drop(report);
    let value: Value =
        serde_json::from_slice(&fs::read(directory.path().join("report.json")).unwrap()).unwrap();
    assert_eq!(value["status"], "environment_blocked");
    assert_eq!(value["completion_confirmation"], "drop_fallback");
    let before = fs::read_dir(directory.path()).unwrap().count();
    let result = Report::with_identity(
        "invalid",
        directory.path(),
        json!({}),
        Err(io::Error::other("身份读取失败")),
    );
    assert!(matches!(result, Err(error) if error.to_string() == "身份读取失败"));
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), before);
}
