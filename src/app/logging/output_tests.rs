//! 使用子进程隔离全局 subscriber、工作目录和环境变量。
use serde_json::Value;
use std::{
    ffi::OsStr,
    path::Path,
    process::{Command, Output},
};

#[test]
fn probe() {
    if std::env::var_os("BT_SNIFFER_LOGGING_TEST_CHILD").is_none() {
        return;
    }
    let (_logging, mut diagnostics) = super::init().unwrap();
    tracing::info!(
        event = "application_info",
        schema_version = 1u64,
        count = 7u64,
        success = true,
        detail = "引号\"、反斜线\\和换行\n",
        absent = tracing::field::Empty
    );
    tracing::warn!(event = "application_warn", schema_version = 1u64);
    tracing::error!(event = "application_error", schema_version = 1u64);
    tracing::warn!(target:"foreign_dependency",event="dependency_warn",schema_version=1u64);
    tracing::error!(target:"foreign_dependency",event="dependency_error",schema_version=1u64);
    tracing::debug!(event = "application_debug");
    tracing::trace!(event = "application_trace");
    tracing::info!(target:"foreign_dependency",event="dependency_info");
    std::thread::spawn(|| tracing::info!(event = "thread_event"))
        .join()
        .unwrap();
    diagnostics.report(true);
}

fn run_probe(directory: &Path, environment: Option<&OsStr>) -> Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "app::logging::output_tests::probe",
            "--nocapture",
        ])
        .current_dir(directory)
        .env("BT_SNIFFER_LOGGING_TEST_CHILD", "1")
        .env_remove("RUST_LOG")
        .env("NO_COLOR", "1");
    if let Some(value) = environment {
        command.env("RUST_LOG", value);
    }
    command.output().unwrap()
}
fn log_files(directory: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(directory.join("logs"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            name.starts_with("bt-sniffer.") && name.ends_with(".jsonl")
        })
        .collect()
}
fn records(directory: &Path) -> Vec<Value> {
    let paths = log_files(directory);
    assert_eq!(paths.len(), 1);
    std::fs::read_to_string(&paths[0])
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// 专用数据库线程使用进程 subscriber；失败回滚、旧领取和重复完成不能输出提交成功事件。
#[test]
fn metadata_commit_event_requires_applied_transaction() {
    const CHILD: &str = "BT_SNIFFER_METADATA_LOG_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        use crate::collection::{
            jobs::UpdateResult, peer::VerifiedMetadata, test_storage::TestStorage,
        };
        use crate::storage::StorageConfig;

        let (_logging, mut diagnostics) = super::init().unwrap();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let storage = TestStorage::open(StorageConfig::new("state"))
                    .await
                    .unwrap();
                let store = &storage.handle;
                let metadata = || VerifiedMetadata::fixture(b"d4:name1:x6:pieces0:e".to_vec());
                store.enable_fetch(1);
                store
                    .save_hashes(&[metadata().info_hash()], 100)
                    .await
                    .unwrap();
                let old = store.claim_job(100).await.unwrap().unwrap();
                store.recover_jobs(200).await.unwrap();
                let current = store.claim_job(200).await.unwrap().unwrap();
                assert_eq!(
                    store.complete_job(old, metadata(), 201).await.unwrap(),
                    UpdateResult::Stale
                );
                store
                    .call(|connection| {
                        connection.execute_batch(
                            "CREATE TRIGGER fail_log_commit BEFORE UPDATE ON fetch_jobs
                             WHEN NEW.state = 'succeeded'
                             BEGIN SELECT RAISE(ABORT, 'injected commit failure'); END;",
                        )?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert!(
                    store
                        .complete_job(current.clone(), metadata(), 202)
                        .await
                        .is_err()
                );
                assert_eq!(store.fetch_stats().await.unwrap().metadata_count, 0);
                store
                    .call(|connection| {
                        connection.execute_batch("DROP TRIGGER fail_log_commit")?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    store
                        .complete_job(current.clone(), metadata(), 203)
                        .await
                        .unwrap(),
                    UpdateResult::Applied
                );
                assert_eq!(
                    store.complete_job(current, metadata(), 204).await.unwrap(),
                    UpdateResult::Stale
                );
                assert_eq!(store.fetch_stats().await.unwrap().metadata_count, 1);
                storage.shutdown().await.unwrap();
            });
        diagnostics.report(true);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "app::logging::output_tests::metadata_commit_event_requires_applied_transaction",
            "--nocapture",
        ])
        .current_dir(directory.path())
        .env(CHILD, "1")
        .env(
            "RUST_LOG",
            "warn,bt_sniffer::collection::jobs::transitions=debug",
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events = records(directory.path());
    let commits: Vec<_> = events
        .iter()
        .filter(|event| event["fields"]["event"] == "metadata_committed")
        .collect();
    assert_eq!(commits.len(), 1);
    let fields = &commits[0]["fields"];
    assert_eq!(fields["schema_version"].as_u64(), Some(2));
    assert_eq!(fields["phase"], "commit");
    assert_eq!(
        fields["bytes"].as_u64(),
        Some(b"d4:name1:x6:pieces0:e".len() as u64)
    );
    assert_eq!(fields["source"], "127.0.0.1:6881");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(stderr.matches("metadata_committed").count(), 1);
    assert!(stderr.contains(&format!(
        "run_id={}",
        commits[0]["run_id"].as_str().unwrap()
    )));
}

#[test]
fn default_outputs_types_threads_and_append() {
    for environment in [None, Some(OsStr::new(""))] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join(".env"), "RUST_LOG=off\n").unwrap();
        let output = run_probe(directory.path(), environment);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let events = records(directory.path());
        let stderr = String::from_utf8(output.stderr).unwrap();
        let id = events[0]["run_id"].as_str().unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|ch| ch.is_ascii_hexdigit()));
        for event in &events {
            assert_eq!(event["run_id"], id);
            assert!(event["timestamp"].as_str().unwrap().ends_with('Z'));
            let name = event["fields"]["event"].as_str().unwrap();
            assert!(
                stderr
                    .lines()
                    .any(|line| line.contains(name) && line.ends_with(&format!("run_id={id}")))
            );
        }
        for name in [
            "application_info",
            "application_warn",
            "application_error",
            "dependency_warn",
            "dependency_error",
            "thread_event",
            "logging_queue",
        ] {
            assert!(
                events.iter().any(|event| event["fields"]["event"] == name),
                "{name}"
            );
        }
        for name in ["application_debug", "application_trace", "dependency_info"] {
            assert!(!events.iter().any(|event| event["fields"]["event"] == name));
            assert!(!stderr.contains(name));
        }
        let fields = &events
            .iter()
            .find(|event| event["fields"]["event"] == "application_info")
            .unwrap()["fields"];
        assert_eq!(fields["count"].as_u64(), Some(7));
        assert_eq!(fields["success"].as_bool(), Some(true));
        assert_eq!(fields["detail"], "引号\"、反斜线\\和换行\n");
        assert!(fields.get("absent").is_none());
        assert!(!stderr.contains('\u{1b}'));
        let paths = log_files(directory.path());
        assert_eq!(
            paths[0].file_name().unwrap().to_str().unwrap(),
            format!(
                "bt-sniffer.{}.jsonl",
                &events[0]["timestamp"].as_str().unwrap()[..10]
            )
        );
        let original = std::fs::read(&paths[0]).unwrap();
        assert!(run_probe(directory.path(), environment).status.success());
        assert!(std::fs::read(&paths[0]).unwrap().starts_with(&original));
        let appended = records(directory.path());
        assert_eq!(appended.len(), events.len() * 2);
        assert_ne!(appended[events.len()]["run_id"], id);
    }
}

#[test]
fn filters_replace_defaults_on_both_outputs() {
    for (filter, present, absent) in [
        (
            "warn,bt_sniffer=info,bt_sniffer::app::logging::output_tests=debug",
            vec!["application_debug", "application_info", "dependency_warn"],
            vec!["dependency_info", "application_trace"],
        ),
        (
            "error",
            vec!["application_error", "dependency_error"],
            vec![
                "application_info",
                "application_warn",
                "dependency_warn",
                "logging_queue",
            ],
        ),
        (
            "bt_sniffer::app::logging::output_tests=trace",
            vec!["application_trace", "application_info"],
            vec!["dependency_warn", "logging_queue"],
        ),
        (
            "off",
            vec![],
            vec![
                "application_info",
                "application_error",
                "dependency_error",
                "logging_queue",
            ],
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let output = run_probe(directory.path(), Some(OsStr::new(filter)));
        assert!(output.status.success());
        let events = records(directory.path());
        let stderr = String::from_utf8(output.stderr).unwrap();
        for name in present {
            assert!(
                events.iter().any(|event| event["fields"]["event"] == name),
                "{filter}: {name}"
            );
            assert!(stderr.contains(name));
        }
        for name in absent {
            assert!(
                !events.iter().any(|event| event["fields"]["event"] == name),
                "{filter}: {name}"
            );
            assert!(!stderr.contains(name));
        }
        if filter == "off" {
            assert!(events.is_empty());
            assert!(stderr.is_empty());
        }
    }
}

#[test]
fn invalid_filter_precedes_file_creation() {
    let mut values = vec![std::ffi::OsString::from("bt_sniffer=invalid")];
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        values.push(std::ffi::OsString::from_vec(vec![255]));
    }
    for value in values {
        let directory = tempfile::tempdir().unwrap();
        let output = run_probe(directory.path(), Some(&value));
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("RUST_LOG"));
        assert!(!directory.path().join("logs").exists());
    }
}

#[test]
fn startup_limits_jsonl_files_and_preserves_old_text() {
    let directory = tempfile::tempdir().unwrap();
    let logs = directory.path().join("logs");
    std::fs::create_dir(&logs).unwrap();
    for day in 1..=9 {
        std::fs::write(
            logs.join(format!("bt-sniffer.2000-01-{day:02}.jsonl")),
            "old",
        )
        .unwrap();
    }
    let preserved = [
        "bt-sniffer.2000-01-01.log",
        "other.2000-01-01.jsonl",
        "bt-sniffer.2000-01-01.txt",
    ];
    for name in preserved {
        std::fs::write(logs.join(name), "unrelated").unwrap();
    }
    assert!(
        run_probe(directory.path(), Some(OsStr::new("off")))
            .status
            .success()
    );
    assert_eq!(log_files(directory.path()).len(), 7);
    for name in preserved {
        assert_eq!(
            std::fs::read_to_string(logs.join(name)).unwrap(),
            "unrelated"
        );
    }
    assert!(
        run_probe(directory.path(), Some(OsStr::new("off")))
            .status
            .success()
    );
    assert!(log_files(directory.path()).len() <= 7);
}
