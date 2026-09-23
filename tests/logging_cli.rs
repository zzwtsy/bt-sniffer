//! 真实二进制入口回归；各进程使用临时工作目录，联网测试仅绑定 loopback 并禁用引导。
use std::{
    path::Path,
    process::{Command, Output},
};

fn command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bt-sniffer"));
    command
        .current_dir(directory)
        .env_remove("RUST_LOG")
        .env_remove("NO_COLOR");
    command
}

fn local_command(directory: &Path) -> Command {
    let mut command = command(directory);
    command.args([
        "--state-dir",
        "state",
        "--ipv4-only",
        "--listen-v4",
        "127.0.0.1:0",
        "--allow-local",
        "--no-bootstrap",
    ]);
    command
}

fn log_text(directory: &Path) -> String {
    let files: Vec<_> = std::fs::read_dir(directory.join("logs"))
        .expect("日志目录应存在")
        .map(|entry| entry.expect("日志目录项应可读取").path())
        .collect();
    assert_eq!(files.len(), 1);
    std::fs::read_to_string(&files[0]).expect("日志文件应可读取")
}

fn stderr(output: &Output) -> &str {
    std::str::from_utf8(&output.stderr).expect("标准错误输出应为 UTF-8")
}

/// 两端、后台线程和退出阶段必须使用同一个非空运行标识。
fn assert_run_id(file: &str, stderr: &str) {
    let mut ids = std::collections::HashSet::new();
    for line in file.lines() {
        let event: serde_json::Value = serde_json::from_str(line).expect("每行日志应为 JSON");
        let id = event["run_id"]
            .as_str()
            .expect("日志事件应包含字符串 run_id");
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        ids.insert(id.to_owned());
    }
    for line in stderr.lines() {
        let id = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("run_id="))
            .expect("文本事件缺少 run_id");
        ids.insert(id.to_owned());
    }
    assert_eq!(ids.len(), 1);
}

#[test]
fn help_version_and_removed_option_do_not_create_resources() {
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["--log-format", "json"],
    ] {
        let directory = tempfile::tempdir().unwrap();
        let output = command(directory.path()).args(&args).output().unwrap();
        assert_eq!(output.status.success(), args[0] != "--log-format");
        if args[0] == "--help" {
            assert!(!String::from_utf8_lossy(&output.stdout).contains("--log-format"));
        }
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[test]
fn file_creation_failure_is_explicit_and_precedes_business_startup() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("logs"), "occupied").unwrap();
    let output = local_command(directory.path()).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(stderr(&output).contains("日志初始化失败"));
    assert!(stderr(&output).contains("无法创建日志目录"));
    assert!(stderr(&output).contains(directory.path().join("logs").to_str().unwrap()));
    assert!(!directory.path().join("state").exists());
    assert_eq!(
        std::fs::read_to_string(directory.path().join("logs")).unwrap(),
        "occupied"
    );
}

#[test]
fn final_application_error_reaches_both_outputs() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("state"), "occupied").unwrap();
    let output = local_command(directory.path()).output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!stderr(&output).contains("监听失败"), "{}", stderr(&output));
    assert_run_id(&log_text(directory.path()), stderr(&output));
    {
        let text = stderr(&output);
        assert!(text.contains("application_failed"));
        assert!(text.contains("schema_version=1"));
        assert!(!text.contains('\u{1b}'));
        assert!(text.contains("logging_queue"));
    }
    let events = json_events(directory.path());
    let failure = events
        .iter()
        .position(|e| e["fields"]["event"] == "application_failed")
        .unwrap();
    assert_eq!(events[failure]["fields"]["schema_version"], 1);
    assert_eq!(events[failure]["fields"]["phase"], "exit");
    assert_final_queues(&events[failure + 1..]);
}

#[cfg(unix)]
#[test]
fn sigterm_keeps_final_snapshot_and_shutdown_logs() {
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Stdio},
        sync::mpsc,
        time::{Duration, Instant},
    };
    // 失败时也回收本测试的子进程；成功路径必须等待正式 SIGTERM 收尾。
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let mut child = Process(
        local_command(directory.path())
            .args(["--sample", "--fetch"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let reader = BufReader::new(child.0.stderr.take().unwrap());
    let (sender, receiver) = mpsc::channel();
    let capture = std::thread::spawn(move || {
        let mut text = String::new();
        for line in reader.lines() {
            let line = line.unwrap();
            text.push_str(&line);
            text.push('\n');
            let _ = sender.send(line);
        }
        text
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let line = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("等待真实节点启动日志超时或进程提前退出");
        assert!(!line.contains("application_failed"), "{line}");
        if line.contains("first_attempt_backlog") {
            break;
        }
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(35);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "SIGTERM 收尾超时");
        std::thread::sleep(Duration::from_millis(10));
    };
    let stderr = capture.join().unwrap();
    assert!(status.success(), "{stderr}");
    use std::io::Read;
    let mut stdout = String::new();
    child
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    assert!(stdout.is_empty());
    assert_run_id(&log_text(directory.path()), &stderr);
    {
        let text = stderr;
        let startup = text
            .lines()
            .find(|line| line.contains("application_start"))
            .unwrap();
        // --allow-local 必须反映在有效配置中，不能误报默认 PublicOnly。
        assert!(startup.contains("address_policy=LocalUnicast"), "{startup}");
        assert!(startup.contains("schema_version=3"));
        assert!(startup.contains("log_contract_version=3"));
        assert!(startup.contains("fetch_timeout_ms=120000"));
        assert!(!startup.contains("metadata_config="));
        assert!(startup.contains("admission_policy_version=2"));
        assert!(startup.contains("scheduling_policy_version=2"));
        assert!(startup.contains("extension_handshake_policy_version=2"));
        assert!(
            text.lines()
                .any(|line| line.contains("extension_compatibility_summary")
                    && line.contains("final_snapshot=true"))
        );
        assert!(text.contains("backpressure_basis=\"first_attempt_waiting\""));
        let sampler = text
            .lines()
            .find(|line| line.contains("sampler_diagnostic"))
            .unwrap();
        for field in [
            "collector_paused=",
            "candidates=",
            "in_flight=",
            "successful=",
            "failed=",
            "unsupported=",
            "pause=",
        ] {
            assert!(sampler.contains(field), "{sampler}");
        }
        assert!(text.lines().any(
            |line| line.contains("admission_backfill") && line.contains("final_snapshot=true")
        ));
        assert!(text.lines().any(|line| line.contains("collector_summary")
            && line.contains("final_snapshot=true")
            && line.contains("running_workers=0")));

        for event in [
            "bencode_sample_summary",
            "attempt_summary",
            "connect_history_summary",
        ] {
            assert!(
                text.lines().any(|line| line.contains(event)
                    && line.contains("final_snapshot=true")
                    && line.contains("schema_version=1")),
                "{text}"
            );
        }
        assert!(text.contains("first_attempt_backlog"));
        assert!(text.contains("session_shutdown"), "{text}");
        assert!(text.contains("success=true"));
        assert!(text.contains("schema_version=1"));
        assert!(text.contains("logging_queue"));
        assert!(!text.contains('\u{1b}'));
    }
    let events = json_events(directory.path());
    let fields: Vec<_> = events.iter().map(|event| &event["fields"]).collect();
    let startup = fields
        .iter()
        .find(|f| f["event"] == "application_start")
        .unwrap();
    assert_eq!(startup["address_policy"], "LocalUnicast");
    assert_eq!(startup["schema_version"], 3);
    assert_eq!(startup["log_contract_version"], 3);
    assert_eq!(startup["log_filter"], "warn,bt_sniffer=info");
    assert_eq!(startup["fetch_timeout_ms"], 120000);
    assert!(startup.get("metadata_config").is_none());
    for policy in [
        "admission_policy_version",
        "scheduling_policy_version",
        "extension_handshake_policy_version",
    ] {
        assert_eq!(startup[policy], 2);
    }
    assert_eq!(startup["backpressure_basis"], "first_attempt_waiting");
    let sampler = fields
        .iter()
        .find(|f| f["event"] == "sampler_diagnostic")
        .unwrap();
    for field in [
        "collector_paused",
        "candidates",
        "in_flight",
        "successful",
        "failed",
        "unsupported",
        "pause",
    ] {
        assert!(sampler.get(field).is_some(), "{sampler}");
    }
    for name in [
        "extension_compatibility_summary",
        "admission_backfill",
        "bencode_sample_summary",
        "attempt_summary",
        "connect_history_summary",
    ] {
        assert!(
            fields.iter().any(|f| f["event"] == name
                && f["final_snapshot"] == true
                && f["schema_version"] == 1),
            "{name}"
        );
    }
    assert!(fields.iter().any(|f| f["event"] == "collector_summary"
        && f["final_snapshot"] == true
        && f["running_workers"] == 0));
    assert!(fields.iter().any(|f| f["event"] == "first_attempt_backlog"));
    let shutdown = events
        .iter()
        .position(|e| e["fields"]["event"] == "session_shutdown")
        .unwrap();
    assert_eq!(events[shutdown]["fields"]["success"], true);
    assert_eq!(events[shutdown]["fields"]["schema_version"], 1);
    assert_final_queues(&events[shutdown + 1..]);
    assert!(directory.path().join("state/state.sqlite3").exists());
    assert!(!directory.path().join("state/state.sqlite3-wal").exists());
    assert!(!directory.path().join("state/state.sqlite3-shm").exists());
}

fn json_events(directory: &Path) -> Vec<serde_json::Value> {
    log_text(directory)
        .lines()
        .map(|line| serde_json::from_str(line).expect("每行日志应为 JSON"))
        .collect()
}

fn assert_final_queues(events: &[serde_json::Value]) {
    for sink in ["file", "stderr"] {
        let queues: Vec<_> = events
            .iter()
            .filter(|event| {
                event["fields"]["event"] == "logging_queue"
                    && event["fields"]["sink"] == sink
                    && event["fields"]["final_snapshot"] == true
            })
            .collect();
        assert_eq!(queues.len(), 1);
        let fields = &queues[0]["fields"];
        assert_eq!(fields["schema_version"], 1);
        assert_eq!(fields["queue_capacity"], 4096);
        assert_eq!(fields["dropped_total"], 0);
        assert_eq!(fields["dropped_since_last_report"], 0);
    }
}

#[test]
fn invalid_environment_fails_before_resources_but_not_help() {
    let mut values = vec![std::ffi::OsString::from("bt_sniffer=invalid")];
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        values.push(std::ffi::OsString::from_vec(vec![255]));
    }
    for value in values {
        let dir = tempfile::tempdir().unwrap();
        let failed = local_command(dir.path())
            .env("RUST_LOG", &value)
            .output()
            .unwrap();
        assert!(!failed.status.success());
        assert!(stderr(&failed).contains("日志初始化失败"));
        assert!(stderr(&failed).contains("RUST_LOG"));
        assert!(failed.stdout.is_empty());
        for arg in ["--help", "--version"] {
            assert!(
                command(dir.path())
                    .arg(arg)
                    .env("RUST_LOG", &value)
                    .output()
                    .unwrap()
                    .status
                    .success()
            );
        }
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
