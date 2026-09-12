//! 真实二进制入口回归；各进程使用临时工作目录，联网测试仅绑定 loopback 并禁用引导。
use std::{
    path::Path,
    process::{Command, Output},
};

fn command(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bt-sniffer"));
    command.current_dir(directory);
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
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    std::fs::read_to_string(&files[0]).unwrap()
}

fn stderr(output: &Output) -> &str {
    std::str::from_utf8(&output.stderr).unwrap()
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
    for text in [stderr(&output).to_owned(), log_text(directory.path())] {
        assert!(text.contains("application_failed"));
        assert!(text.contains("schema_version=1"));
        assert!(!text.contains('\u{1b}'));
        assert!(!text.contains("logging_queue"));
    }
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
        if line.contains("节点正在监听") {
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
    for text in [stderr, log_text(directory.path())] {
        assert!(text.contains("session_shutdown"), "{text}");
        assert!(text.contains("success=true"));
        assert!(text.contains("schema_version=1"));
        assert!(!text.contains("logging_queue"));
        assert!(!text.contains('\u{1b}'));
    }
    assert!(directory.path().join("state/state.sqlite3").exists());
    assert!(!directory.path().join("state/state.sqlite3-wal").exists());
    assert!(!directory.path().join("state/state.sqlite3-shm").exists());
}
