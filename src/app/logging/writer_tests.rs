use std::{
    io::{self, Write},
    sync::{Arc, Condvar, Mutex, mpsc},
    time::Duration,
};
struct Gate {
    released: Arc<(Mutex<bool>, Condvar)>,
    entered: mpsc::Sender<()>,
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl Write for Gate {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _ = self.entered.send(());
        let (lock, wake) = &*self.released;
        let _ready = wake
            .wait_while(lock.lock().unwrap(), |ready| !*ready)
            .unwrap();
        self.bytes.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[test]
fn each_output_progresses_while_the_other_is_blocked() {
    for blocked_file in [true, false] {
        let mut writers = Vec::new();
        let mut guards = Vec::new();
        let mut observations = Vec::new();
        for is_file in [true, false] {
            let released = Arc::new((Mutex::new(is_file != blocked_file), Condvar::new()));
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let (entered, receiver) = mpsc::channel();
            let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
                .buffered_lines_limit(4096)
                .lossy(true)
                .finish(Gate {
                    released: released.clone(),
                    entered,
                    bytes: bytes.clone(),
                });
            writers.push(writer);
            guards.push(guard);
            observations.push((released, receiver, bytes));
        }
        let (done, completed) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            for writer in &mut writers {
                writer.write_all(b"final event\n").unwrap();
            }
            done.send(()).unwrap();
        });
        let produced = completed.recv_timeout(Duration::from_secs(2));
        let file_entered = observations[0].1.recv_timeout(Duration::from_secs(2));
        let stderr_entered = observations[1].1.recv_timeout(Duration::from_secs(2));
        let healthy_index = usize::from(blocked_file);
        // 健康端 guard 交接完成后才核对输出，不通过固定睡眠推测已写出。
        drop(guards.remove(healthy_index));
        let healthy = observations[healthy_index].2.lock().unwrap().clone();
        for (released, _, _) in &observations {
            *released.0.lock().unwrap() = true;
            released.1.notify_all();
        }
        producer.join().unwrap();
        drop(guards);
        assert!(produced.is_ok());
        assert!(file_entered.is_ok() && stderr_entered.is_ok());
        assert_eq!(healthy, b"final event\n");
        for (_, _, bytes) in observations {
            assert_eq!(*bytes.lock().unwrap(), b"final event\n");
        }
    }
}

#[test]
fn slow_writer_comparison_and_guard_drain() {
    for (nonblocking, healthy) in [(false, false), (true, false), (false, true), (true, true)] {
        let gate = Arc::new((Mutex::new(healthy), Condvar::new()));
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let (entered, rx) = mpsc::channel();
        let sink = Gate {
            released: gate.clone(),
            entered,
            bytes: bytes.clone(),
        };
        let (mut writer, guard, counter): (Box<dyn Write + Send>, _, _) = if nonblocking {
            let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
                .buffered_lines_limit(if healthy { 4096 } else { 2 })
                .lossy(true)
                .finish(sink);
            let counter = writer.error_counter();
            (Box::new(writer), Some(guard), Some(counter))
        } else {
            (Box::new(sink), None, None)
        };
        let (done, result) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..128 {
                writer.write_all(b"fixed input\n").unwrap();
            }
            done.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let before_release = result.recv_timeout(Duration::from_millis(50)).is_ok();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        producer.join().unwrap();
        drop(guard);
        assert_eq!(before_release, nonblocking || healthy);
        let dropped = counter.map_or(0, |c| c.dropped_lines());
        assert_eq!(bytes.lock().unwrap().len() / 12 + dropped, 128);
        println!(
            "WRITER_REPORT={}",
            serde_json::json!({"nonblocking":nonblocking,"input":128,"queue_capacity":if healthy {4096} else {2},"healthy":healthy,"producer_completed_while_sink_blocked":before_release,"dropped":dropped,"written":bytes.lock().unwrap().len()/12})
        );
    }
}

/// 阻塞一个真实输出端，精确制造三次丢弃，再验证同一计数句柄的累计和增量。
#[test]
fn queue_diagnostics_observe_each_sink_and_reset_interval() {
    for blocked_index in 0..2 {
        let mut writers = Vec::new();
        let mut guards = Vec::new();
        let mut releases = Vec::new();
        let mut entered = Vec::new();
        for index in 0..2 {
            let released = Arc::new((Mutex::new(index != blocked_index), Condvar::new()));
            let (notify, observed) = mpsc::channel();
            let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
                .buffered_lines_limit(super::QUEUE_CAPACITY)
                .lossy(true)
                .finish(Gate {
                    released: released.clone(),
                    entered: notify,
                    bytes: Arc::default(),
                });
            writers.push(writer);
            guards.push(guard);
            releases.push(released);
            entered.push(observed);
        }
        let mut diagnostics = super::QueueDiagnostics::new(
            writers[0].error_counter(),
            writers[1].error_counter(),
            super::DEFAULT_FILTER.into(),
        );
        writers[blocked_index].write_all(b"first\n").unwrap();
        let ready = entered[blocked_index].recv_timeout(Duration::from_secs(2));
        // 第一次写入已离开队列并阻塞在 Gate；其余容量刚好装满，再额外投递三条。
        for _ in 0..super::QUEUE_CAPACITY + 3 {
            writers[blocked_index].write_all(b"queued\n").unwrap();
        }
        let log = tempfile::NamedTempFile::new().unwrap();
        let sink = log.reopen().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            .with_writer(move || sink.try_clone().unwrap())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            diagnostics.report(false);
            diagnostics.report(true);
        });
        // 在断言之前释放 Gate，避免失败的测试遗留阻塞 writer。
        for released in releases {
            *released.0.lock().unwrap() = true;
            released.1.notify_all();
        }
        drop(guards);
        assert!(ready.is_ok());
        let text = std::fs::read_to_string(log.path()).unwrap();
        let events: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 4);
        for (index, event) in events.iter().enumerate() {
            let fields = &event["fields"];
            let blocked = index % 2 == blocked_index;
            assert_eq!(fields["event"], "logging_queue");
            assert_eq!(fields["sink"], ["file", "stderr"][index % 2]);
            assert_eq!(fields["schema_version"], 1);
            assert_eq!(fields["queue_capacity"], super::QUEUE_CAPACITY);
            assert_eq!(fields["dropped_total"], if blocked { 3 } else { 0 });
            assert_eq!(
                fields["dropped_since_last_report"],
                if blocked && index < 2 { 3 } else { 0 }
            );
            assert_eq!(fields["final_snapshot"], index >= 2);
            assert_eq!(
                event["level"],
                if blocked && index < 2 { "WARN" } else { "INFO" }
            );
        }
    }
}
