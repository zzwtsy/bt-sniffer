use super::*;
use std::sync::Arc;
use tokio::sync::{Semaphore, oneshot};

#[tokio::test(start_paused = true)]
async fn channel_closes_after_producer_and_preserves_all_messages() {
    let values = vec![1, 2, 3, 4, 5];
    let received = tokio::time::timeout(Duration::from_secs(1), collect_messages(values.clone()))
        .await
        .expect("生产者结束后接收循环应退出")
        .expect("接收者仍存活");
    assert_eq!(received, values);
    assert!(collect_messages(Vec::new()).await.unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancellation_waits_for_cleanup_and_returns_the_permit() {
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let permits = Arc::new(Semaphore::new(1));
    // 在 spawn 前申请，避免创建无界的等待许可任务。
    let permit = permits.clone().acquire_owned().await.unwrap();
    let (started, ready) = oneshot::channel();
    let (cleaned, cleanup_done) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let _permit = permit;
        started.send(()).unwrap();
        worker_stop.cancelled().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        cleaned.send(()).unwrap();
        Ok(())
    });
    ready.await.unwrap();
    assert_eq!(permits.available_permits(), 0);

    shutdown(tasks, &stop, Duration::from_secs(1))
        .await
        .unwrap();

    cleanup_done.await.unwrap();
    assert_eq!(permits.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn deadline_is_an_error_and_abort_is_joined_before_return() {
    let permits = Arc::new(Semaphore::new(1));
    let permit = permits.clone().acquire_owned().await.unwrap();
    let (started, ready) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        let _permit = permit;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
        Ok(())
    });
    ready.await.unwrap();

    let errors = shutdown(tasks, &CancellationToken::new(), Duration::from_secs(1))
        .await
        .unwrap_err();

    assert!(matches!(errors.as_slice(), [ShutdownError::Deadline]));
    assert_eq!(permits.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn business_failure_and_panic_are_both_reported() {
    let mut tasks = JoinSet::new();
    tasks.spawn(async { Err("write failed") });
    tasks.spawn(async { panic!("injected worker panic") });

    let errors = shutdown(tasks, &CancellationToken::new(), Duration::from_secs(1))
        .await
        .unwrap_err();

    assert_eq!(errors.len(), 2);
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, ShutdownError::Worker("write failed")))
    );
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, ShutdownError::Task(error) if error.is_panic()))
    );
}

#[tokio::test(start_paused = true)]
async fn workers_share_one_deadline_instead_of_resetting_it_per_join() {
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let mut ready = Vec::new();
    for seconds in [2, 4] {
        let worker_stop = stop.clone();
        let (started, receiver) = oneshot::channel();
        ready.push(receiver);
        tasks.spawn(async move {
            started.send(()).unwrap();
            worker_stop.cancelled().await;
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            Ok(())
        });
    }
    for receiver in ready {
        receiver.await.unwrap();
    }
    let start = Instant::now();

    let errors = shutdown(tasks, &stop, Duration::from_secs(3))
        .await
        .unwrap_err();

    assert!(matches!(errors.as_slice(), [ShutdownError::Deadline]));
    assert_eq!(Instant::now() - start, Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn no_workers_finishes_without_a_fixed_cleanup_sleep() {
    let start = Instant::now();
    shutdown(
        JoinSet::new(),
        &CancellationToken::new(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(Instant::now(), start);
}
