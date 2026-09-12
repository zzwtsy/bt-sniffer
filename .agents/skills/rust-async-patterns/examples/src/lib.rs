//! 无网络的生命周期示例；取消与完成确认是两个独立步骤。

use tokio::{
    sync::mpsc,
    task::{JoinError, JoinSet},
    time::{Duration, Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

/// 生产者拥有全部 sender；它结束后消费者才会观察到通道排空并关闭。
/// 两个 future 在同一个父任务内运行，父调用取消时不会留下分离任务。
pub async fn collect_messages(values: Vec<u32>) -> Result<Vec<u32>, mpsc::error::SendError<u32>> {
    let (sender, mut receiver) = mpsc::channel(2);
    let produce = async move {
        for value in values {
            sender.send(value).await?;
        }
        drop(sender);
        Ok(())
    };
    let consume = async move {
        let mut values = Vec::new();
        while let Some(value) = receiver.recv().await {
            values.push(value);
        }
        values
    };
    let (sent, values) = tokio::join!(produce, consume);
    sent?;
    Ok(values)
}

/// 保存全部已观察到的业务失败、任务失败和超时，避免返回虚假的完整成功。
#[derive(Debug)]
pub enum ShutdownError {
    Worker(&'static str),
    Task(JoinError),
    Deadline,
}

/// 调用前停止接纳新任务；取消后在共同期限内等待所有任务完成。
///
/// 超时后 abort 并回收剩余异步任务，返回错误。这里要求任务及时让出执行权，
/// 且析构不阻塞；不适用于已启动的 spawn_blocking 或强制终止系统线程。
/// 中途丢弃本函数会由 JoinSet 发出 abort，但调用者将无法得到清理完成确认。
pub async fn shutdown(
    mut tasks: JoinSet<Result<(), &'static str>>,
    stop: &CancellationToken,
    grace: Duration,
) -> Result<(), Vec<ShutdownError>> {
    let deadline = Instant::now() + grace;
    stop.cancel();
    let mut errors = Vec::new();
    let completed = timeout_at(deadline, async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(ShutdownError::Worker(error)),
                Err(error) => errors.push(ShutdownError::Task(error)),
            }
        }
    })
    .await;

    if completed.is_err() {
        errors.push(ShutdownError::Deadline);
        tasks.abort_all();
        // abort 只是请求；继续 join 才能确认 future 已销毁，许可已归还。
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(ShutdownError::Worker(error)),
                Err(error) if error.is_cancelled() => {}
                Err(error) => errors.push(ShutdownError::Task(error)),
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests;
