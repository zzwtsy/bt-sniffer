//! 恢复、重试和提交领取；事务负责 generation，纯函数只计算退避结果。
use super::{
    Job, LOCAL_RETRY_DELAY_MS, MAX_FAILED_ATTEMPTS, RETRY_BASE_DELAY_MS, RetryReason, UpdateResult,
};
use crate::collection::{
    peer::VerifiedMetadata, records::check_metadata_size, store::CollectionStore,
};
use crate::storage::StorageError;
use rusqlite::{OptionalExtension, params};
impl CollectionStore {
    /// 启动时恢复未完成任务并使旧领取失效；now_ms 是恢复时的 UTC 毫秒。
    pub(crate) async fn recover_jobs(&self, now_ms: i64) -> Result<(), StorageError> {
        let mut observation = self
            .observer
            .span(crate::observation::Kind::Lifecycle, "jobs_recover");
        self.call(move |connection| {
            observation.executing();
            let tx = connection.transaction()?;
            // 恢复 running 时立即递增领取版本，不必等下次领取才拒绝旧 worker 的结果。
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'pending',
                     due_at = ?1,
                     updated_at = ?1,
                     generation = generation + 1
                 WHERE state = 'running'",
                [now_ms],
            )?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'succeeded',
                     error = NULL
                 WHERE EXISTS (
                     SELECT 1
                     FROM metadata
                     WHERE metadata.hash = fetch_jobs.hash
                 )",
                [],
            )?;
            tx.commit()?;
            observation.finish("applied");
            Ok(())
        })
        .await
    }
    /// 安排当前领取的下一轮尝试，now_ms 是 UTC 毫秒。
    /// Deferred 和 Local 只延期；Failed 增加失败次数，达到上限后休眠。
    ///
    /// Ok(Applied) 表示重试或休眠状态已提交；Ok(Stale) 表示领取已失效，本次未更新任务。
    /// Err 表示命令通道或数据库操作失败，不能当作 Stale；事务内出错时回滚未提交的修改。
    /// 调用者取消等待不会撤销已入队的操作，因此取消本身不能证明重试是否已安排。
    pub(crate) async fn retry_job(
        &self,
        job: Job,
        now_ms: i64,
        reason: RetryReason,
    ) -> Result<UpdateResult, StorageError> {
        let observer = self.observer.for_job(&job.hash.0, job.generation);
        let mut observation = observer.span(crate::observation::Kind::Retry, "retry_transaction");
        let jitter = 0.8 + rand::random::<f64>() * 0.4;
        self.call(move |connection| {
            observation.executing();
            let tx = connection.transaction()?;
            // 必须同时匹配 hash、领取版本与 running；只有 hash 相同不足以接纳迟到结果。
            // 读取失败次数和写回状态共用事务，下面的 UPDATE 延续这里的领取检查。
            let attempts: Option<u32> = tx
                .query_row(
                    "SELECT attempts
                     FROM fetch_jobs
                     WHERE hash = ?1
                       AND generation = ?2
                       AND state = 'running'",
                    params![job.hash.0.as_slice(), job.generation],
                    |row| row.get(0),
                )
                .optional()?;
            let applied = attempts.is_some();
            let mut scheduled = None;
            if let Some(attempts) = attempts {
                let transition = retry_transition(attempts, reason, now_ms, jitter);
                tx.execute(
                    "UPDATE fetch_jobs
                     SET state = ?3,
                         attempts = ?4,
                         due_at = ?5,
                         updated_at = ?6,
                         error = ?7
                     WHERE hash = ?1
                       AND generation = ?2",
                    params![
                        job.hash.0.as_slice(),
                        job.generation,
                        transition.state,
                        transition.attempts,
                        transition.due_at,
                        now_ms,
                        reason.category(),
                    ],
                )?;
                scheduled = Some(transition);
            }
            // 领取失效时没有写入，但仍需显式 commit；提交报错时返回 Err，不返回 Stale。
            tx.commit()?;
            observer.emit(
                crate::observation::Kind::Retry,
                "retry",
                if applied { "applied" } else { "stale" },
                || {
                    serde_json::json!({
                        "reason": reason.category(),
                        "remote_failure": reason.failure_category().is_some(),
                        "state": scheduled.as_ref().map(|transition| transition.state),
                        "remote_failures": scheduled.as_ref().map(|transition| transition.attempts),
                        "due_at_ms": scheduled.as_ref().map(|transition| transition.due_at),
                    })
                },
            );
            observation.finish(if applied { "applied" } else { "stale" });
            Ok(if applied {
                UpdateResult::Applied
            } else {
                UpdateResult::Stale
            })
        })
        .await
    }
    /// 保存当前领取的 metadata，并将任务标记为成功；now_ms 是 UTC 毫秒。
    ///
    /// Ok(Applied) 表示完成事务已提交；已有相同 metadata 时也会完成任务，不代表新增一行。
    /// Ok(Stale) 表示领取已失效，本次不保存 metadata、不更新任务，也不清除地址提示。
    /// hash 不匹配时先返回 Err(Conflict)，不进入领取检查；同一 hash 已有不同字节也返回 Conflict。
    /// 预算不足、命令通道或数据库操作失败同样返回 Err，不能当作 Stale。
    /// 保存 metadata、更新任务状态和清除地址提示共用事务，失败时一起回滚。
    /// 调用者取消等待不会撤销已入队的操作，也不能据此判断事务是否已经提交。
    pub(crate) async fn complete_job(
        &self,
        job: Job,
        metadata: VerifiedMetadata,
        now_ms: i64,
    ) -> Result<UpdateResult, StorageError> {
        let observer = self.observer.for_job(&job.hash.0, job.generation);
        let mut observation =
            observer.span(crate::observation::Kind::Commit, "complete_transaction");
        if metadata.info_hash() != job.hash {
            observation.finish("conflict");
            return Err(StorageError::Conflict);
        }
        // 入队前先取得 metadata 字节预算；job、metadata 和许可一起移入命令。
        // 入队后许可由命令持有到处理结束，不因调用者取消 oneshot 等待而提前释放。
        let permit = self.budget(metadata.info().len()).await?;
        #[cfg(test)]
        let barrier =
            self.take_test_barrier(super::super::test_storage::BlockedOperation::Completion);
        self.submit(permit, move |connection| {
            observation.executing();
            #[cfg(test)]
            if let Some(barrier) = barrier {
                barrier.wait()?;
            }
            let tx = connection.transaction()?;
            let current: bool = tx.query_row(
                "SELECT EXISTS (
                     SELECT 1
                     FROM fetch_jobs
                     WHERE hash = ?1
                       AND generation = ?2
                       AND state = 'running'
                 )",
                params![job.hash.0.as_slice(), job.generation],
                |row| row.get(0),
            )?;
            if !current {
                // 尚未写入；提前返回时 tx 按默认析构行为回滚，结束这个只读事务。
                observation.finish("stale");
                return Ok(UpdateResult::Stale);
            }
            check_metadata_size(&tx, job.hash)?;
            let existing_info: Option<Vec<u8>> = tx
                .query_row(
                    "SELECT info
                     FROM metadata
                     WHERE hash = ?1",
                    [job.hash.0.as_slice()],
                    |row| row.get(0),
                )
                .optional()?;
            if existing_info
                .as_ref()
                .is_some_and(|bytes| bytes.as_slice() != metadata.info())
            {
                return Err(StorageError::Conflict);
            }
            tx.execute(
                "INSERT INTO metadata
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (hash) DO NOTHING",
                params![
                    job.hash.0.as_slice(), // ?1：hash
                    metadata.info(),       // ?2：info，保留原始字节
                    now_ms,                // ?3：fetched_at
                ],
            )?;
            tx.execute(
                "UPDATE fetch_jobs
                 SET state = 'succeeded',
                     updated_at = ?2,
                     error = NULL
                 WHERE hash = ?1",
                params![job.hash.0.as_slice(), now_ms],
            )?;
            tx.execute(
                "DELETE FROM peer_hints
                 WHERE hash = ?1",
                [job.hash.0.as_slice()],
            )?;
            // 领取检查与三次写入都在本事务内；只有提交成功后才记录日志并返回 Applied。
            tx.commit()?;
            observer.emit(crate::observation::Kind::Commit,"metadata","applied",||serde_json::json!({"bytes":metadata.info().len(),"peer":metadata.source().to_string(),"peer_id":crate::observation::hex(&metadata.peer_id().0)}));
            observation.finish("applied");
            tracing::debug!(
                event = "metadata_committed",
                schema_version = 1u64,
                phase = "commit",
                hash = ?metadata.info_hash(),
                source = %metadata.source(),
                peer_id = ?metadata.peer_id(),
                bytes = metadata.info().len(),
                "原始 metadata 已提交"
            );
            Ok(UpdateResult::Applied)
        })
        .await
    }
}

/// 仅为本次事务计算新值，不读取数据库、不抽取随机数，也不修改领取状态。
#[derive(Debug, PartialEq)]
struct RetryTransition {
    state: &'static str,
    attempts: u32,
    due_at: i64,
}
fn retry_transition(
    attempts: u32,
    reason: RetryReason,
    now_ms: i64,
    jitter: f64,
) -> RetryTransition {
    let remote_failure = matches!(reason, RetryReason::Failed(_));
    let attempts = attempts + u32::from(remote_failure);
    let state = if attempts >= MAX_FAILED_ATTEMPTS {
        "dormant"
    } else {
        "retry_wait"
    };
    let delay_ms = if !remote_failure {
        LOCAL_RETRY_DELAY_MS
    } else {
        (RETRY_BASE_DELAY_MS * f64::from(1u32 << attempts.saturating_sub(1)) * jitter) as i64
    };
    RetryTransition {
        state,
        attempts,
        due_at: now_ms.saturating_add(delay_ms),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{failure::AttemptFailure, jobs::LocalReason};
    #[test]
    fn retry_threshold_jitter_and_local_deferral_keep_existing_rules() {
        let remote = RetryReason::Failed(AttemptFailure::PeerIo);
        for (before, state, delays) in [
            (0, "retry_wait", [48_000, 60_000, 72_000]),
            (4, "retry_wait", [768_000, 960_000, 1_152_000]),
            (5, "dormant", [1_536_000, 1_920_000, 2_304_000]),
        ] {
            for (jitter, delay) in [0.8, 1.0, 1.2].into_iter().zip(delays) {
                let next = retry_transition(before, remote, 1000, jitter);
                assert_eq!(next.state, state);
                assert_eq!(next.attempts, before + 1);
                assert_eq!(next.due_at, 1000 + delay);
            }
        }
        for reason in [
            RetryReason::Deferred,
            RetryReason::Local(LocalReason::Cancelled),
            RetryReason::Local(LocalReason::NoRoute),
            RetryReason::Local(LocalReason::ResourceWait),
        ] {
            for before in [0, 5, 6] {
                let next = retry_transition(before, reason, 1000, 0.8);
                assert_eq!(next.attempts, before);
                assert_eq!(next.due_at, 61_000);
                assert_eq!(
                    next.state,
                    if before == 6 { "dormant" } else { "retry_wait" }
                );
            }
        }
        assert_eq!(
            retry_transition(0, remote, i64::MAX - 1, 1.0).due_at,
            i64::MAX
        );
    }
}
