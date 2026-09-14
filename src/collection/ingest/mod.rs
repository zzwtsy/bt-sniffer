//! 消费采样分段并保存发现记录；未确认分段与接收队列交回应用用于退出重试。
use super::Collector;
use super::diagnostics::metrics::Counter;
use super::store::CollectionStore;
use crate::clock::unix_millis;
use crate::dht::dispatcher::AnnounceEvent;
use crate::dht::dispatcher::SampleBatch;
use crate::storage::StorageError;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
/// 采样消费拥有未确认批次与接收队列；应用只持有整体状态并监督其执行。
pub(crate) struct SampleIngest {
    clock: crate::clock::Clock,
    receiver: mpsc::Receiver<SampleBatch>,
    /// 已取出但尚未完整确认的批次；取消等待后仍由本对象持有。
    current: Option<SampleBatch>,
    /// 当前批次中已收到数据库成功确认的 hash 数。
    offset: usize,
    error: Option<StorageError>,
}
impl SampleIngest {
    /// 时钟由应用注入；构造不消费批次，也不启动任务。
    pub(crate) fn new(receiver: mpsc::Receiver<SampleBatch>, clock: crate::clock::Clock) -> Self {
        Self {
            clock,
            receiver,
            current: None,
            offset: 0,
            error: None,
        }
    }
    /// 最近一次保存失败；成功完成批次后清除，取消等待不伪造写入错误。
    pub(crate) fn error(&self) -> Option<&StorageError> {
        self.error.as_ref()
    }
    /// 正常消费与退出重试共用；只有数据库确认后才推进分段偏移。
    /// 丢弃等待不撤销已入队命令，恢复时重放未确认分段依赖原 UPSERT 幂等性。
    pub(crate) async fn run(&mut self, store: &CollectionStore) -> Result<(), StorageError> {
        let result = self.consume(store).await;
        if let Err(error) = &result {
            self.error = Some(error.clone());
        }
        result
    }
    async fn consume(&mut self, store: &CollectionStore) -> Result<(), StorageError> {
        loop {
            if self.current.is_none() {
                self.current = self.receiver.recv().await;
                self.offset = 0;
            }
            let Some(batch) = &self.current else {
                return Ok(());
            };
            let at = unix_millis(batch.observed_at)?;
            tracing::debug!(
                event = "sample_batch_save_started",
                schema_version = 1u64,
                phase = "persist",
                responder = ?batch.responder,
                target = ?batch.target,
                received_at = ?batch.received_at,
                observed_at_ms = at,
                interval_secs = batch.interval.as_secs(),
                num = batch.num,
                count = batch.samples.len(),
                confirmed_offset = self.offset,
                "开始保存已验证采样批次"
            );
            while self.offset < batch.samples.len() {
                let end = (self.offset + 1024).min(batch.samples.len());
                store
                    .save_hashes_at(
                        &batch.samples[self.offset..end],
                        at,
                        self.clock
                            .millis_at(tokio::time::Instant::now().into_std())?,
                    )
                    .await?;
                self.offset = end;
            }
            self.current = None;
            self.error = None;
        }
    }
}

impl Collector {
    /// 使用事件观察时间保存提示；接纳确认后才增加 AnnouncesAccepted，拒绝计入丢弃。
    pub(super) async fn accept_announce(&self, event: AnnounceEvent) -> Result<(), StorageError> {
        if !self
            .store
            .discover_peer(event.hash, event.peer, unix_millis(event.observed_at)?)
            .await?
        {
            self.ingress.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.add(Counter::AnnouncesAccepted, 1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
