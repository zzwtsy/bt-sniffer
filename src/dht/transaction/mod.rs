//! KRPC transaction 管理。
//!
//! UDP 没有连接状态，因此必须同时检查 transaction ID 和响应来源地址，才能确认一条
//! 响应属于哪个请求。这个模块只维护请求状态，不负责实际发送 UDP 数据报。
//!
//! dispatcher 先注册再发送，并在失败、取消或到期时撤销；本层不持有业务回复通道。

use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::krpc::QueryMethod;

/// 本节点生成的 transaction ID 固定为 4 字节。
const TRANSACTION_ID_SIZE: usize = 4;

/// 一条出站 KRPC 查询的 transaction ID。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TransactionId([u8; TRANSACTION_ID_SIZE]);

impl TransactionId {
    /// 返回适合写入 [`crate::krpc::KrpcMessage::t`] 的字节串。
    pub(super) fn to_byte_buf(self) -> ByteBuf {
        ByteBuf::from(self.0.to_vec())
    }

    /// 返回 transaction ID 的原始字节。
    #[cfg(test)]
    fn as_bytes(&self) -> &[u8; TRANSACTION_ID_SIZE] {
        &self.0
    }

    fn from_counter(counter: u32) -> Self {
        Self(counter.to_be_bytes())
    }
}

impl TryFrom<&[u8]> for TransactionId {
    type Error = TransactionError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; TRANSACTION_ID_SIZE] =
            bytes
                .try_into()
                .map_err(|_| TransactionError::InvalidTransactionIdLength {
                    actual: bytes.len(),
                })?;
        Ok(Self(bytes))
    }
}

/// 正在等待响应的一条查询。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingTransaction {
    pub(super) id: TransactionId,
    pub(super) destination: SocketAddr,
    pub(super) method: QueryMethod,
    pub(super) started_at: Instant,
    pub(super) deadline: Instant,
}

/// 成功匹配到响应的查询信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompletedTransaction {
    pub(super) transaction: PendingTransaction,
    pub(super) completed_at: Instant,
}

/// transaction 注册或匹配失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionError {
    /// 等待中的请求已经达到配置上限。
    AtCapacity { limit: usize },
    /// 响应中的 transaction ID 长度不是本节点生成的 4 字节格式。
    InvalidTransactionIdLength { actual: usize },
    /// 没有找到对应的等待中请求，可能是迟到、重复或伪造的响应。
    UnknownTransaction(TransactionId),
    /// transaction ID 存在，但响应来自错误地址。
    SourceMismatch {
        id: TransactionId,
        expected: SocketAddr,
        actual: SocketAddr,
    },
    /// 响应到达时请求已经超时。
    Expired(PendingTransaction),
    /// 计数器回绕后，尝试的 ID 都仍在使用。
    IdSpaceExhausted,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtCapacity { limit } => {
                write!(formatter, "等待中的 transaction 已达到 {limit} 条上限")
            }
            Self::InvalidTransactionIdLength { actual } => write!(
                formatter,
                "响应 transaction ID 必须是 {TRANSACTION_ID_SIZE} 字节，实际为 {actual} 字节"
            ),
            Self::UnknownTransaction(id) => {
                write!(formatter, "没有等待 transaction {:02x?}", id.0)
            }
            Self::SourceMismatch {
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "transaction {:02x?} 应来自 {expected}，实际来自 {actual}",
                id.0
            ),
            Self::Expired(transaction) => {
                write!(formatter, "transaction {:02x?} 已超时", transaction.id.0)
            }
            Self::IdSpaceExhausted => write!(formatter, "没有可用的 transaction ID"),
        }
    }
}

impl Error for TransactionError {}

/// 管理一个 UDP socket 发出的所有 KRPC 查询。
#[derive(Debug)]
pub(crate) struct TransactionManager {
    timeout: Duration,
    max_pending: usize,
    next_counter: u32,
    pending: HashMap<TransactionId, PendingTransaction>,
}

impl TransactionManager {
    /// 创建 transaction manager。
    pub(crate) fn new(timeout: Duration, max_pending: usize) -> Self {
        Self::with_initial_counter(timeout, max_pending, 0)
    }

    /// 使用指定的初始计数器创建 manager，主要用于可重复的测试。
    fn with_initial_counter(timeout: Duration, max_pending: usize, initial_counter: u32) -> Self {
        Self {
            timeout,
            max_pending,
            next_counter: initial_counter,
            // 不按外部配置直接预分配，避免错误的超大上限在初始化时耗尽内存。
            pending: HashMap::new(),
        }
    }

    /// 返回当前等待响应的请求数量。
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    /// 返回 transaction 总上限，供维护查询预留用户容量。
    pub(super) fn max_pending(&self) -> usize {
        self.max_pending
    }

    /// 当前是否没有等待中的请求。
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// 注册一条即将发送的查询并返回新的 transaction ID。
    pub(super) fn register(
        &mut self,
        destination: SocketAddr,
        method: QueryMethod,
        now: Instant,
    ) -> Result<TransactionId, TransactionError> {
        if self.pending.len() >= self.max_pending {
            return Err(TransactionError::AtCapacity {
                limit: self.max_pending,
            });
        }

        // 正常情况下第一次就能找到空闲 ID；循环只负责安全处理 u32 回绕。
        for _ in 0..=self.max_pending {
            let id = TransactionId::from_counter(self.next_counter);
            self.next_counter = self.next_counter.wrapping_add(1);
            if self.pending.contains_key(&id) {
                continue;
            }

            let transaction = PendingTransaction {
                id,
                destination,
                method,
                started_at: now,
                deadline: now + self.timeout,
            };
            self.pending.insert(id, transaction);
            return Ok(id);
        }

        Err(TransactionError::IdSpaceExhausted)
    }

    /// 使用 transaction ID 和来源地址匹配一条响应。
    ///
    /// 来源不匹配时不会删除原 transaction，合法响应之后仍然可以完成它。
    pub(super) fn complete(
        &mut self,
        transaction_id: &[u8],
        source: SocketAddr,
        now: Instant,
    ) -> Result<CompletedTransaction, TransactionError> {
        let id = TransactionId::try_from(transaction_id)?;
        let Some(transaction) = self.pending.get(&id) else {
            return Err(TransactionError::UnknownTransaction(id));
        };

        if transaction.destination != source {
            return Err(TransactionError::SourceMismatch {
                id,
                expected: transaction.destination,
                actual: source,
            });
        }

        let transaction = self
            .pending
            .remove(&id)
            .expect("transaction 已在上方确认存在");
        if now >= transaction.deadline {
            return Err(TransactionError::Expired(transaction));
        }

        Ok(CompletedTransaction {
            transaction,
            completed_at: now,
        })
    }

    /// 主动取消一条请求，例如 UDP 发送失败时撤销刚注册的 transaction。
    pub(super) fn cancel(&mut self, id: TransactionId) -> Option<PendingTransaction> {
        self.pending.remove(&id)
    }

    /// 移除并返回所有已经到期的请求。
    pub(super) fn expire(&mut self, now: Instant) -> Vec<PendingTransaction> {
        let expired_ids: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, transaction)| now >= transaction.deadline)
            .map(|(&id, _)| id)
            .collect();

        let mut expired: Vec<_> = expired_ids
            .into_iter()
            .filter_map(|id| self.pending.remove(&id))
            .collect();
        expired.sort_by_key(|transaction| (transaction.deadline, transaction.id));
        expired
    }

    /// 返回最早的超时时间，方便上层设置下一次定时唤醒。
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .map(|transaction| transaction.deadline)
            .min()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod udp_tests;
