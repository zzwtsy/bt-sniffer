//! 观测分类、关联上下文与过滤条件。

use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

/// 分类为稳定接口标签；步骤和结果由事实所有者给出，不使用 Debug 格式分类。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Lifecycle,
    Bootstrap,
    Routing,
    Rpc,
    Sampling,
    Discovery,
    Admission,
    Job,
    Lookup,
    Peer,
    Piece,
    Validation,
    Commit,
    Retry,
    Backpressure,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Context {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) swarm_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) peer_attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rpc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) observation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) batch_id: Option<String>,
}

/// 上下文只在启用且首次修改时分配；克隆句柄共享不可变关联字段。
#[derive(Debug, Clone, Default)]
pub(crate) struct TraceContext(Option<Arc<Context>>);

impl std::ops::Deref for TraceContext {
    type Target = Context;

    fn deref(&self) -> &Context {
        static EMPTY: Context = Context {
            node_id: None,
            swarm_key: None,
            generation: None,
            parent_span_id: None,
            span_id: None,
            peer_attempt_id: None,
            rpc_id: None,
            observation_id: None,
            batch_id: None,
        };
        self.0.as_deref().unwrap_or(&EMPTY)
    }
}

impl std::ops::DerefMut for TraceContext {
    fn deref_mut(&mut self) -> &mut Context {
        Arc::make_mut(self.0.get_or_insert_with(|| Arc::new(Context::default())))
    }
}

impl Serialize for TraceContext {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        std::ops::Deref::deref(self).serialize(serializer)
    }
}

impl Context {
    pub(super) fn matches(&self, filter: &Filter) -> bool {
        filter
            .hash
            .as_ref()
            .is_none_or(|hash| self.swarm_key.as_ref() == Some(hash))
            && filter.object.as_ref().is_none_or(|id| {
                [
                    self.span_id.as_ref(),
                    self.parent_span_id.as_ref(),
                    self.peer_attempt_id.as_ref(),
                    self.rpc_id.as_ref(),
                    self.observation_id.as_ref(),
                    self.batch_id.as_ref(),
                ]
                .contains(&Some(id))
            })
    }
}

#[derive(Debug, Default)]
pub(crate) struct Filter {
    pub(crate) hash: Option<String>,
    pub(crate) object: Option<String>,
    pub(crate) kind: Option<Kind>,
}

pub(crate) fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
