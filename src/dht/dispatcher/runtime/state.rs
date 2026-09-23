//! Dispatcher 状态、待处理查询上下文和构造过程。

#[cfg(test)]
use super::super::api::FindNodeResponse;
use super::super::{
    api::{
        Command, DhtDispatcherConfig, DhtHandle, DispatcherCreateError, PingResponse, QueryError,
        RemoteNode,
    },
    maintenance::MaintenanceState,
    sampling::SampleCache,
};
use super::current_time;
use crate::dht::{
    krpc::NodeId,
    peer_store::PeerStore,
    routing::{AddressFamily, NodeContact, RoutingTable},
    token::TokenManager,
    transaction::{TransactionId, TransactionManager},
    udp::UdpTransport,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

/// transaction manager 之外，dispatcher 完成请求所需的业务上下文。
#[derive(Debug)]
pub(in crate::dht::dispatcher) struct PendingDispatch {
    pub(in crate::dht::dispatcher) observation: crate::observation::Span,
    pub(in crate::dht::dispatcher) remote: RemoteNode,
    pub(in crate::dht::dispatcher) purpose: PendingPurpose,
}

/// 请求业务目的；从待发意图移入 pending，结束时据此回复调用者或推进内部状态机。
#[derive(Debug)]
pub(in crate::dht::dispatcher) enum PendingPurpose {
    Fetch {
        observer: crate::observation::Observer,
        progress: Arc<super::super::api::RpcProgress>,
        cancel: tokio_util::sync::CancellationToken,
        reply: oneshot::Sender<Result<super::super::fetch::GetPeersResponse, QueryError>>,
    },
    Recovery {
        id: NodeId,
    },
    Sampling(super::super::sampler::Request),
    UserPing {
        reply: oneshot::Sender<Result<PingResponse, QueryError>>,
        cancel: tokio_util::sync::CancellationToken,
    },
    #[cfg(test)]
    UserFindNode {
        reply: oneshot::Sender<Result<FindNodeResponse, QueryError>>,
        cancel: tokio_util::sync::CancellationToken,
    },
    Verification {
        key: (NodeId, SocketAddr),
        permit: Option<crate::dht::traffic::VerificationPermit>,
    },
    BucketProbe {
        incumbent: NodeContact,
        remaining: Vec<NodeContact>,
        candidate: NodeContact,
        attempt: u8,
    },
    MaintenanceLookup {
        node_id: NodeId,
    },
}

/// 单个 UDP socket 对应的 DHT 状态所有者与消息分派器。
#[derive(Debug)]
pub(crate) struct DhtDispatcher {
    pub(crate) observer: crate::observation::Observer,
    pub(in crate::dht::dispatcher) budget: Arc<crate::dht::traffic::Budget>,
    pub(in crate::dht::dispatcher) queued: VecDeque<super::super::traffic::Queued>,
    pub(in crate::dht::dispatcher) queue_deadline: Option<Instant>,
    pub(in crate::dht::dispatcher) fetch_ingress: Option<super::super::fetch::FetchIngress>,
    pub(in crate::dht::dispatcher) sampling_paused: bool,
    pub(in crate::dht::dispatcher) automatic_policy: crate::address::AddressPolicy,
    pub(in crate::dht::dispatcher) clock: crate::clock::Clock,
    pub(in crate::dht::dispatcher) recovery: super::super::recovery::Recovery,
    pub(in crate::dht::dispatcher) transport: UdpTransport,
    pub(in crate::dht::dispatcher) routing: RoutingTable,
    pub(in crate::dht::dispatcher) transactions: TransactionManager,
    pub(super) commands: mpsc::Receiver<Command>,
    pub(in crate::dht::dispatcher) pending: HashMap<TransactionId, PendingDispatch>,
    pub(in crate::dht::dispatcher) verifications: HashSet<(NodeId, SocketAddr)>,
    pub(in crate::dht::dispatcher) bucket_probes: HashSet<(NodeId, SocketAddr)>,
    pub(in crate::dht::dispatcher) maintenance: MaintenanceState,
    pub(in crate::dht::dispatcher) tokens: TokenManager,
    pub(in crate::dht::dispatcher) peers: PeerStore,
    pub(in crate::dht::dispatcher) samples: SampleCache,
    pub(in crate::dht::dispatcher) sampler: super::super::sampler::Sampler,
}

impl DhtDispatcher {
    #[cfg(test)]
    pub(crate) fn new(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        Self::with_config(
            transport,
            routing,
            transactions,
            DhtDispatcherConfig::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_config(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        Self::with_budget(transport, routing, transactions, config, Arc::default())
    }

    pub(crate) fn with_budget(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
        budget: Arc<crate::dht::traffic::Budget>,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        if config.command_capacity == 0 {
            return Err(DispatcherCreateError::EmptyCommandQueue);
        }
        let maintenance = config.maintenance;
        if maintenance.enabled
            && (maintenance.refresh_after.is_zero()
                || maintenance.lookup_parallelism == 0
                || maintenance.shortlist_size == 0
                || maintenance.max_queries < maintenance.shortlist_size
                || maintenance.retry_initial.is_zero()
                || maintenance.retry_max < maintenance.retry_initial)
        {
            return Err(DispatcherCreateError::InvalidMaintenanceConfig(
                "时间必须大于零，且 max_queries 不得小于 shortlist_size",
            ));
        }
        let local_address = transport
            .local_addr()
            .map_err(|error| DispatcherCreateError::LocalAddress(error.to_string()))?;
        if !matches!(
            (routing.address_family(), local_address),
            (AddressFamily::Ipv4, SocketAddr::V4(_)) | (AddressFamily::Ipv6, SocketAddr::V6(_))
        ) {
            return Err(DispatcherCreateError::AddressFamilyMismatch {
                socket: local_address,
                routing: routing.address_family(),
            });
        }
        let peers = PeerStore::new(config.peer_store, routing.address_family(), current_time())
            .map_err(DispatcherCreateError::InvalidPeerStoreConfig)?;
        let tokens =
            TokenManager::new(current_time()).map_err(|_| DispatcherCreateError::TokenEntropy)?;
        let (sender, commands) = mpsc::channel(config.command_capacity);
        let maintenance = MaintenanceState::new(maintenance, current_time());
        Ok((
            Self {
                observer: Default::default(),
                budget,
                queued: Default::default(),
                queue_deadline: None,
                fetch_ingress: None,
                sampling_paused: false,
                automatic_policy: config.peer_store.address_policy,
                transport,
                routing,
                transactions,
                commands,
                pending: HashMap::new(),
                verifications: HashSet::new(),
                bucket_probes: HashSet::new(),
                maintenance,
                peers,
                tokens,
                samples: SampleCache::default(),
                sampler: super::super::sampler::Sampler::default(),
                recovery: super::super::recovery::Recovery::default(),
                clock: crate::clock::Clock::default(),
            },
            DhtHandle {
                commands: sender,
                observer: Default::default(),
            },
        ))
    }
}
