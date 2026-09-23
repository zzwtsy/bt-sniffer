//! 数据库打开、节点装配以及 fetch、sampling 和 monitor 启动。

use super::{FaultLog, FaultReporter, Session, SessionFault, TaskOutput, TaskRole};
use crate::{
    address::AddressPolicy,
    clock::unix_millis,
    collection::{ingest::SampleIngest, store::CollectionStore},
    dht::{
        dispatcher::{DhtDispatcher, DhtDispatcherConfig, DhtHandle, SamplerConfig},
        persistence::{DhtStore, identity},
        routing::{AddressFamily, RoutingTable},
        transaction::TransactionManager,
        udp::UdpTransport,
    },
    storage::{Storage, StorageConfig, StorageError},
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

impl Session {
    #[cfg(test)]
    pub(crate) async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        Self::open_with_traffic(config, crate::dht::traffic::Config::default()).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_traffic(
        config: StorageConfig,
        traffic: crate::dht::traffic::Config,
    ) -> Result<Self, StorageError> {
        let budget =
            Arc::new(crate::dht::traffic::Budget::new(traffic).map_err(StorageError::Invalid)?);
        Self::open_with_budget(config, budget).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_budget(
        config: StorageConfig,
        budget: Arc<crate::dht::traffic::Budget>,
    ) -> Result<Self, StorageError> {
        Self::open_observed(config, budget, Default::default()).await
    }

    pub(crate) async fn open_observed(
        config: StorageConfig,
        budget: Arc<crate::dht::traffic::Budget>,
        observer: crate::observation::Observer,
    ) -> Result<Self, StorageError> {
        let mut span = observer.span(crate::observation::Kind::Lifecycle, "storage_open");
        let (report, errors) = FaultReporter::new();
        let storage = Storage::open(config)
            .await
            .inspect_err(|_| span.finish("failed"))?;
        let mut collection_store = CollectionStore::new(storage.handle.clone());
        collection_store.observer = observer.clone();
        span.finish("ready");
        let dht_store = DhtStore::new(storage.handle.clone());
        Ok(Self {
            observer,
            monitor: None,
            budget,
            storage: Some(storage),
            collection_store,
            dht_store,
            nodes: Vec::new(),
            stop_snapshots: CancellationToken::new(),
            stop_fetch: CancellationToken::new(),
            tasks: JoinSet::new(),
            roles: HashMap::new(),
            report,
            errors,
            faults: FaultLog::default(),
            shutdown_stage: "尚未开始".into(),
        })
    }

    pub(crate) async fn add_node(
        &mut self,
        instance: &str,
        transport: UdpTransport,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
        policy: AddressPolicy,
    ) -> Result<DhtHandle, StorageError> {
        let store = self.dht_store.clone();
        let address = transport
            .local_addr()
            .map_err(|error| StorageError::Io(error.to_string()))?;
        let family = if address.is_ipv4() {
            AddressFamily::Ipv4
        } else {
            AddressFamily::Ipv6
        };
        let mut identity_span = self
            .observer
            .span(crate::observation::Kind::Lifecycle, "identity_restore");
        let identity =
            identity::load_or_create(&store, instance, family, unix_millis(SystemTime::now())?)
                .await
                .inspect_err(|_| identity_span.finish("failed"))?;
        identity_span.finish("ready");
        if self
            .nodes
            .iter()
            .any(|node| node.identity.key == identity.key)
        {
            return Err(StorageError::Conflict);
        }
        let mut contacts_span = self
            .observer
            .span(crate::observation::Kind::Lifecycle, "contacts_restore");
        let contacts = store
            .load_contacts(identity)
            .await
            .inspect_err(|_| contacts_span.finish("failed"))?;
        contacts_span.finish("ready");
        let mut cooldown_span = self
            .observer
            .span(crate::observation::Kind::Lifecycle, "cooldowns_restore");
        let cooldowns = store
            .restore_cooldowns(identity, unix_millis(SystemTime::now())?)
            .await
            .inspect_err(|_| cooldown_span.finish("failed"))?;
        cooldown_span.finish("ready");
        let table = RoutingTable::new(
            identity.node_id,
            family,
            tokio::time::Instant::now().into_std(),
        );
        let (mut dispatcher, mut handle) =
            DhtDispatcher::with_budget(transport, table, transactions, config, self.budget.clone())
                .map_err(|error| StorageError::Database(error.to_string()))?;
        let mut node_observer = self.observer.clone();
        if node_observer.enabled() {
            node_observer.context.node_id = Some(crate::observation::hex(&identity.node_id.0));
        }
        dispatcher.observer = node_observer.clone();
        handle.observer = node_observer;
        self.observer.emit(crate::observation::Kind::Lifecycle,"node_restore","ready",||serde_json::json!({"node_id":crate::observation::hex(&identity.node_id.0),"contacts":contacts.len(),"address":address.to_string()}));
        dispatcher.attach_storage(store.clone(), identity, contacts, cooldowns, policy)?;
        let report = self.report.clone();
        dispatcher.report_storage_errors_to(report.storage_callback());
        let index = self.nodes.len();
        let task = self
            .tasks
            .spawn(async move { TaskOutput::Dispatcher(dispatcher.run_persistent().await) });
        self.roles.insert(task.id(), TaskRole::Dispatcher(index));
        let stop = self.stop_snapshots.clone();
        let snapshot_handle = handle.clone();
        let snapshot = self.tasks.spawn(async move {
            let mut last = Vec::new();
            let mut timer = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(60),
                Duration::from_secs(60),
            );
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = timer.tick() => {} }
                let operation = async {
                    let contacts = snapshot_handle.routing_snapshot().await?;
                    if contacts != last {
                        store.save_contacts(identity, &contacts).await?;
                        last = contacts;
                    }
                    Ok::<_, StorageError>(())
                };
                let result =
                    tokio::select! { _ = stop.cancelled() => break, result = operation => result };
                if let Err(error) = result {
                    return TaskOutput::Snapshot(Err(error));
                }
            }
            TaskOutput::Snapshot(Ok(()))
        });
        self.roles.insert(snapshot.id(), TaskRole::Snapshot(index));
        self.nodes.push(super::Node {
            identity,
            handle: handle.clone(),
            exit: None,
            collection: None,
            collecting: false,
        });
        Ok(handle)
    }

    pub(crate) async fn start_fetch(
        &mut self,
        config: crate::collection::Config,
    ) -> Result<(), crate::collection::CollectorError> {
        if self.nodes.is_empty()
            || self
                .roles
                .values()
                .any(|role| *role == TaskRole::FetchCoordinator)
        {
            return Err(StorageError::Conflict.into());
        }
        let collector = crate::collection::Collector::new(
            self.collection_store.clone(),
            self.nodes.iter().map(|node| node.handle.clone()).collect(),
            config,
            crate::clock::Clock::default(),
            self.stop_fetch.clone(),
            self.report.pause_notifications(),
            self.report.collector_callback(self.faults.clone()),
        )
        .await?;
        let task = self
            .tasks
            .spawn(async move { TaskOutput::Fetch(collector.run().await) });
        self.roles.insert(task.id(), TaskRole::FetchCoordinator);
        Ok(())
    }

    pub(crate) async fn start_sampling(
        &mut self,
        node: usize,
        config: SamplerConfig,
    ) -> Result<(), StorageError> {
        let store = self.collection_store.clone();
        let index = node;
        let node = self
            .nodes
            .get_mut(node)
            .ok_or(StorageError::Invalid("节点索引无效"))?;
        if node.collecting {
            return Err(StorageError::Conflict);
        }
        let receiver = node
            .handle
            .start_sampling(config)
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let handle = node.handle.clone();
        let report = self.report.clone();
        let task = self.tasks.spawn(async move {
            let mut state = SampleIngest::new(receiver, crate::clock::Clock::default());
            if let Err(error) = state.run(&store).await {
                report.publish(SessionFault::StorageWrite(error.clone()));
                let _ = handle.pause_for_storage(error).await;
            }
            TaskOutput::Collector(state)
        });
        node.collecting = true;
        self.roles
            .insert(task.id(), TaskRole::SampleCollector(index));
        Ok(())
    }

    pub(crate) fn start_monitor(&mut self, listener: tokio::net::TcpListener) {
        self.monitor = Some(crate::monitor::Monitor::start(
            listener,
            self.collection_store.clone(),
            self.nodes.iter().map(|node| node.handle.clone()).collect(),
            self.observer.clone(),
        ));
    }
}
