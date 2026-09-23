//! 网络、控制命令和各类期限在同一 dispatcher task 中交替推进。

use super::super::api::{Command, DispatcherError};
use super::{DhtDispatcher, PendingPurpose};
use crate::dht::{
    krpc::MessageType,
    udp::{ReceivedMessage, UdpTransportError},
};
use std::{future::pending, time::Instant};

impl DhtDispatcher {
    #[cfg(test)]
    pub(crate) async fn run(mut self) -> Result<(), DispatcherError> {
        self.run_loop().await
    }

    pub(in crate::dht::dispatcher) async fn run_loop(&mut self) -> Result<(), DispatcherError> {
        loop {
            let cancellations: Vec<_> = self
                .pending
                .values()
                .map(|pending| &pending.purpose)
                .chain(self.queued.iter().map(|queued| &queued.purpose))
                .filter_map(|purpose| match purpose {
                    PendingPurpose::Fetch { cancel, .. }
                    | PendingPurpose::UserPing { cancel, .. } => Some(cancel.clone()),
                    #[cfg(test)]
                    PendingPurpose::UserFindNode { cancel, .. } => Some(cancel.clone()),
                    _ => None,
                })
                .collect();
            let deadline = self.transactions.next_deadline();
            let maintenance_deadline = self.maintenance.deadline(&self.routing);
            let peer_deadline = self.peers.next_deadline();
            let sampler_deadline = self.sampler.deadline;
            let recovery_deadline = self.recovery.deadline(self.recovery_capacity());
            let output = self.sampler.output_watch();
            tokio::select! {
                _ = super::super::fetch::wait_cancelled(cancellations) => { self.cancel_fetch_queries(); },
                _ = wait_until(self.queue_deadline) => {},
                _ = wait_until(recovery_deadline) => {},
                _ = self.sampler.storage_event() => {},
                _ = wait_until(sampler_deadline), if !self.sampling_paused => {},
                permit = super::super::sampler::watch_output(output), if !self.sampling_paused => {
                    match permit { Ok(permit) => self.sampler.accept_permit(permit), Err(()) => self.stop_sampler(current_time()) }
                }
                received = self.transport.recv_datagram() => match received {
                    Ok(datagram) => {
                        let pending_source=self.pending.values().any(|pending| pending.remote.address==datagram.source);
                        if self.budget.inbound(datagram.source.ip(),datagram.bytes.len(),pending_source)
                            && let Ok(received)=datagram.decode()
                            && (!pending_source || received.message.y != MessageType::Query || self.budget.disguised_query(received.source.ip())) {
                            self.handle_received(received,current_time()).await;
                        }
                    },
                    Err(UdpTransportError::Decode { .. }) | Err(UdpTransportError::MessageTooLarge { source: Some(_), .. }) => {},
                    Err(error) => { self.close_pending(); return Err(DispatcherError::Transport(error)); }
                },
                command = self.commands.recv() => match command {
                    Some(Command::Shutdown { reply }) => { self.close_pending(); let _ = reply.send(()); return Ok(()); }
                    Some(command) => self.handle_command(command, current_time()).await,
                    None => { self.close_pending(); return Ok(()); }
                },
                _ = wait_until(deadline) => { self.expire_transactions(current_time()).await; }
                _ = wait_until(maintenance_deadline) => {}
                _ = wait_until(peer_deadline) => { self.peers.expire(current_time(), 256); }
            }
            self.advance_outbound(current_time()).await;
            self.advance_maintenance(current_time()).await;
            self.advance_recovery(current_time()).await;
            self.advance_sampler(current_time()).await;
        }
    }

    async fn handle_received(&mut self, received: ReceivedMessage, now: Instant) {
        match received.message.y {
            MessageType::Query => self.handle_query(received, now).await,
            MessageType::Response => self.handle_response(received, now).await,
            MessageType::Error => self.handle_error(received, now).await,
        }
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => pending::<()>().await,
    }
}

pub(in crate::dht::dispatcher) fn current_time() -> Instant {
    tokio::time::Instant::now().into_std()
}
