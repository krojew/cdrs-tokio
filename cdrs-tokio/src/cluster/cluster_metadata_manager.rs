use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwap;
use futures::future::abortable;
use tokio::sync::broadcast::Receiver;

use crate::cluster::{ClusterMetadata, ConnectionManager, Node};

use crate::events::ServerEvent;

use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::CdrsTransport;

use super::topology::TopologyReader;

// TODO: handle topology changes
pub struct ClusterMetadataManager<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + Send + Sync,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<LB>,
    event_receiver: Receiver<ServerEvent>,
    metadata: ArcSwap<ClusterMetadata<T, CM>>,
    topology_reader: TopologyReader<T>,
    refresh_duration: tokio::time::Duration,
    _transport: PhantomData<T>,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > ClusterMetadataManager<T, CM, LB>
{
    pub fn new(
        load_balancing: Arc<LB>,
        contact_points: Vec<Arc<Node<T, CM>>>,
        event_receiver: Receiver<ServerEvent>,
        topology_reader: TopologyReader<T>,
        refresh_duration: tokio::time::Duration,
    ) -> Self {
        ClusterMetadataManager {
            load_balancing,
            event_receiver,
            metadata: ArcSwap::from_pointee(ClusterMetadata::new(contact_points)),
            topology_reader,
            refresh_duration,
            _transport: Default::default(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.metadata.load().clone()
    }

    async fn perform_refresh(&mut self) {
        let nodes = self
            .load_balancing
            .query_plan(None, self.metadata().as_ref());

        if nodes.is_empty() {
            println!("handle an error here ?");
            return;
        }

        for node in nodes {
            if let Ok(connection) = node.new_connection(None).await {
                let topo_info = self.topology_reader.read_topology_info(connection).await;
                println!("{:?}", topo_info);
                return;
            }
        }
    }

    #[allow(dead_code)]
    pub async fn work(mut self) {
        loop {
            //let mut cur_request: Option<RefreshRequest> = None;

            // Wait until it's time for the next refresh
            let (sleep_future, _handle) = abortable(tokio::time::sleep(self.refresh_duration));
            tokio::pin!(sleep_future);

            tokio::select! {
               _ = sleep_future => {},
               // recv_res = self.refresh_channel.recv() => {
               //     match recv_res {
               //         Some(request) => cur_request = Some(request),
               //         None => return, // If refresh_channel was closed then cluster was dropped, we can stop working
               //     }
               // }
               recv_res = self.event_receiver.recv() => {
                   if let Ok(event) = recv_res {
                       //debug!("Received server event: {:?}", event);
                       match event {
                           ServerEvent::TopologyChange(_) => (), // Refresh immediately
                           ServerEvent::StatusChange(_status) => {

                               // If some node went down/up, update it's marker and refresh
                               // later as planned.

                               // match status {
                               //     StatusChangeType::Down(addr) => self.change_node_down_marker(addr, true),
                               //     StatusChangeType::Up(addr) => self.change_node_down_marker(addr, false),
                               // }
                               continue;
                           },
                           _ => continue, // Don't go to refreshing
                       }
                   } else {
                       // If server_events_channel was closed, than TopologyReader was dropped,
                       // so we can probably stop working too
                       return;
                   }
               }
            }

            // Perform the refresh
            //debug!("Requesting topology refresh");
            let _refresh_res = self.perform_refresh().await;

            // Send refresh result if there was a request
            // if let Some(request) = cur_request {
            //     // We can ignore sending error - if no one waits for the response we can drop it
            //     let _ = request.response_chan.send(refresh_res);
            // }
        }
    }
}
