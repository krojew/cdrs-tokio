use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::sleep;
use tracing::*;

use crate::cluster::{ClusterMetadataManager, ConnectionManager};
use crate::events::{ServerEvent, SimpleServerEvent};
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::retry::ReconnectionPolicy;
use crate::transport::CdrsTransport;

const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(10);
const EVENT_CHANNEL_CAPACITY: usize = 32;

pub struct ControlConnection<
    T: CdrsTransport,
    CM: ConnectionManager<T> + Send + Sync,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<LB>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
    event_sender: Sender<ServerEvent>,
    current_connection: Option<T>,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > ControlConnection<T, CM, LB>
{
    pub fn new(
        load_balancing: Arc<LB>,
        reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
        cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
        event_sender: Sender<ServerEvent>,
    ) -> Self {
        ControlConnection {
            load_balancing,
            reconnection_policy,
            cluster_metadata_manager,
            event_sender,
            current_connection: None,
        }
    }

    pub async fn run(mut self) {
        let (event_frame_sender, event_frame_receiver) = channel(EVENT_CHANNEL_CAPACITY);
        Self::process_events(event_frame_receiver, self.event_sender.clone());

        'listen: loop {
            if let Some(current_connection) = self.current_connection.take() {
                let register_frame = Frame::new_req_register(vec![
                    SimpleServerEvent::SchemaChange,
                    SimpleServerEvent::StatusChange,
                    SimpleServerEvent::TopologyChange,
                ]);

                // in case of error, simply reconnect
                if current_connection
                    .write_frame(&register_frame)
                    .await
                    .is_ok()
                {
                    current_connection.read_frames().await;
                }
            } else {
                debug!("Establishing new control connection...");

                let mut schedule = self.reconnection_policy.new_node_schedule();

                loop {
                    let nodes = self
                        .load_balancing
                        .query_plan(None, self.cluster_metadata_manager.metadata().as_ref());
                    if nodes.is_empty() {
                        warn!("No nodes found for control connection!");

                        // as long as the session is alive, try establishing control connection
                        let delay = schedule.next_delay().unwrap_or(DEFAULT_RECONNECT_DELAY);
                        sleep(delay).await;
                        continue;
                    }

                    for node in nodes {
                        if let Ok(connection) =
                            node.new_connection(Some(event_frame_sender.clone())).await
                        {
                            debug!("Established new control connection.");

                            self.current_connection = Some(connection);
                            continue 'listen;
                        }
                    }
                }
            }
        }
    }

    fn process_events(
        mut event_frame_receiver: Receiver<Frame>,
        event_sender: Sender<ServerEvent>,
    ) {
        tokio::spawn(async move {
            while let Some(frame) = event_frame_receiver.recv().await {
                if let Ok(body) = frame.body() {
                    if let Some(event) = body.into_server_event() {
                        let _ = event_sender.send(event.event);
                    }
                }
            }
        });
    }
}
