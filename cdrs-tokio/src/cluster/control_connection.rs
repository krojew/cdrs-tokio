use derive_more::Constructor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::sleep;
use tracing::*;

use crate::cluster::{ClusterMetadataManager, ConnectionManager, SessionContext};
use crate::events::{ServerEvent, SimpleServerEvent};
use crate::frame::Frame;
use crate::load_balancing::LoadBalancingStrategy;
use crate::retry::{ReconnectionPolicy, ReconnectionSchedule};
use crate::transport::CdrsTransport;

const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(10);
const EVENT_CHANNEL_CAPACITY: usize = 32;

#[derive(Constructor)]
pub struct ControlConnection<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<LB>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
    event_sender: Sender<ServerEvent>,
    session_context: Arc<SessionContext<T>>,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > ControlConnection<T, CM, LB>
{
    pub async fn run(self) {
        let (event_frame_sender, event_frame_receiver) = channel(EVENT_CHANNEL_CAPACITY);
        let (error_sender, mut error_receiver) = channel(1);

        Self::process_events(event_frame_receiver, self.event_sender.clone());

        'listen: loop {
            let current_connection = self
                .session_context
                .control_connection_transport
                .load()
                .clone();
            if let Some(current_connection) = current_connection {
                let register_frame = Frame::new_req_register(vec![
                    SimpleServerEvent::SchemaChange,
                    SimpleServerEvent::StatusChange,
                    SimpleServerEvent::TopologyChange,
                ]);

                // in case of error, simply reconnect
                let result = current_connection.write_frame(&register_frame).await;
                match result {
                    Ok(_) => {
                        let error = error_receiver.recv().await;
                        match error {
                            Some(error) => {
                                // show info and try to reconnect
                                error!(%error, "Error in control connection! Trying to reconnect.");
                            }
                            None => {
                                // shouldn't happen, since the connection is shared, but bail out
                                // anyway
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        error!(%error, "Error subscribing to events! Trying to reconnect.");
                    }
                }

                self.session_context
                    .control_connection_transport
                    .store(None);
            } else {
                debug!("Establishing new control connection...");

                let mut schedule = self.reconnection_policy.new_node_schedule();

                loop {
                    let nodes = self
                        .load_balancing
                        .query_plan(None, self.cluster_metadata_manager.metadata().as_ref());
                    if nodes.is_empty() {
                        warn!("No nodes found for control connection!");

                        Self::wait_for_reconnection(&mut schedule).await;
                        continue;
                    }

                    for node in nodes {
                        if let Ok(connection) = node
                            .new_connection(
                                Some(event_frame_sender.clone()),
                                Some(error_sender.clone()),
                            )
                            .await
                        {
                            debug!("Established new control connection.");

                            self.session_context
                                .control_connection_transport
                                .store(Some(Arc::new(connection)));

                            if let Err(error) =
                                self.cluster_metadata_manager.refresh_metadata().await
                            {
                                error!(%error, "Error refreshing nodes! Trying to refresh control connection.");
                                continue;
                            }

                            continue 'listen;
                        }
                    }

                    // all nodes failed
                    Self::wait_for_reconnection(&mut schedule).await;
                }
            }
        }
    }

    async fn wait_for_reconnection(schedule: &mut Box<dyn ReconnectionSchedule + Send + Sync>) {
        // as long as the session is alive, try establishing control connection
        let delay = schedule.next_delay().unwrap_or(DEFAULT_RECONNECT_DELAY);
        sleep(delay).await;
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
