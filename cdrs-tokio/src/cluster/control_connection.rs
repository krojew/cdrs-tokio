use derive_more::Constructor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::{sleep, timeout, Instant};
use tracing::*;

use crate::cluster::connection_pool::{
    create_heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT,
};
use crate::cluster::topology::Node;
use crate::cluster::{ClusterMetadataManager, ConnectionManager, SessionContext};
use crate::load_balancing::LoadBalancingStrategy;
use crate::retry::{ReconnectionPolicy, ReconnectionSchedule};
use crate::transport::CdrsTransport;
use cassandra_protocol::error::Error;
use cassandra_protocol::events::{ServerEvent, SimpleServerEvent};
use cassandra_protocol::frame::{Envelope, Version};

const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(10);
const EVENT_CHANNEL_CAPACITY: usize = 32;

#[derive(Constructor)]
pub(crate) struct ControlConnection<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    load_balancing: Arc<LB>,
    contact_points: Vec<Arc<Node<T, CM>>>,
    reconnection_policy: Arc<dyn ReconnectionPolicy + Send + Sync>,
    cluster_metadata_manager: Arc<ClusterMetadataManager<T, CM>>,
    event_sender: Sender<ServerEvent>,
    session_context: Arc<SessionContext<T>>,
    version: Version,
}

impl<
        T: CdrsTransport,
        CM: ConnectionManager<T>,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > ControlConnection<T, CM, LB>
{
    pub(crate) async fn run(self, init_complete_sender: tokio::sync::oneshot::Sender<()>) {
        let (event_envelope_sender, event_envelope_receiver) = channel(EVENT_CHANNEL_CAPACITY);
        let (error_sender, mut error_receiver) = channel(1);

        Self::process_events(event_envelope_receiver, self.event_sender.clone());
        let mut init_complete_sender = Some(init_complete_sender);

        'listen: loop {
            let current_connection = self
                .session_context
                .control_connection_transport
                .load()
                .clone();
            if let Some(current_connection) = current_connection {
                let register_envelope = Envelope::new_req_register(
                    vec![
                        SimpleServerEvent::SchemaChange,
                        SimpleServerEvent::StatusChange,
                        SimpleServerEvent::TopologyChange,
                    ],
                    self.version,
                );

                // in case of error, simply reconnect
                let result = current_connection
                    .write_envelope(&register_envelope, false)
                    .await;
                if let Some(sender) = init_complete_sender.take() {
                    sender.send(()).ok();
                }
                match result {
                    Ok(_) => {
                        let error = Self::wait_for_failure(
                            current_connection.as_ref(),
                            &mut error_receiver,
                            DEFAULT_HEARTBEAT_INTERVAL,
                            DEFAULT_HEARTBEAT_TIMEOUT,
                            self.version,
                        )
                        .await;
                        match error {
                            Some(error) => {
                                // show info and try to reconnect
                                warn!(%error, "Error in control connection! Trying to reconnect.");
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
                    let mut full_refresh = false;

                    let mut plan = self
                        .load_balancing
                        .query_plan(None, self.cluster_metadata_manager.metadata().as_ref());
                    if plan.nodes.is_empty() {
                        warn!("No nodes found for control connection!");

                        Self::wait_for_reconnection(&mut schedule).await;

                        // when the whole cluster goes down, there's nothing to update LB state, so
                        // we're left with contact points
                        plan.nodes.clone_from(&self.contact_points);
                        // it means that we need to build metadata from scratch once we are
                        // reconnected
                        full_refresh = true;
                    }

                    for node in &plan.nodes {
                        if let Ok(connection) = node
                            .new_connection(
                                Some(event_envelope_sender.clone()),
                                Some(error_sender.clone()),
                            )
                            .await
                        {
                            debug!("Established new control connection.");

                            self.session_context
                                .control_connection_transport
                                .store(Some(Arc::new(connection)));

                            if let Err(error) = self
                                .cluster_metadata_manager
                                .refresh_metadata(full_refresh)
                                .await
                            {
                                error!(%error, "Error refreshing nodes! Trying to refresh control connection.");
                                continue;
                            }

                            continue 'listen;
                        }
                    }

                    // all nodes failed
                    if init_complete_sender.is_some() {
                        // dropping the sender makes session initialization fail
                        error!("Cannot establish control connection to any contact point!");
                        return;
                    }

                    Self::wait_for_reconnection(&mut schedule).await;
                }
            }
        }
    }

    /// Waits until the connection reports an error or stops answering heartbeats, which also
    /// keep otherwise idle connections from being dropped by NATs and load balancers. Returns
    /// `None` if the error channel gets closed.
    async fn wait_for_failure(
        connection: &T,
        error_receiver: &mut Receiver<Error>,
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        version: Version,
    ) -> Option<Error> {
        let heartbeat_envelope = Envelope::new_req_options(version);
        let mut interval = create_heartbeat_interval(Instant::now(), heartbeat_interval);

        loop {
            tokio::select! {
                error = error_receiver.recv() => return error,
                _ = interval.tick() => {
                    match timeout(
                        heartbeat_timeout,
                        connection.write_envelope(&heartbeat_envelope, false),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => return Some(error),
                        Err(_) => {
                            // metadata queries in flight would otherwise wait until the OS drops
                            // the connection
                            connection.mark_broken();

                            return Some(Error::Timeout(
                                "Timeout waiting for control connection heartbeat response!"
                                    .into(),
                            ))
                        }
                    }
                }
            }
        }
    }

    async fn wait_for_reconnection(schedule: &mut Box<dyn ReconnectionSchedule + Send + Sync>) {
        // as long as the session is alive, try establishing the control connection
        let delay = schedule.next_delay().unwrap_or(DEFAULT_RECONNECT_DELAY);
        sleep(delay).await;
    }

    fn process_events(
        mut event_envelope_receiver: Receiver<Envelope>,
        event_sender: Sender<ServerEvent>,
    ) {
        tokio::spawn(async move {
            while let Some(envelope) = event_envelope_receiver.recv().await {
                if let Ok(body) = envelope.response_body() {
                    if let Some(event) = body.into_server_event() {
                        let _ = event_sender.send(event.event);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::connection_manager::MockConnectionManager;
    use crate::cluster::connection_pool::ConnectionPoolFactory;
    use crate::cluster::topology::{NodeDistance, NodeState};
    use crate::load_balancing::node_distance_evaluator::MockNodeDistanceEvaluator;
    use crate::load_balancing::{
        InitializingWrapperLoadBalancingStrategy, RoundRobinLoadBalancingStrategy,
    };
    use crate::retry::ConstantReconnectionPolicy;
    use crate::transport::MockCdrsTransport;
    use cassandra_protocol::error::Error;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::sync::{broadcast, oneshot, watch};
    use tokio::time::timeout;

    type TestConnectionManager = MockConnectionManager<MockCdrsTransport>;
    type TestLoadBalancing = InitializingWrapperLoadBalancingStrategy<
        MockCdrsTransport,
        TestConnectionManager,
        RoundRobinLoadBalancingStrategy<MockCdrsTransport, TestConnectionManager>,
    >;

    // upper bound for awaiting outcomes which are expected to happen right away
    const TEST_GUARD_TIMEOUT: Duration = Duration::from_secs(5);

    fn create_control_connection(
        contact_points: Vec<SocketAddr>,
        connection_manager: TestConnectionManager,
    ) -> ControlConnection<MockCdrsTransport, TestConnectionManager, TestLoadBalancing> {
        let (_, keyspace_receiver) = watch::channel(None);
        let reconnection_policy =
            Arc::new(ConstantReconnectionPolicy::new(Duration::from_millis(1)));

        let connection_pool_factory = Arc::new(ConnectionPoolFactory::new(
            Default::default(),
            Version::V4,
            connection_manager,
            keyspace_receiver,
            reconnection_policy.clone(),
        ));

        let contact_points: Vec<_> = contact_points
            .into_iter()
            .map(|contact_point| {
                Arc::new(Node::new_with_state(
                    connection_pool_factory.clone(),
                    contact_point,
                    None,
                    None,
                    Some(NodeDistance::Local),
                    NodeState::Up,
                    Default::default(),
                    "".into(),
                    "".into(),
                ))
            })
            .collect();

        let session_context = Arc::new(SessionContext::default());
        let cluster_metadata_manager = Arc::new(ClusterMetadataManager::new(
            contact_points.clone(),
            connection_pool_factory,
            session_context.clone(),
            Box::new(MockNodeDistanceEvaluator::new()),
            Version::V4,
            false,
        ));

        let (event_sender, _) = broadcast::channel(1);

        ControlConnection::new(
            Arc::new(InitializingWrapperLoadBalancingStrategy::new(
                RoundRobinLoadBalancingStrategy::new(),
                contact_points.clone(),
            )),
            contact_points,
            reconnection_policy,
            cluster_metadata_manager,
            event_sender,
            session_context,
            Version::V4,
        )
    }

    async fn assert_initialization_fails(
        control_connection: ControlConnection<
            MockCdrsTransport,
            TestConnectionManager,
            TestLoadBalancing,
        >,
    ) {
        let (init_complete_sender, init_complete_receiver) = oneshot::channel();
        let handle = tokio::spawn(control_connection.run(init_complete_sender));

        let result = timeout(TEST_GUARD_TIMEOUT, init_complete_receiver)
            .await
            .expect("initialization did not finish");
        handle.abort();

        assert!(result.is_err(), "initialization reported success");
    }

    #[tokio::test]
    async fn run_fails_initialization_when_no_contact_point_is_reachable() {
        let mut connection_manager = TestConnectionManager::new();
        connection_manager
            .expect_connection()
            .returning(|_, _, _| Box::pin(async { Err(Error::General("refused".into())) }));

        assert_initialization_fails(create_control_connection(
            vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9042),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9043),
            ],
            connection_manager,
        ))
        .await;
    }

    type TestControlConnection =
        ControlConnection<MockCdrsTransport, TestConnectionManager, TestLoadBalancing>;

    #[tokio::test]
    async fn wait_for_failure_detects_unanswered_heartbeat() {
        let mut connection = MockCdrsTransport::new();
        connection
            .expect_write_envelope()
            .returning(|_, _| Box::pin(futures::future::pending()));
        // fails other requests waiting on the unresponsive connection
        connection.expect_mark_broken().times(1).return_const(());

        let (_error_sender, mut error_receiver) = channel(1);

        let error = timeout(
            TEST_GUARD_TIMEOUT,
            TestControlConnection::wait_for_failure(
                &connection,
                &mut error_receiver,
                Duration::from_millis(1),
                Duration::from_millis(10),
                Version::V4,
            ),
        )
        .await
        .expect("unanswered heartbeat was not detected");

        assert!(matches!(error, Some(Error::Timeout(_))), "{:?}", error);
    }

    #[tokio::test]
    async fn wait_for_failure_keeps_waiting_while_heartbeats_are_answered() {
        let (error_sender, mut error_receiver) = channel(1);
        let heartbeats = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut connection = MockCdrsTransport::new();
        let counted_heartbeats = heartbeats.clone();
        connection.expect_write_envelope().returning(move |_, _| {
            if counted_heartbeats.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 2 {
                error_sender
                    .try_send(Error::General("connection reset".into()))
                    .unwrap();
            }

            Box::pin(async { Ok(Envelope::new_req_options(Version::V4)) })
        });

        let error = timeout(
            TEST_GUARD_TIMEOUT,
            TestControlConnection::wait_for_failure(
                &connection,
                &mut error_receiver,
                Duration::from_millis(1),
                Duration::from_secs(60),
                Version::V4,
            ),
        )
        .await
        .expect("connection error was not reported");

        assert!(matches!(error, Some(Error::General(_))), "{:?}", error);
        assert!(heartbeats.load(std::sync::atomic::Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn run_fails_initialization_without_contact_points() {
        assert_initialization_fails(create_control_connection(
            vec![],
            TestConnectionManager::new(),
        ))
        .await;
    }
}
