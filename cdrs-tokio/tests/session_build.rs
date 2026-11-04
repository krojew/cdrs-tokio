#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::session::{SessionBuildError, SessionBuilder, TcpSessionBuilder};
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::retry::NeverReconnectionPolicy;
#[cfg(feature = "e2e-tests")]
use std::sync::Arc;
#[cfg(feature = "e2e-tests")]
use std::time::Duration;

// nothing listens on port 1 of the loopback interface, so connections are refused
#[cfg(feature = "e2e-tests")]
const CLOSED_ADDR: &str = "127.0.0.1:1";

#[cfg(feature = "e2e-tests")]
const BUILD_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn session_build_fails_fast_with_default_reconnection_policy() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(CLOSED_ADDR.into())
        .build()
        .await
        .unwrap();

    let result = tokio::time::timeout(
        BUILD_TIMEOUT,
        TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config).build(),
    )
    .await
    .expect("session build did not fail in time");

    assert_eq!(result.err(), Some(SessionBuildError::SessionInitFailed));
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn session_build_fails_fast_with_never_reconnection_policy() {
    let cluster_config = NodeTcpConfigBuilder::new()
        .with_contact_point(CLOSED_ADDR.into())
        .build()
        .await
        .unwrap();

    let result = tokio::time::timeout(
        BUILD_TIMEOUT,
        TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
            .with_reconnection_policy(Arc::new(NeverReconnectionPolicy))
            .build(),
    )
    .await
    .expect("session build did not fail in time");

    assert_eq!(result.err(), Some(SessionBuildError::SessionInitFailed));
}
