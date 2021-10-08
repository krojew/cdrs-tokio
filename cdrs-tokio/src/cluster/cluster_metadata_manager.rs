use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::broadcast::Receiver;

use crate::cluster::{ClusterMetadata, ConnectionManager, Node};

use crate::events::ServerEvent;
use crate::transport::CdrsTransport;

// TODO: handle topology changes
pub struct ClusterMetadataManager<T: CdrsTransport, CM: ConnectionManager<T>> {
    #[allow(dead_code)]
    event_receiver: Receiver<ServerEvent>,
    metadata: ArcSwap<ClusterMetadata<T, CM>>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>> ClusterMetadataManager<T, CM> {
    pub fn new(
        event_receiver: Receiver<ServerEvent>,
        contact_points: Vec<Arc<Node<T, CM>>>,
    ) -> Self {
        ClusterMetadataManager {
            event_receiver,
            metadata: ArcSwap::from_pointee(ClusterMetadata::new(contact_points)),
        }
    }

    #[inline]
    pub fn metadata(&self) -> Arc<ClusterMetadata<T, CM>> {
        self.metadata.load().clone()
    }
}
