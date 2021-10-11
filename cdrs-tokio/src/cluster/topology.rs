use crate::transport::CdrsTransport;
use std::marker::PhantomData;

/// Allows to read current topology info from the cluster
pub struct TopologyReader<T: CdrsTransport + Send + Sync + 'static> {
    _transport: PhantomData<T>,
}

impl<T: CdrsTransport + Send + Sync + 'static> TopologyReader<T> {
    /// Creates new TopologyReader, which connects to known_peers in the background
    //pub fn new(_control_connection: ControlConnection<T, CM, LB>) -> Self {
    pub fn new() -> Self {
        TopologyReader {
            _transport: Default::default(),
        }
    }

    // Fetches current topology info from the cluster
    // pub async fn read_topology_info(&mut self) -> Result<TopologyInfo> {}
    #[allow(dead_code)]
    pub async fn read_topology_info(&mut self, _transport: T) {
        todo!();
    }

    // async fn fetch_topology_info(&self) -> Result<TopologyInfo> {}

    // fn update_known_peers(&mut self, topology_info: &TopologyInfo) {}
}
