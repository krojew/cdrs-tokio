use crate::cluster::connection_manager::ConnectionManager;
use crate::load_balancing::LoadBalancingStrategy;
use crate::transport::CdrsTransport;
use std::marker::PhantomData;

/// Allows to read current topology info from the cluster
pub struct TopologyReader<
    T: CdrsTransport + Send + Sync + 'static,
    CM: ConnectionManager<T> + Send + Sync,
    LB: LoadBalancingStrategy<T, CM> + Send + Sync,
> {
    //control_connection: &'a ControlConnection<T, CM, LB>,
    _lb: PhantomData<LB>,         // TODO remove
    _connection: PhantomData<CM>, // TODO remove this
    _transport: PhantomData<T>,
}

impl<
        T: CdrsTransport + Send + Sync + 'static,
        CM: ConnectionManager<T> + Send + Sync,
        LB: LoadBalancingStrategy<T, CM> + Send + Sync,
    > TopologyReader<T, CM, LB>
{
    /// Creates new TopologyReader, which connects to known_peers in the background
    //pub fn new(_control_connection: ControlConnection<T, CM, LB>) -> Self {
    pub fn new() -> Self {
        TopologyReader {
            //       control_connection,
            _lb: Default::default(),
            _connection: Default::default(),
            _transport: Default::default(),
        }
    }

    // Fetches current topology info from the cluster
    // pub async fn read_topology_info(&mut self) -> Result<TopologyInfo> {}
    #[allow(dead_code)]
    pub async fn read_topology_info(&mut self) {
        todo!();
    }

    // async fn fetch_topology_info(&self) -> Result<TopologyInfo> {}

    // fn update_known_peers(&mut self, topology_info: &TopologyInfo) {}
}
