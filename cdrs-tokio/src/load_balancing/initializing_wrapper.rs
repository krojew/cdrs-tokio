use std::sync::Arc;

use crate::cluster::topology::Node;
use crate::cluster::{ClusterMetadata, ConnectionManager};
use crate::load_balancing::{LoadBalancingStrategy, QueryPlan, Request};
use crate::transport::CdrsTransport;

// Wrapper strategy which returns contact points until cluster metadata gets populated.
pub struct InitializingWrapperLoadBalancingStrategy<
    T: CdrsTransport + 'static,
    CM: ConnectionManager<T> + 'static,
    LB: LoadBalancingStrategy<T, CM>,
> {
    inner: LB,
    contact_points_query_plan: QueryPlan<T, CM>,
}

impl<T: CdrsTransport, CM: ConnectionManager<T>, LB: LoadBalancingStrategy<T, CM>>
    LoadBalancingStrategy<T, CM> for InitializingWrapperLoadBalancingStrategy<T, CM, LB>
{
    fn query_plan(
        &self,
        request: Option<Request>,
        cluster: &ClusterMetadata<T, CM>,
    ) -> QueryPlan<T, CM> {
        if cluster.has_nodes() {
            self.inner.query_plan(request, cluster)
        } else {
            self.contact_points_query_plan.clone()
        }
    }
}

impl<T: CdrsTransport, CM: ConnectionManager<T>, LB: LoadBalancingStrategy<T, CM>>
    InitializingWrapperLoadBalancingStrategy<T, CM, LB>
{
    pub fn new(inner: LB, contact_points: Vec<Arc<Node<T, CM>>>) -> Self {
        InitializingWrapperLoadBalancingStrategy {
            inner,
            contact_points_query_plan: QueryPlan::new(contact_points),
        }
    }
}
