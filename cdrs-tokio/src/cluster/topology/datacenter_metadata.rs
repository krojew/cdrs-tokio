use derive_more::Constructor;

/// Information about a datacenter.
#[derive(Clone, Debug, Constructor)]
pub struct DatacenterMetadata {
    pub rack_count: usize,
}
