/// Information about a datacenter.
///
/// New fields may be added in future releases. To build it, use [`DatacenterMetadata::new`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DatacenterMetadata {
    pub rack_count: usize,
}

impl DatacenterMetadata {
    /// Creates datacenter information with the given number of racks. Fields added in future
    /// releases get their default values.
    pub fn new(rack_count: usize) -> Self {
        DatacenterMetadata { rack_count }
    }
}
