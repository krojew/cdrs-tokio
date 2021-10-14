use crate::transport::CdrsTransport;
use arc_swap::ArcSwapOption;

pub struct SessionContext<T: CdrsTransport> {
    pub control_connection_transport: ArcSwapOption<T>,
}

impl<T: CdrsTransport> Default for SessionContext<T> {
    fn default() -> Self {
        SessionContext {
            control_connection_transport: ArcSwapOption::empty(),
        }
    }
}
