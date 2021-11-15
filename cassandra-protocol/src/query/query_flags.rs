use bitflags::bitflags;

bitflags! {
    pub struct QueryFlags: u8 {
        /// If set indicates that Query Params contains value.
        const VALUE = 0x01;
        /// If set indicates that Query Params does not contain metadata.
        const SKIP_METADATA = 0x02;
        /// If set indicates that Query Params contains page size.
        const PAGE_SIZE = 0x04;
        /// If set indicates that Query Params contains paging state.
        const WITH_PAGING_STATE = 0x08;
        /// If set indicates that Query Params contains serial consistency.
        const WITH_SERIAL_CONSISTENCY = 0x10;
        /// If set indicates that Query Params contains default timestamp.
        const WITH_DEFAULT_TIMESTAMP = 0x20;
        /// If set indicates that Query Params values are named ones.
        const WITH_NAMES_FOR_VALUES = 0x40;
    }
}

impl Default for QueryFlags {
    #[inline]
    fn default() -> Self {
        QueryFlags::empty()
    }
}
