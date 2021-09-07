const FLAGS_VALUE: u8 = 0x01;
const FLAGS_SKIP_METADATA: u8 = 0x02;
const WITH_PAGE_SIZE: u8 = 0x04;
const WITH_PAGING_STATE: u8 = 0x08;
const WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const WITH_NAME_FOR_VALUES: u8 = 0x40;

/// Cassandra Query Flags.
#[derive(Clone, Debug, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum QueryFlags {
    /// If set indicates that Query Params contains value.
    Value,
    /// If set indicates that Query Params does not contain metadata.
    SkipMetadata,
    /// If set indicates that Query Params contains page size.
    PageSize,
    /// If set indicates that Query Params contains paging state.
    WithPagingState,
    /// If set indicates that Query Params contains serial consistency.
    WithSerialConsistency,
    /// If set indicates that Query Params contains default timestamp.
    WithDefaultTimestamp,
    /// If set indicates that Query Params values are named ones.
    WithNamesForValues,
}

impl QueryFlags {
    #[inline]
    pub(crate) fn has_value(byte: u8) -> bool {
        (byte & FLAGS_VALUE) != 0
    }

    #[inline]
    pub(crate) fn has_skip_metadata(byte: u8) -> bool {
        (byte & FLAGS_SKIP_METADATA) != 0
    }

    #[inline]
    pub(crate) fn has_page_size(byte: u8) -> bool {
        (byte & WITH_PAGE_SIZE) != 0
    }

    #[inline]
    pub(crate) fn has_with_paging_state(byte: u8) -> bool {
        (byte & WITH_PAGING_STATE) != 0
    }

    #[inline]
    pub(crate) fn has_with_serial_consistency(byte: u8) -> bool {
        (byte & WITH_SERIAL_CONSISTENCY) != 0
    }

    #[inline]
    pub(crate) fn has_with_default_timestamp(byte: u8) -> bool {
        (byte & WITH_DEFAULT_TIMESTAMP) != 0
    }

    #[inline]
    pub(crate) fn has_with_names_for_values(byte: u8) -> bool {
        (byte & WITH_NAME_FOR_VALUES) != 0
    }
}

impl From<QueryFlags> for u8 {
    fn from(value: QueryFlags) -> Self {
        match value {
            QueryFlags::Value => FLAGS_VALUE,
            QueryFlags::SkipMetadata => FLAGS_SKIP_METADATA,
            QueryFlags::PageSize => WITH_PAGE_SIZE,
            QueryFlags::WithPagingState => WITH_PAGING_STATE,
            QueryFlags::WithSerialConsistency => WITH_SERIAL_CONSISTENCY,
            QueryFlags::WithDefaultTimestamp => WITH_DEFAULT_TIMESTAMP,
            QueryFlags::WithNamesForValues => WITH_NAME_FOR_VALUES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_value(byte: u8) -> u8 {
        byte | FLAGS_VALUE
    }

    fn add_skip_metadata(byte: u8) -> u8 {
        byte | FLAGS_SKIP_METADATA
    }

    fn add_page_size(byte: u8) -> u8 {
        byte | WITH_PAGE_SIZE
    }

    fn add_with_paging_state(byte: u8) -> u8 {
        byte | WITH_PAGING_STATE
    }

    fn add_with_serial_consistency(byte: u8) -> u8 {
        byte | WITH_SERIAL_CONSISTENCY
    }

    fn add_with_default_timestamp(byte: u8) -> u8 {
        byte | WITH_DEFAULT_TIMESTAMP
    }

    fn add_with_names_for_values(byte: u8) -> u8 {
        byte | WITH_NAME_FOR_VALUES
    }

    #[test]
    fn has_value_test() {
        assert!(
            QueryFlags::has_value(FLAGS_VALUE | 0x10),
            "should show that the flag has value"
        );
        assert!(
            !QueryFlags::has_value(FLAGS_SKIP_METADATA),
            "should show that the flag does NOT have value"
        );
    }

    #[test]
    fn set_value_test() {
        assert_eq!(add_value(0), FLAGS_VALUE, "should set has value flag");
    }

    #[test]
    fn has_skip_metadata_test() {
        assert!(
            QueryFlags::has_skip_metadata(FLAGS_SKIP_METADATA | 0x10),
            "should show that the flag has skip metadata"
        );
        assert!(
            !QueryFlags::has_skip_metadata(FLAGS_VALUE),
            "should show that the flag does NOT have skip metadata"
        );
    }

    #[test]
    fn set_skip_metadata_test() {
        assert_eq!(
            add_skip_metadata(0),
            FLAGS_SKIP_METADATA,
            "should set has skip metadata flag"
        );
    }

    #[test]
    fn has_page_size_test() {
        assert!(
            QueryFlags::has_page_size(WITH_PAGE_SIZE | 0x10),
            "should show that the flag has with page size"
        );
        assert!(
            !QueryFlags::has_page_size(FLAGS_VALUE),
            "should show that the flag does NOT have with page size"
        );
    }

    #[test]
    fn set_page_size_test() {
        assert_eq!(
            add_page_size(0),
            WITH_PAGE_SIZE,
            "should set has page size flag"
        );
    }

    #[test]
    fn has_with_paging_state_test() {
        assert!(
            QueryFlags::has_with_paging_state(WITH_PAGING_STATE | 0x10),
            "should show that the flag has with paging state"
        );
        assert!(
            !QueryFlags::has_with_paging_state(FLAGS_VALUE),
            "should show that the flag does NOT have with paging state"
        );
    }

    #[test]
    fn set_with_paging_state_test() {
        assert_eq!(
            add_with_paging_state(0),
            WITH_PAGING_STATE,
            "should set has with paging state flag"
        );
    }

    #[test]
    fn has_with_serial_consistency_test() {
        assert!(
            QueryFlags::has_with_serial_consistency(WITH_SERIAL_CONSISTENCY | 0x11),
            "should show that the flag has with serial consistency"
        );
        assert!(
            !QueryFlags::has_with_serial_consistency(FLAGS_VALUE),
            "should show that the flag does NOT have with serial consistency"
        );
    }

    #[test]
    fn set_with_serial_consistency_test() {
        assert_eq!(
            add_with_serial_consistency(0),
            WITH_SERIAL_CONSISTENCY,
            "should set has with serial consistency flag"
        );
    }

    #[test]
    fn has_with_default_timestamp_test() {
        assert!(
            QueryFlags::has_with_default_timestamp(WITH_DEFAULT_TIMESTAMP | 0x10),
            "should show that the flag has with default timestamp"
        );
        assert!(
            !QueryFlags::has_with_default_timestamp(FLAGS_VALUE),
            "should show that the flag does NOT have with default timestamp"
        );
    }

    #[test]
    fn set_with_default_timestamp_test() {
        assert_eq!(
            add_with_default_timestamp(0),
            WITH_DEFAULT_TIMESTAMP,
            "should set has with serial consistency flag"
        );
    }

    #[test]
    fn has_with_names_for_values_test() {
        assert!(
            QueryFlags::has_with_names_for_values(WITH_NAME_FOR_VALUES | 0x10),
            "should show that the flag has with name for values"
        );
        assert!(
            !QueryFlags::has_with_names_for_values(FLAGS_VALUE),
            "should show that the flag does NOT have with name for values"
        );
    }

    #[test]
    fn set_with_names_for_values_test() {
        assert_eq!(
            add_with_names_for_values(0),
            WITH_NAME_FOR_VALUES,
            "should set has with name for values flag"
        );
    }

    #[test]
    fn as_byte_test() {
        assert_eq!(
            u8::from(QueryFlags::Value),
            FLAGS_VALUE,
            "should propery convert values flag"
        );

        assert_eq!(
            u8::from(QueryFlags::SkipMetadata),
            FLAGS_SKIP_METADATA,
            "should propery convert skip metadata flag"
        );

        assert_eq!(
            u8::from(QueryFlags::PageSize),
            WITH_PAGE_SIZE,
            "should propery convert with page size flag"
        );

        assert_eq!(
            u8::from(QueryFlags::WithPagingState),
            WITH_PAGING_STATE,
            "should propery convert with paging state flag"
        );

        assert_eq!(
            u8::from(QueryFlags::WithSerialConsistency),
            WITH_SERIAL_CONSISTENCY,
            "should propery convert with serial consistency flag"
        );

        assert_eq!(
            u8::from(QueryFlags::WithDefaultTimestamp),
            WITH_DEFAULT_TIMESTAMP,
            "should propery convert with default timestamp flag"
        );

        assert_eq!(
            u8::from(QueryFlags::WithNamesForValues),
            WITH_NAME_FOR_VALUES,
            "should propery convert with name for values flag"
        );
    }
}
