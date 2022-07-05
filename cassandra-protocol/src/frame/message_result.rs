use bitflags::bitflags;
use derive_more::{Constructor, Display};
use std::convert::{TryFrom, TryInto};
use std::io::{Cursor, Error as IoError, Read};

use crate::error;
use crate::error::Error;
use crate::frame::events::SchemaChange;
use crate::frame::{FromBytes, FromCursor, Serialize, Version};
use crate::types::rows::Row;
use crate::types::{
    from_cursor_str, serialize_str, try_i16_from_bytes, try_i32_from_bytes, try_u64_from_bytes,
    CBytes, CBytesShort, CInt, CIntShort, INT_LEN, SHORT_LEN,
};

/// `ResultKind` is enum which represents types of result.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Hash, Display)]
pub enum ResultKind {
    /// Void result.
    Void,
    /// Rows result.
    Rows,
    /// Set keyspace result.
    SetKeyspace,
    /// Prepared result.
    Prepared,
    /// Schema change result.
    SchemaChange,
}

impl Serialize for ResultKind {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        CInt::from(*self).serialize(cursor, version);
    }
}

impl FromBytes for ResultKind {
    fn from_bytes(bytes: &[u8]) -> error::Result<ResultKind> {
        try_i32_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(ResultKind::try_from)
    }
}

impl From<ResultKind> for CInt {
    fn from(value: ResultKind) -> Self {
        match value {
            ResultKind::Void => 0x0001,
            ResultKind::Rows => 0x0002,
            ResultKind::SetKeyspace => 0x0003,
            ResultKind::Prepared => 0x0004,
            ResultKind::SchemaChange => 0x0005,
        }
    }
}

impl TryFrom<CInt> for ResultKind {
    type Error = Error;

    fn try_from(value: CInt) -> Result<Self, Self::Error> {
        match value {
            0x0001 => Ok(ResultKind::Void),
            0x0002 => Ok(ResultKind::Rows),
            0x0003 => Ok(ResultKind::SetKeyspace),
            0x0004 => Ok(ResultKind::Prepared),
            0x0005 => Ok(ResultKind::SchemaChange),
            _ => Err(Error::UnexpectedResultKind(value)),
        }
    }
}

impl FromCursor for ResultKind {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<ResultKind> {
        let mut buff = [0; INT_LEN];
        cursor.read_exact(&mut buff)?;

        let rk = CInt::from_be_bytes(buff);
        rk.try_into()
    }
}

/// `ResponseBody` is a generalized enum that represents all types of responses. Each of enum
/// option wraps related body type.
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Hash)]
pub enum ResResultBody {
    /// Void response body. It's an empty struct.
    Void,
    /// Rows response body. It represents a body of response which contains rows.
    Rows(BodyResResultRows),
    /// Set keyspace body. It represents a body of set_keyspace query and usually contains
    /// a name of just set namespace.
    SetKeyspace(BodyResResultSetKeyspace),
    /// Prepared response body.
    Prepared(BodyResResultPrepared),
    /// Schema change body
    SchemaChange(SchemaChange),
}

impl Serialize for ResResultBody {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match &self {
            ResResultBody::Void => {
                ResultKind::Void.serialize(cursor, version);
            }
            ResResultBody::Rows(rows) => {
                ResultKind::Rows.serialize(cursor, version);
                rows.serialize(cursor, version);
            }
            ResResultBody::SetKeyspace(set_keyspace) => {
                ResultKind::SetKeyspace.serialize(cursor, version);
                set_keyspace.serialize(cursor, version);
            }
            ResResultBody::Prepared(prepared) => {
                ResultKind::Prepared.serialize(cursor, version);
                prepared.serialize(cursor, version);
            }
            ResResultBody::SchemaChange(schema_change) => {
                ResultKind::SchemaChange.serialize(cursor, version);
                schema_change.serialize(cursor, version);
            }
        }
    }
}

impl ResResultBody {
    fn parse_body_from_cursor(
        cursor: &mut Cursor<&[u8]>,
        result_kind: ResultKind,
        version: Version,
    ) -> error::Result<ResResultBody> {
        Ok(match result_kind {
            ResultKind::Void => ResResultBody::Void,
            ResultKind::Rows => {
                ResResultBody::Rows(BodyResResultRows::from_cursor(cursor, version)?)
            }
            ResultKind::SetKeyspace => {
                ResResultBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(cursor, version)?)
            }
            ResultKind::Prepared => {
                ResResultBody::Prepared(BodyResResultPrepared::from_cursor(cursor, version)?)
            }
            ResultKind::SchemaChange => {
                ResResultBody::SchemaChange(SchemaChange::from_cursor(cursor, version)?)
            }
        })
    }

    /// Converts body into `Vec<Row>` if body's type is `Row` and returns `None` otherwise.
    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResResultBody::Rows(rows_body) => Some(Row::from_body(rows_body)),
            _ => None,
        }
    }

    /// Returns `Some` rows metadata if envelope result is of type rows and `None` otherwise
    pub fn as_rows_metadata(&self) -> Option<&RowsMetadata> {
        match self {
            ResResultBody::Rows(rows_body) => Some(&rows_body.metadata),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// PREPARE query.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResResultBody::Prepared(p) => Some(p),
            _ => None,
        }
    }

    /// Unwraps body and returns BodyResResultSetKeyspace which contains an exact result of
    /// use keyspace query.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResResultBody::SetKeyspace(p) => Some(p),
            _ => None,
        }
    }
}

impl ResResultBody {
    pub fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<ResResultBody> {
        let result_kind = ResultKind::from_cursor(cursor, version)?;
        ResResultBody::parse_body_from_cursor(cursor, result_kind, version)
    }
}

/// It represents set keyspace result body. Body contains keyspace name.
#[derive(Debug, Constructor, PartialEq, Ord, PartialOrd, Eq, Clone, Hash)]
pub struct BodyResResultSetKeyspace {
    /// It contains name of keyspace that was set.
    pub body: String,
}

impl Serialize for BodyResResultSetKeyspace {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str(cursor, &self.body, version);
    }
}

impl FromCursor for BodyResResultSetKeyspace {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        _version: Version,
    ) -> error::Result<BodyResResultSetKeyspace> {
        from_cursor_str(cursor).map(|x| BodyResResultSetKeyspace::new(x.to_string()))
    }
}

/// Structure that represents result of type
/// [rows](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L533).
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Hash)]
pub struct BodyResResultRows {
    /// Rows metadata
    pub metadata: RowsMetadata,
    /// Number of rows.
    pub rows_count: CInt,
    /// From spec: it is composed of `rows_count` of rows.
    pub rows_content: Vec<Vec<CBytes>>,
    /// Protocol version.
    pub protocol_version: Version,
}

impl Serialize for BodyResResultRows {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.metadata.serialize(cursor, version);
        self.rows_count.serialize(cursor, version);
        self.rows_content
            .iter()
            .flatten()
            .for_each(|x| x.serialize(cursor, version));
    }
}

impl BodyResResultRows {
    fn rows_content(
        cursor: &mut Cursor<&[u8]>,
        rows_count: i32,
        columns_count: i32,
        version: Version,
    ) -> error::Result<Vec<Vec<CBytes>>> {
        (0..rows_count)
            .map(|_| {
                (0..columns_count)
                    .map(|_| CBytes::from_cursor(cursor, version))
                    .collect::<Result<_, _>>()
            })
            .collect::<Result<_, _>>()
    }
}

impl FromCursor for BodyResResultRows {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<BodyResResultRows> {
        let metadata = RowsMetadata::from_cursor(cursor, version)?;
        let rows_count = CInt::from_cursor(cursor, version)?;
        let rows_content =
            BodyResResultRows::rows_content(cursor, rows_count, metadata.columns_count, version)?;

        Ok(BodyResResultRows {
            metadata,
            rows_count,
            rows_content,
            protocol_version: version,
        })
    }
}

/// Rows metadata.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct RowsMetadata {
    /// Flags.
    pub flags: RowsMetadataFlags,
    /// Number of columns.
    pub columns_count: i32,
    /// Paging state.
    pub paging_state: Option<CBytes>,
    /// New, changed result set metadata. The new metadata ID must also be used in subsequent
    /// executions of the corresponding prepared statement, if any.
    pub new_metadata_id: Option<CBytesShort>,
    // In fact by specification Vec should have only two elements representing the
    // (unique) keyspace name and table name the columns belong to
    /// `Option` that may contain global table space.
    pub global_table_spec: Option<TableSpec>,
    /// List of column specifications.
    pub col_specs: Vec<ColSpec>,
}

impl Serialize for RowsMetadata {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        // First we need assert that the flags match up with the data we were provided.
        // If they dont match up then it is impossible to encode.
        assert_eq!(
            self.flags.contains(RowsMetadataFlags::HAS_MORE_PAGES),
            self.paging_state.is_some()
        );

        match (
            self.flags.contains(RowsMetadataFlags::NO_METADATA),
            self.flags.contains(RowsMetadataFlags::GLOBAL_TABLE_SPACE),
        ) {
            (false, false) => {
                assert!(self.global_table_spec.is_none());
                assert!(!self.col_specs.is_empty());
            }
            (false, true) => {
                assert!(!self.col_specs.is_empty());
            }
            (true, _) => {
                assert!(self.global_table_spec.is_none());
                assert!(self.col_specs.is_empty());
            }
        }

        self.flags.serialize(cursor, version);

        self.columns_count.serialize(cursor, version);

        if let Some(paging_state) = &self.paging_state {
            paging_state.serialize(cursor, version);
        }

        if let Some(new_metadata_id) = &self.new_metadata_id {
            new_metadata_id.serialize(cursor, version);
        }

        if let Some(global_table_spec) = &self.global_table_spec {
            global_table_spec.serialize(cursor, version);
        }

        self.col_specs
            .iter()
            .for_each(|x| x.serialize(cursor, version));
    }
}

impl FromCursor for RowsMetadata {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<RowsMetadata> {
        let flags = RowsMetadataFlags::from_bits_truncate(CInt::from_cursor(cursor, version)?);
        let columns_count = CInt::from_cursor(cursor, version)?;

        let paging_state = if flags.contains(RowsMetadataFlags::HAS_MORE_PAGES) {
            Some(CBytes::from_cursor(cursor, version)?)
        } else {
            None
        };

        if flags.contains(RowsMetadataFlags::NO_METADATA) {
            return Ok(RowsMetadata {
                flags,
                columns_count,
                paging_state,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![],
            });
        }

        let new_metadata_id = if flags.contains(RowsMetadataFlags::METADATA_CHANGED) {
            Some(CBytesShort::from_cursor(cursor, version)?)
        } else {
            None
        };

        let has_global_table_space = flags.contains(RowsMetadataFlags::GLOBAL_TABLE_SPACE);
        let global_table_spec =
            extract_global_table_space(cursor, has_global_table_space, version)?;

        let col_specs =
            ColSpec::parse_colspecs(cursor, columns_count, has_global_table_space, version)?;

        Ok(RowsMetadata {
            flags,
            columns_count,
            paging_state,
            new_metadata_id,
            global_table_spec,
            col_specs,
        })
    }
}

bitflags! {
    pub struct RowsMetadataFlags: i32 {
        const GLOBAL_TABLE_SPACE = 0x0001;
        const HAS_MORE_PAGES = 0x0002;
        const NO_METADATA = 0x0004;
        const METADATA_CHANGED = 0x0008;
    }
}

impl Serialize for RowsMetadataFlags {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.bits().serialize(cursor, version)
    }
}

impl From<RowsMetadataFlags> for i32 {
    fn from(value: RowsMetadataFlags) -> Self {
        value.bits()
    }
}

impl FromBytes for RowsMetadataFlags {
    fn from_bytes(bytes: &[u8]) -> error::Result<RowsMetadataFlags> {
        try_u64_from_bytes(bytes).map_err(Into::into).and_then(|f| {
            RowsMetadataFlags::from_bits(f as i32)
                .ok_or_else(|| "Unexpected rows metadata flag".into())
        })
    }
}

/// Table specification.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct TableSpec {
    pub ks_name: String,
    pub table_name: String,
}

impl Serialize for TableSpec {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str(cursor, &self.ks_name, version);
        serialize_str(cursor, &self.table_name, version);
    }
}

impl FromCursor for TableSpec {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<Self> {
        let ks_name = from_cursor_str(cursor)?.to_string();
        let table_name = from_cursor_str(cursor)?.to_string();
        Ok(TableSpec {
            ks_name,
            table_name,
        })
    }
}

/// Single column specification.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct ColSpec {
    /// The initial <ks_name> and <table_name> are strings and only present
    /// if the Global_tables_spec flag is NOT set
    pub table_spec: Option<TableSpec>,
    /// Column name
    pub name: String,
    /// Column type defined in spec in 4.2.5.2
    pub col_type: ColTypeOption,
}

impl Serialize for ColSpec {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        if let Some(table_spec) = &self.table_spec {
            table_spec.serialize(cursor, version);
        }

        serialize_str(cursor, &self.name, version);
        self.col_type.serialize(cursor, version);
    }
}

impl ColSpec {
    pub fn parse_colspecs(
        cursor: &mut Cursor<&[u8]>,
        column_count: i32,
        has_global_table_space: bool,
        version: Version,
    ) -> error::Result<Vec<ColSpec>> {
        (0..column_count)
            .map(|_| {
                let table_spec = if !has_global_table_space {
                    Some(TableSpec::from_cursor(cursor, version)?)
                } else {
                    None
                };

                let name = from_cursor_str(cursor)?.to_string();
                let col_type = ColTypeOption::from_cursor(cursor, version)?;

                Ok(ColSpec {
                    table_spec,
                    name,
                    col_type,
                })
            })
            .collect::<Result<_, _>>()
    }
}

/// Cassandra data types which could be returned by a server.
#[derive(Debug, Clone, Display, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum ColType {
    Custom,
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
    Duration,
    List,
    Map,
    Set,
    Udt,
    Tuple,
    Null,
}

impl TryFrom<CIntShort> for ColType {
    type Error = Error;

    fn try_from(value: CIntShort) -> Result<Self, Self::Error> {
        match value {
            0x0000 => Ok(ColType::Custom),
            0x0001 => Ok(ColType::Ascii),
            0x0002 => Ok(ColType::Bigint),
            0x0003 => Ok(ColType::Blob),
            0x0004 => Ok(ColType::Boolean),
            0x0005 => Ok(ColType::Counter),
            0x0006 => Ok(ColType::Decimal),
            0x0007 => Ok(ColType::Double),
            0x0008 => Ok(ColType::Float),
            0x0009 => Ok(ColType::Int),
            0x000B => Ok(ColType::Timestamp),
            0x000C => Ok(ColType::Uuid),
            0x000D => Ok(ColType::Varchar),
            0x000E => Ok(ColType::Varint),
            0x000F => Ok(ColType::Timeuuid),
            0x0010 => Ok(ColType::Inet),
            0x0011 => Ok(ColType::Date),
            0x0012 => Ok(ColType::Time),
            0x0013 => Ok(ColType::Smallint),
            0x0014 => Ok(ColType::Tinyint),
            0x0015 => Ok(ColType::Duration),
            0x0020 => Ok(ColType::List),
            0x0021 => Ok(ColType::Map),
            0x0022 => Ok(ColType::Set),
            0x0030 => Ok(ColType::Udt),
            0x0031 => Ok(ColType::Tuple),
            0x0080 => Ok(ColType::Varchar),
            _ => Err(Error::UnexpectedColumnType(value)),
        }
    }
}

impl FromBytes for ColType {
    fn from_bytes(bytes: &[u8]) -> error::Result<ColType> {
        try_i16_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(ColType::try_from)
    }
}

impl Serialize for ColType {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        (match self {
            ColType::Custom => 0x0000,
            ColType::Ascii => 0x0001,
            ColType::Bigint => 0x0002,
            ColType::Blob => 0x0003,
            ColType::Boolean => 0x0004,
            ColType::Counter => 0x0005,
            ColType::Decimal => 0x0006,
            ColType::Double => 0x0007,
            ColType::Float => 0x0008,
            ColType::Int => 0x0009,
            ColType::Timestamp => 0x000B,
            ColType::Uuid => 0x000C,
            ColType::Varchar => 0x000D,
            ColType::Varint => 0x000E,
            ColType::Timeuuid => 0x000F,
            ColType::Inet => 0x0010,
            ColType::Date => 0x0011,
            ColType::Time => 0x0012,
            ColType::Smallint => 0x0013,
            ColType::Tinyint => 0x0014,
            ColType::Duration => 0x0015,
            ColType::List => 0x0020,
            ColType::Map => 0x0021,
            ColType::Set => 0x0022,
            ColType::Udt => 0x0030,
            ColType::Tuple => 0x0031,
            _ => 0x6666,
        } as CIntShort)
            .serialize(cursor, version);
    }
}

impl FromCursor for ColType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, _version: Version) -> error::Result<ColType> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let t = CIntShort::from_be_bytes(buff);
        t.try_into()
    }
}

/// Cassandra option that represent column type.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct ColTypeOption {
    /// Id refers to `ColType`.
    pub id: ColType,
    /// Values depending on column type.
    pub value: Option<ColTypeOptionValue>,
}

impl Serialize for ColTypeOption {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.id.serialize(cursor, version);
        if let Some(value) = &self.value {
            value.serialize(cursor, version);
        }
    }
}

impl FromCursor for ColTypeOption {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<ColTypeOption> {
        let id = ColType::from_cursor(cursor, version)?;
        let value = match id {
            ColType::Custom => Some(ColTypeOptionValue::CString(
                from_cursor_str(cursor)?.to_string(),
            )),
            ColType::Set => {
                let col_type = ColTypeOption::from_cursor(cursor, version)?;
                Some(ColTypeOptionValue::CSet(Box::new(col_type)))
            }
            ColType::List => {
                let col_type = ColTypeOption::from_cursor(cursor, version)?;
                Some(ColTypeOptionValue::CList(Box::new(col_type)))
            }
            ColType::Udt => Some(ColTypeOptionValue::UdtType(CUdt::from_cursor(
                cursor, version,
            )?)),
            ColType::Tuple => Some(ColTypeOptionValue::TupleType(CTuple::from_cursor(
                cursor, version,
            )?)),
            ColType::Map => {
                let name_type = ColTypeOption::from_cursor(cursor, version)?;
                let value_type = ColTypeOption::from_cursor(cursor, version)?;
                Some(ColTypeOptionValue::CMap(
                    Box::new(name_type),
                    Box::new(value_type),
                ))
            }
            _ => None,
        };

        Ok(ColTypeOption { id, value })
    }
}

/// Enum that represents all possible types of `value` of `ColTypeOption`.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub enum ColTypeOptionValue {
    CString(String),
    ColType(ColType),
    CSet(Box<ColTypeOption>),
    CList(Box<ColTypeOption>),
    UdtType(CUdt),
    TupleType(CTuple),
    CMap(Box<ColTypeOption>, Box<ColTypeOption>),
}

impl Serialize for ColTypeOptionValue {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        match self {
            Self::CString(c) => serialize_str(cursor, c, version),
            Self::ColType(c) => c.serialize(cursor, version),
            Self::CSet(c) => c.serialize(cursor, version),
            Self::CList(c) => c.serialize(cursor, version),
            Self::UdtType(c) => c.serialize(cursor, version),
            Self::TupleType(c) => c.serialize(cursor, version),
            Self::CMap(v1, v2) => {
                v1.serialize(cursor, version);
                v2.serialize(cursor, version);
            }
        }
    }
}

/// User defined type.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct CUdt {
    /// Keyspace name.
    pub ks: String,
    /// Udt name
    pub udt_name: String,
    /// List of pairs `(name, type)` where name is field name and type is type of field.
    pub descriptions: Vec<(String, ColTypeOption)>,
}

impl Serialize for CUdt {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        serialize_str(cursor, &self.ks, version);
        serialize_str(cursor, &self.udt_name, version);
        (self.descriptions.len() as i16).serialize(cursor, version);
        self.descriptions.iter().for_each(|(name, col_type)| {
            serialize_str(cursor, name, version);
            col_type.serialize(cursor, version);
        });
    }
}

impl FromCursor for CUdt {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<CUdt> {
        let ks = from_cursor_str(cursor)?.to_string();
        let udt_name = from_cursor_str(cursor)?.to_string();

        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let n = i16::from_be_bytes(buff);
        let mut descriptions = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let name = from_cursor_str(cursor)?.to_string();
            let col_type = ColTypeOption::from_cursor(cursor, version)?;
            descriptions.push((name, col_type));
        }

        Ok(CUdt {
            ks,
            udt_name,
            descriptions,
        })
    }
}

/// User defined type.
/// [Read more...](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L608)
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct CTuple {
    /// List of types.
    pub types: Vec<ColTypeOption>,
}

impl Serialize for CTuple {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        (self.types.len() as i16).serialize(cursor, version);
        self.types.iter().for_each(|f| f.serialize(cursor, version));
    }
}

impl FromCursor for CTuple {
    fn from_cursor(cursor: &mut Cursor<&[u8]>, version: Version) -> error::Result<CTuple> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let n = i16::from_be_bytes(buff);
        let mut types = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let col_type = ColTypeOption::from_cursor(cursor, version)?;
            types.push(col_type);
        }

        Ok(CTuple { types })
    }
}

/// The structure represents a body of a response envelope of type `prepared`
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Hash)]
pub struct BodyResResultPrepared {
    /// id of prepared request
    pub id: CBytesShort,
    /// result metadata id (only available since V5)
    pub result_metadata_id: Option<CBytesShort>,
    /// metadata
    pub metadata: PreparedMetadata,
    /// It is defined exactly the same as <metadata> in the Rows
    /// documentation.
    pub result_metadata: RowsMetadata,
}

impl Serialize for BodyResResultPrepared {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.id.serialize(cursor, version);

        if let Some(result_metadata_id) = &self.result_metadata_id {
            result_metadata_id.serialize(cursor, version);
        }

        self.metadata.serialize(cursor, version);
        self.result_metadata.serialize(cursor, version);
    }
}

impl BodyResResultPrepared {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<BodyResResultPrepared> {
        let id = CBytesShort::from_cursor(cursor, version)?;

        let result_metadata_id = if version == Version::V5 {
            Some(CBytesShort::from_cursor(cursor, version)?)
        } else {
            None
        };

        let metadata = PreparedMetadata::from_cursor(cursor, version)?;
        let result_metadata = RowsMetadata::from_cursor(cursor, version)?;

        Ok(BodyResResultPrepared {
            id,
            result_metadata_id,
            metadata,
            result_metadata,
        })
    }
}

bitflags! {
    pub struct PreparedMetadataFlags: i32 {
        const GLOBAL_TABLE_SPACE = 0x0001;
    }
}

impl Serialize for PreparedMetadataFlags {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        self.bits().serialize(cursor, version);
    }
}

/// The structure that represents metadata of prepared response.
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub struct PreparedMetadata {
    pub pk_indexes: Vec<i16>,
    pub global_table_spec: Option<TableSpec>,
    pub col_specs: Vec<ColSpec>,
}

impl Serialize for PreparedMetadata {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>, version: Version) {
        if self.global_table_spec.is_some() {
            PreparedMetadataFlags::GLOBAL_TABLE_SPACE
        } else {
            PreparedMetadataFlags::empty()
        }
        .serialize(cursor, version);

        let columns_count = self.col_specs.len() as i32;
        columns_count.serialize(cursor, version);

        let pk_count = self.pk_indexes.len() as i32;
        pk_count.serialize(cursor, version);

        self.pk_indexes
            .iter()
            .for_each(|f| f.serialize(cursor, version));

        if let Some(global_table_spec) = &self.global_table_spec {
            global_table_spec.serialize(cursor, version);
        }

        self.col_specs
            .iter()
            .for_each(|x| x.serialize(cursor, version));
    }
}

impl PreparedMetadata {
    fn from_cursor(
        cursor: &mut Cursor<&[u8]>,
        version: Version,
    ) -> error::Result<PreparedMetadata> {
        let flags = PreparedMetadataFlags::from_bits_truncate(CInt::from_cursor(cursor, version)?);
        let columns_count = CInt::from_cursor(cursor, version)?;

        let pk_count = if let Version::V3 = version {
            0
        } else {
            // v4 or v5
            CInt::from_cursor(cursor, version)?
        };

        let pk_indexes = (0..pk_count)
            .map(|_| {
                let mut buff = [0; SHORT_LEN];
                cursor.read_exact(&mut buff)?;

                Ok(i16::from_be_bytes(buff))
            })
            .collect::<Result<Vec<i16>, IoError>>()?;

        let has_global_table_space = flags.contains(PreparedMetadataFlags::GLOBAL_TABLE_SPACE);
        let global_table_spec =
            extract_global_table_space(cursor, has_global_table_space, version)?;
        let col_specs =
            ColSpec::parse_colspecs(cursor, columns_count, has_global_table_space, version)?;

        Ok(PreparedMetadata {
            pk_indexes,
            global_table_spec,
            col_specs,
        })
    }
}

fn extract_global_table_space(
    cursor: &mut Cursor<&[u8]>,
    has_global_table_space: bool,
    version: Version,
) -> error::Result<Option<TableSpec>> {
    Ok(if has_global_table_space {
        Some(TableSpec::from_cursor(cursor, version)?)
    } else {
        None
    })
}

//noinspection DuplicatedCode
#[cfg(test)]
fn test_encode_decode(bytes: &[u8], expected: ResResultBody) {
    {
        let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
        let result = ResResultBody::from_cursor(&mut cursor, Version::V4).unwrap();
        assert_eq!(expected, result);
    }

    {
        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);
        expected.serialize(&mut cursor, Version::V4);
        assert_eq!(buffer, bytes);
    }
}

#[cfg(test)]
mod cudt {
    use super::*;

    //noinspection DuplicatedCode
    #[test]
    fn cudt() {
        let bytes = &[
            0, 3, 98, 97, 114, // keyspace name - bar
            0, 3, 102, 111, 111, // udt_name - foo
            0, 2, // length
            // pair 1
            0, 3, 98, 97, 114, //name - bar
            0, 9, // col type int
            //
            // // pair 2
            0, 3, 102, 111, 111, // name - foo
            0, 9, // col type int
        ];
        let expected = CUdt {
            ks: "bar".into(),
            udt_name: "foo".into(),
            descriptions: vec![
                (
                    "bar".into(),
                    ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                ),
                (
                    "foo".into(),
                    ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                ),
            ],
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let udt = CUdt::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(udt, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod ctuple {
    use super::*;

    #[test]
    fn ctuple() {
        let bytes = &[0, 3, 0, 9, 0, 9, 0, 9];
        let expected = CTuple {
            types: vec![
                ColTypeOption {
                    id: ColType::Int,
                    value: None,
                };
                3
            ],
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let tuple = CTuple::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(tuple, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod col_spec {
    use super::*;

    #[test]
    fn col_spec_with_table_spec() {
        let bytes = &[
            // table spec
            0, 3, 98, 97, 114, // bar
            0, 3, 102, 111, 111, //foo
            //
            0, 3, 102, 111, 111, //name - foo
            //
            0, 9, // col type - int
        ];

        let expected = vec![ColSpec {
            table_spec: Some(TableSpec {
                ks_name: "bar".into(),
                table_name: "foo".into(),
            }),
            name: "foo".into(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        }];

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let col_spec = ColSpec::parse_colspecs(&mut cursor, 1, false, Version::V4).unwrap();
            assert_eq!(col_spec, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected[0].serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }

    #[test]
    fn col_spec_without_table_spec() {
        let bytes = &[
            0, 3, 102, 111, 111, //name - foo
            //
            0, 9, // col type - int
        ];
        let expected = vec![ColSpec {
            table_spec: None,
            name: "foo".into(),
            col_type: ColTypeOption {
                id: ColType::Int,
                value: None,
            },
        }];

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let col_spec = ColSpec::parse_colspecs(&mut cursor, 1, true, Version::V4).unwrap();
            assert_eq!(col_spec, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected[0].serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod col_type_option {
    use super::*;

    #[test]
    fn col_type_options_int() {
        let bytes = &[0, 9];
        let expected = ColTypeOption {
            id: ColType::Int,
            value: None,
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let col_type_option = ColTypeOption::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(col_type_option, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }

    #[test]
    fn col_type_options_map() {
        let bytes = &[0, 33, 0, 9, 0, 9];
        let expected = ColTypeOption {
            id: ColType::Map,
            value: Some(ColTypeOptionValue::CMap(
                Box::new(ColTypeOption {
                    id: ColType::Int,
                    value: None,
                }),
                Box::new(ColTypeOption {
                    id: ColType::Int,
                    value: None,
                }),
            )),
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let col_type_option = ColTypeOption::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(col_type_option, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod table_spec {
    use super::*;

    #[test]
    fn table_spec() {
        let bytes = &[
            0, 3, 98, 97, 114, // bar
            0, 3, 102, 111, 111, //foo
        ];
        let expected = TableSpec {
            ks_name: "bar".into(),
            table_name: "foo".into(),
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let table_spec = TableSpec::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(table_spec, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
mod void {
    use super::*;

    #[test]
    fn test_void() {
        let bytes = &[0, 0, 0, 1];
        let expected = ResResultBody::Void;
        test_encode_decode(bytes, expected);
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod rows_metadata {
    use super::*;

    #[test]
    fn rows_metadata() {
        let bytes = &[
            0, 0, 0, 8, // rows metadata flag
            0, 0, 0, 2, // columns count
            0, 1, 1, // new metadata id
            //
            // Col Spec 1
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // name
            0, 9, // col type id
            //
            // Col spec 2
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // name
            0, 19, // col type
        ];

        let expected = RowsMetadata {
            flags: RowsMetadataFlags::METADATA_CHANGED,

            columns_count: 2,
            paging_state: None,
            new_metadata_id: Some(CBytesShort::new(vec![1])),
            global_table_spec: None,
            col_specs: vec![
                ColSpec {
                    table_spec: Some(TableSpec {
                        ks_name: "ksname1".into(),
                        table_name: "tablename".into(),
                    }),
                    name: "foo".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: Some(TableSpec {
                        ks_name: "ksname1".into(),
                        table_name: "tablename".into(),
                    }),
                    name: "bar".into(),
                    col_type: ColTypeOption {
                        id: ColType::Smallint,
                        value: None,
                    },
                },
            ],
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let metadata = RowsMetadata::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(metadata, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod rows {
    use super::*;

    #[test]
    fn test_rows() {
        let bytes = &[
            0, 0, 0, 2, // rows flag
            0, 0, 0, 0, // rows metadata flag
            0, 0, 0, 2, // columns count
            //
            // Col Spec 1
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // name
            0, 9, // col type id
            //
            // Col spec 2
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // name
            0, 19, // col type
            0, 0, 0, 0, // rows count
        ];

        let expected = ResResultBody::Rows(BodyResResultRows {
            metadata: RowsMetadata {
                flags: RowsMetadataFlags::empty(),
                columns_count: 2,
                paging_state: None,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![
                    ColSpec {
                        table_spec: Some(TableSpec {
                            ks_name: "ksname1".into(),
                            table_name: "tablename".into(),
                        }),
                        name: "foo".into(),
                        col_type: ColTypeOption {
                            id: ColType::Int,
                            value: None,
                        },
                    },
                    ColSpec {
                        table_spec: Some(TableSpec {
                            ks_name: "ksname1".into(),
                            table_name: "tablename".into(),
                        }),
                        name: "bar".into(),
                        col_type: ColTypeOption {
                            id: ColType::Smallint,
                            value: None,
                        },
                    },
                ],
            },
            rows_count: 0,
            rows_content: vec![],
            protocol_version: Version::V4,
        });

        test_encode_decode(bytes, expected);
    }

    #[test]
    fn test_rows_no_metadata() {
        let bytes = &[
            0, 0, 0, 2, // rows flag
            0, 0, 0, 4, // rows metadata flag
            0, 0, 0, 3, // columns count
            0, 0, 0, 0, // rows count
        ];

        let expected = ResResultBody::Rows(BodyResResultRows {
            metadata: RowsMetadata {
                flags: RowsMetadataFlags::NO_METADATA,
                columns_count: 3,
                paging_state: None,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![],
            },
            rows_count: 0,
            rows_content: vec![],
            protocol_version: Version::V4,
        });

        test_encode_decode(bytes, expected);
    }
}

#[cfg(test)]
mod keyspace {
    use super::*;

    #[test]
    fn test_set_keyspace() {
        let bytes = &[
            0, 0, 0, 3, // keyspace flag
            0, 4, 98, 108, 97, 104, // blah
        ];

        let expected = ResResultBody::SetKeyspace(BodyResResultSetKeyspace {
            body: "blah".into(),
        });

        test_encode_decode(bytes, expected);
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod prepared_metadata {
    use super::*;

    #[test]
    fn prepared_metadata() {
        let bytes = &[
            0, 0, 0, 0, // global table space flag
            0, 0, 0, 2, // columns counts
            0, 0, 0, 1, // pk_count
            0, 0, // pk_index
            //
            // col specs
            // col spec 1
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // foo
            0, 9, // id
            //
            // col spec 2
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // bar
            0, 19, // id
        ];

        let expected = PreparedMetadata {
            pk_indexes: vec![0],
            global_table_spec: None,
            col_specs: vec![
                ColSpec {
                    table_spec: Some(TableSpec {
                        ks_name: "ksname1".into(),
                        table_name: "tablename".into(),
                    }),
                    name: "foo".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: Some(TableSpec {
                        ks_name: "ksname1".into(),
                        table_name: "tablename".into(),
                    }),
                    name: "bar".into(),
                    col_type: ColTypeOption {
                        id: ColType::Smallint,
                        value: None,
                    },
                },
            ],
        };

        {
            let mut cursor: Cursor<&[u8]> = Cursor::new(bytes);
            let metadata = PreparedMetadata::from_cursor(&mut cursor, Version::V4).unwrap();
            assert_eq!(metadata, expected);
        }

        {
            let mut buffer = Vec::new();
            let mut cursor = Cursor::new(&mut buffer);
            expected.serialize(&mut cursor, Version::V4);
            assert_eq!(buffer, bytes);
        }
    }
}

#[cfg(test)]
//noinspection DuplicatedCode
mod prepared {
    use super::*;
    use crate::types::{to_short, CBytesShort};

    #[test]
    fn test_prepared() {
        let bytes = &[
            0, 0, 0, 4, // prepared
            0, 2, 0, 1, // id
            //
            // prepared flags
            0, 0, 0, 0, // global table space flag
            0, 0, 0, 2, // columns counts
            0, 0, 0, 1, // pk_count
            0, 0, // pk_index
            //
            // col specs
            // col spec 1
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // foo
            0, 9, // id
            //
            // col spec 2
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // bar
            0, 19, // id
            //
            // rows metadata
            0, 0, 0, 0, // empty flags
            0, 0, 0, 2, // columns count
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 102, 111, 111, // foo
            0, 9, // int
            0, 7, 107, 115, 110, 97, 109, 101, 49, // ksname1
            0, 9, 116, 97, 98, 108, 101, 110, 97, 109, 101, // tablename
            0, 3, 98, 97, 114, // bar
            0, 19, // id
        ];

        let expected = ResResultBody::Prepared(BodyResResultPrepared {
            id: CBytesShort::new(to_short(1)),
            result_metadata_id: None,
            metadata: PreparedMetadata {
                pk_indexes: vec![0],
                global_table_spec: None,
                col_specs: vec![
                    ColSpec {
                        table_spec: Some(TableSpec {
                            ks_name: "ksname1".into(),
                            table_name: "tablename".into(),
                        }),
                        name: "foo".into(),
                        col_type: ColTypeOption {
                            id: ColType::Int,
                            value: None,
                        },
                    },
                    ColSpec {
                        table_spec: Some(TableSpec {
                            ks_name: "ksname1".into(),
                            table_name: "tablename".into(),
                        }),
                        name: "bar".into(),
                        col_type: ColTypeOption {
                            id: ColType::Smallint,
                            value: None,
                        },
                    },
                ],
            },
            result_metadata: RowsMetadata {
                flags: RowsMetadataFlags::empty(),
                columns_count: 2,
                paging_state: None,
                new_metadata_id: None,
                global_table_spec: None,
                col_specs: vec![
                    ColSpec {
                        table_spec: Some(TableSpec {
                            ks_name: "ksname1".into(),
                            table_name: "tablename".into(),
                        }),
                        name: "foo".into(),
                        col_type: ColTypeOption {
                            id: ColType::Int,
                            value: None,
                        },
                    },
                    ColSpec {
                        table_spec: Some(TableSpec {
                            table_name: "tablename".into(),
                            ks_name: "ksname1".into(),
                        }),
                        name: "bar".into(),
                        col_type: ColTypeOption {
                            id: ColType::Smallint,
                            value: None,
                        },
                    },
                ],
            },
        });

        test_encode_decode(bytes, expected);
    }
}

#[cfg(test)]
mod schema_change {
    use crate::frame::events::{SchemaChangeOptions, SchemaChangeTarget, SchemaChangeType};

    use super::*;

    #[test]
    fn test_schema_change() {
        let bytes = &[
            0, 0, 0, 5, // schema change
            0, 7, 67, 82, 69, 65, 84, 69, 68, // change type - created
            0, 8, 75, 69, 89, 83, 80, 65, 67, 69, // target keyspace
            0, 4, 98, 108, 97, 104, // options - blah
        ];

        let expected = ResResultBody::SchemaChange(SchemaChange {
            change_type: SchemaChangeType::Created,
            target: SchemaChangeTarget::Keyspace,
            options: SchemaChangeOptions::Keyspace("blah".into()),
        });

        test_encode_decode(bytes, expected);
    }
}
