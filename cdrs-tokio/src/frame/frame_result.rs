use bitflags::bitflags;
use derive_more::{Constructor, Display};
use std::convert::{TryFrom, TryInto};
use std::io::{Cursor, Error as IoError, Read};

use crate::error;
use crate::error::Error;
use crate::frame::events::SchemaChange;
use crate::frame::{FromBytes, FromCursor, Serialize};
use crate::types::rows::Row;
use crate::types::*;

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
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        i32::from(*self).serialize(cursor);
    }
}

impl FromBytes for ResultKind {
    fn from_bytes(bytes: &[u8]) -> error::Result<ResultKind> {
        try_i32_from_bytes(bytes)
            .map_err(Into::into)
            .and_then(ResultKind::try_from)
    }
}

impl From<ResultKind> for i32 {
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

impl TryFrom<i32> for ResultKind {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0x0001 => Ok(ResultKind::Void),
            0x0002 => Ok(ResultKind::Rows),
            0x0003 => Ok(ResultKind::SetKeyspace),
            0x0004 => Ok(ResultKind::Prepared),
            0x0005 => Ok(ResultKind::SchemaChange),
            _ => Err(format!("Unexpected result kind: {}", value).into()),
        }
    }
}

impl FromCursor for ResultKind {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<ResultKind> {
        let mut buff = [0; INT_LEN];
        cursor.read_exact(&mut buff)?;

        let rk = i32::from_be_bytes(buff);
        rk.try_into()
    }
}

/// `ResponseBody` is a generalized enum that represents all types of responses. Each of enum
/// option wraps related body type.
#[derive(Debug)]
pub enum ResResultBody {
    /// Void response body. It's an empty struct.
    Void(BodyResResultVoid),
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

impl ResResultBody {
    fn parse_body_from_cursor(
        cursor: &mut Cursor<&[u8]>,
        result_kind: ResultKind,
    ) -> error::Result<ResResultBody> {
        Ok(match result_kind {
            ResultKind::Void => ResResultBody::Void(BodyResResultVoid::from_cursor(cursor)?),
            ResultKind::Rows => ResResultBody::Rows(BodyResResultRows::from_cursor(cursor)?),
            ResultKind::SetKeyspace => {
                ResResultBody::SetKeyspace(BodyResResultSetKeyspace::from_cursor(cursor)?)
            }
            ResultKind::Prepared => {
                ResResultBody::Prepared(BodyResResultPrepared::from_cursor(cursor)?)
            }
            ResultKind::SchemaChange => {
                ResResultBody::SchemaChange(SchemaChange::from_cursor(cursor)?)
            }
        })
    }

    /// It converts body into `Vec<Row>` if body's type is `Row` and returns `None` otherwise.
    pub fn into_rows(self) -> Option<Vec<Row>> {
        match self {
            ResResultBody::Rows(rows_body) => Some(Row::from_frame_body(rows_body)),
            _ => None,
        }
    }

    /// It returns `Some` rows metadata if frame result is of type rows and `None` otherwise
    pub fn as_rows_metadata(&self) -> Option<RowsMetadata> {
        match *self {
            ResResultBody::Rows(ref rows_body) => Some(rows_body.metadata.clone()),
            _ => None,
        }
    }

    /// It unwraps body and returns BodyResResultPrepared which contains an exact result of
    /// PREPARE query.
    pub fn into_prepared(self) -> Option<BodyResResultPrepared> {
        match self {
            ResResultBody::Prepared(p) => Some(p),
            _ => None,
        }
    }

    /// It unwraps body and returns BodyResResultSetKeyspace which contains an exact result of
    /// use keyspace query.
    pub fn into_set_keyspace(self) -> Option<BodyResResultSetKeyspace> {
        match self {
            ResResultBody::SetKeyspace(p) => Some(p),
            _ => None,
        }
    }
}

impl FromCursor for ResResultBody {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<ResResultBody> {
        let result_kind = ResultKind::from_cursor(cursor)?;
        ResResultBody::parse_body_from_cursor(cursor, result_kind)
    }
}

/// Body of a response of type Void
#[derive(Debug, Default, Copy, Clone)]
pub struct BodyResResultVoid;

impl FromBytes for BodyResResultVoid {
    fn from_bytes(_bytes: &[u8]) -> error::Result<BodyResResultVoid> {
        // as it's empty by definition just create BodyResVoid
        let body: BodyResResultVoid = Default::default();
        Ok(body)
    }
}

impl FromCursor for BodyResResultVoid {
    fn from_cursor(_cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultVoid> {
        Ok(Default::default())
    }
}

/// It represents set keyspace result body. Body contains keyspace name.
#[derive(Debug, Constructor)]
pub struct BodyResResultSetKeyspace {
    /// It contains name of keyspace that was set.
    pub body: CString,
}

impl FromCursor for BodyResResultSetKeyspace {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultSetKeyspace> {
        CString::from_cursor(cursor).map(BodyResResultSetKeyspace::new)
    }
}

/// Structure that represents result of type
/// [rows](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L533).
#[derive(Debug)]
pub struct BodyResResultRows {
    /// Rows metadata
    pub metadata: RowsMetadata,
    /// Number of rows.
    pub rows_count: CInt,
    /// From spec: it is composed of `rows_count` of rows.
    pub rows_content: Vec<Vec<CBytes>>,
}

impl BodyResResultRows {
    fn rows_content(
        cursor: &mut Cursor<&[u8]>,
        rows_count: i32,
        columns_count: i32,
    ) -> error::Result<Vec<Vec<CBytes>>> {
        (0..rows_count)
            .map(|_| {
                (0..columns_count)
                    .map(|_| CBytes::from_cursor(cursor))
                    .collect::<Result<_, _>>()
            })
            .collect::<Result<_, _>>()
    }
}

impl FromCursor for BodyResResultRows {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultRows> {
        let metadata = RowsMetadata::from_cursor(cursor)?;
        let rows_count = CInt::from_cursor(cursor)?;
        let rows_content =
            BodyResResultRows::rows_content(cursor, rows_count, metadata.columns_count)?;

        Ok(BodyResResultRows {
            metadata,
            rows_count,
            rows_content,
        })
    }
}

/// Rows metadata.
#[derive(Debug, Clone)]
pub struct RowsMetadata {
    /// Flags.
    pub flags: RowsMetadataFlags,
    /// Number of columns.
    pub columns_count: i32,
    /// Paging state.
    pub paging_state: Option<CBytes>,
    // In fact by specification Vec should have only two elements representing the
    // (unique) keyspace name and table name the columns belong to
    /// `Option` that may contain global table space.
    pub global_table_spec: Option<(CString, CString)>,
    /// List of column specifications.
    pub col_specs: Vec<ColSpec>,
}

impl FromCursor for RowsMetadata {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<RowsMetadata> {
        let flags = RowsMetadataFlags::from_bits_truncate(CInt::from_cursor(cursor)?);
        let columns_count = CInt::from_cursor(cursor)?;

        let paging_state = if flags.contains(RowsMetadataFlags::HAS_MORE_PAGES) {
            Some(CBytes::from_cursor(cursor)?)
        } else {
            None
        };

        let has_global_table_space = flags.contains(RowsMetadataFlags::GLOBAL_TABLE_SPACE);
        let global_table_spec = extract_global_table_space(cursor, has_global_table_space)?;

        let col_specs = ColSpec::parse_colspecs(cursor, columns_count, has_global_table_space)?;

        Ok(RowsMetadata {
            flags,
            columns_count,
            paging_state,
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
    }
}

impl Serialize for RowsMetadataFlags {
    #[inline]
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        self.bits().serialize(cursor)
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

/// Single column specification.
#[derive(Debug, Clone)]
pub struct ColSpec {
    /// The initial <ks_name> is a string and is only present
    /// if the Global_tables_spec flag is NOT set
    pub ks_name: Option<CString>,
    /// The initial <table_name> is a string and is present
    /// if the Global_tables_spec flag is NOT set
    pub table_name: Option<CString>,
    /// Column name
    pub name: CString,
    /// Column type defined in spec in 4.2.5.2
    pub col_type: ColTypeOption,
}

impl ColSpec {
    pub fn parse_colspecs(
        cursor: &mut Cursor<&[u8]>,
        column_count: i32,
        has_global_table_space: bool,
    ) -> error::Result<Vec<ColSpec>> {
        (0..column_count)
            .map(|_| {
                let ks_name: Option<CString> = if !has_global_table_space {
                    Some(CString::from_cursor(cursor)?)
                } else {
                    None
                };

                let table_name = if !has_global_table_space {
                    Some(CString::from_cursor(cursor)?)
                } else {
                    None
                };

                let name = CString::from_cursor(cursor)?;
                let col_type = ColTypeOption::from_cursor(cursor)?;

                Ok(ColSpec {
                    ks_name,
                    table_name,
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
    List,
    Map,
    Set,
    Udt,
    Tuple,
    Null,
}

impl TryFrom<i16> for ColType {
    type Error = Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
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
            0x0020 => Ok(ColType::List),
            0x0021 => Ok(ColType::Map),
            0x0022 => Ok(ColType::Set),
            0x0030 => Ok(ColType::Udt),
            0x0031 => Ok(ColType::Tuple),
            _ => Err("Unexpected column type".into()),
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

impl FromCursor for ColType {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<ColType> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let t = i16::from_be_bytes(buff);
        t.try_into()
    }
}

/// Cassandra option that represent column type.
#[derive(Debug, Clone)]
pub struct ColTypeOption {
    /// Id refers to `ColType`.
    pub id: ColType,
    /// Values depending on column type.
    pub value: Option<ColTypeOptionValue>,
}

impl FromCursor for ColTypeOption {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<ColTypeOption> {
        let id = ColType::from_cursor(cursor)?;
        let value = match id {
            ColType::Custom => Some(ColTypeOptionValue::CString(CString::from_cursor(cursor)?)),
            ColType::Set => {
                let col_type = ColTypeOption::from_cursor(cursor)?;
                Some(ColTypeOptionValue::CSet(Box::new(col_type)))
            }
            ColType::List => {
                let col_type = ColTypeOption::from_cursor(cursor)?;
                Some(ColTypeOptionValue::CList(Box::new(col_type)))
            }
            ColType::Udt => Some(ColTypeOptionValue::UdtType(CUdt::from_cursor(cursor)?)),
            ColType::Tuple => Some(ColTypeOptionValue::TupleType(CTuple::from_cursor(cursor)?)),
            ColType::Map => {
                let name_type = ColTypeOption::from_cursor(cursor)?;
                let value_type = ColTypeOption::from_cursor(cursor)?;
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
#[derive(Debug, Clone)]
pub enum ColTypeOptionValue {
    CString(CString),
    ColType(ColType),
    CSet(Box<ColTypeOption>),
    CList(Box<ColTypeOption>),
    UdtType(CUdt),
    TupleType(CTuple),
    CMap(Box<ColTypeOption>, Box<ColTypeOption>),
}

/// User defined type.
#[derive(Debug, Clone)]
pub struct CUdt {
    /// Keyspace name.
    pub ks: CString,
    /// Udt name
    pub udt_name: CString,
    /// List of pairs `(name, type)` where name is field name and type is type of field.
    pub descriptions: Vec<(CString, ColTypeOption)>,
}

impl FromCursor for CUdt {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<CUdt> {
        let ks = CString::from_cursor(cursor)?;
        let udt_name = CString::from_cursor(cursor)?;

        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let n = i16::from_be_bytes(buff);
        let mut descriptions = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let name = CString::from_cursor(cursor)?;
            let col_type = ColTypeOption::from_cursor(cursor)?;
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
#[derive(Debug, Clone)]
pub struct CTuple {
    /// List of types.
    pub types: Vec<ColTypeOption>,
}

impl FromCursor for CTuple {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<CTuple> {
        let mut buff = [0; SHORT_LEN];
        cursor.read_exact(&mut buff)?;

        let n = i16::from_be_bytes(buff);
        let mut types = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let col_type = ColTypeOption::from_cursor(cursor)?;
            types.push(col_type);
        }

        Ok(CTuple { types })
    }
}

/// The structure represents a body of a response frame of type `prepared`
#[derive(Debug)]
pub struct BodyResResultPrepared {
    /// id of prepared request
    pub id: CBytesShort,
    /// metadata
    pub metadata: PreparedMetadata,
    /// It is defined exactly the same as <metadata> in the Rows
    /// documentation.
    pub result_metadata: RowsMetadata,
}

impl FromCursor for BodyResResultPrepared {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyResResultPrepared> {
        let id = CBytesShort::from_cursor(cursor)?;
        let metadata = PreparedMetadata::from_cursor(cursor)?;
        let result_metadata = RowsMetadata::from_cursor(cursor)?;

        Ok(BodyResResultPrepared {
            id,
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

/// The structure that represents metadata of prepared response.
#[derive(Debug)]
pub struct PreparedMetadata {
    pub flags: PreparedMetadataFlags,
    pub columns_count: i32,
    pub pk_count: i32,
    pub pk_indexes: Vec<i16>,
    pub global_table_spec: Option<(CString, CString)>,
    pub col_specs: Vec<ColSpec>,
}

impl FromCursor for PreparedMetadata {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<PreparedMetadata> {
        let flags = PreparedMetadataFlags::from_bits_truncate(CInt::from_cursor(cursor)?);
        let columns_count = CInt::from_cursor(cursor)?;
        let pk_count = if cfg!(feature = "v3") {
            0
        } else {
            // v4 or v5
            CInt::from_cursor(cursor)?
        };
        let pk_indexes = (0..pk_count)
            .map(|_| {
                let mut buff = [0; SHORT_LEN];
                cursor.read_exact(&mut buff)?;

                Ok(i16::from_be_bytes(buff))
            })
            .collect::<Result<Vec<i16>, IoError>>()?;

        let has_global_table_space = flags.contains(PreparedMetadataFlags::GLOBAL_TABLE_SPACE);
        let global_table_spec = extract_global_table_space(cursor, has_global_table_space)?;
        let col_specs = ColSpec::parse_colspecs(cursor, columns_count, has_global_table_space)?;

        Ok(PreparedMetadata {
            flags,
            columns_count,
            pk_count,
            pk_indexes,
            global_table_spec,
            col_specs,
        })
    }
}

fn extract_global_table_space(
    cursor: &mut Cursor<&[u8]>,
    has_global_table_space: bool,
) -> error::Result<Option<(CString, CString)>> {
    Ok(if has_global_table_space {
        let keyspace = CString::from_cursor(cursor)?;
        let table_name = CString::from_cursor(cursor)?;
        Some((keyspace, table_name))
    } else {
        None
    })
}
