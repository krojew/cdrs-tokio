use num::BigInt;
use std::collections::HashMap;
use std::net::IpAddr;

use super::prelude::{Blob, Decimal};

#[derive(Debug)]
pub enum CassandraType {
    Ascii(String),
    Bigint(BigInt),
    Blob(Blob),
    Boolean(bool),
    Counter(i64),
    Decimal(Decimal),
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64),
    Uuid(uuid::Uuid),
    Varchar(String),
    Varint(BigInt),
    Timeuuid(uuid::Uuid),
    Inet(IpAddr),
    Date(i32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
    List(Vec<CassandraType>),
    Map(Vec<(CassandraType, CassandraType)>),
    Set(Vec<CassandraType>),
    Udt(HashMap<String, CassandraType>),
    Tuple(Vec<CassandraType>),
    Null,
}
