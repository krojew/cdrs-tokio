use derive_more::Constructor;
use num::BigInt;
use std::net::IpAddr;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::frame::frame_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::udt::Udt;
use crate::types::{AsRust, AsRustType, CBytes};

// TODO: consider using pointers to ColTypeOption and Vec<CBytes> instead of owning them.
#[derive(Debug, Constructor)]
pub struct List {
    /// column spec of the list, i.e. id should be List as it's a list and value should contain
    /// a type of list items.
    metadata: ColTypeOption,
    data: Vec<CBytes>,
}

impl List {
    fn map<T, F>(&self, f: F) -> Vec<T>
    where
        F: FnMut(&CBytes) -> T,
    {
        self.data.iter().map(f).collect()
    }
}

impl AsRust for List {}

list_as_rust!(Blob);
list_as_rust!(String);
list_as_rust!(bool);
list_as_rust!(i64);
list_as_rust!(i32);
list_as_rust!(i16);
list_as_rust!(i8);
list_as_rust!(f64);
list_as_rust!(f32);
list_as_rust!(IpAddr);
list_as_rust!(Uuid);
list_as_rust!(List);
list_as_rust!(Map);
list_as_rust!(Udt);
list_as_rust!(Tuple);
list_as_rust!(Decimal);
list_as_rust!(BigInt);

list_as_cassandra_type!();
