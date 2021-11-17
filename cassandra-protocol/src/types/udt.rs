use std::collections::HashMap;
use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};

use chrono::prelude::*;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::{column_is_empty_err, Error, Result};
use crate::frame::frame_result::{CUdt, ColType, ColTypeOption, ColTypeOptionValue};
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::{ByName, CBytes, IntoRustByName};
use num::BigInt;

#[derive(Clone, Debug)]
pub struct Udt {
    data: HashMap<String, (ColTypeOption, CBytes)>,
}

impl Udt {
    pub fn new(data: Vec<CBytes>, metadata: &CUdt) -> Udt {
        let meta_iter = metadata.descriptions.iter();

        let acc: HashMap<String, (ColTypeOption, CBytes)> =
            HashMap::with_capacity(metadata.descriptions.len());
        let d = meta_iter.zip(data.iter()).fold(acc, |mut a, v| {
            let (m, val_b) = v;
            let &(ref name_b, ref val_type) = m;
            let name = name_b.as_plain();
            a.insert(name, (val_type.clone(), val_b.clone()));
            a
        });

        Udt { data: d }
    }
}

impl ByName for Udt {}

into_rust_by_name!(Udt, Blob);
into_rust_by_name!(Udt, String);
into_rust_by_name!(Udt, bool);
into_rust_by_name!(Udt, i64);
into_rust_by_name!(Udt, i32);
into_rust_by_name!(Udt, i16);
into_rust_by_name!(Udt, i8);
into_rust_by_name!(Udt, f64);
into_rust_by_name!(Udt, f32);
into_rust_by_name!(Udt, IpAddr);
into_rust_by_name!(Udt, Uuid);
into_rust_by_name!(Udt, List);
into_rust_by_name!(Udt, Map);
into_rust_by_name!(Udt, Udt);
into_rust_by_name!(Udt, Tuple);
into_rust_by_name!(Udt, PrimitiveDateTime);
into_rust_by_name!(Udt, Decimal);
into_rust_by_name!(Udt, NonZeroI8);
into_rust_by_name!(Udt, NonZeroI16);
into_rust_by_name!(Udt, NonZeroI32);
into_rust_by_name!(Udt, NonZeroI64);
into_rust_by_name!(Udt, NaiveDateTime);
into_rust_by_name!(Udt, DateTime<Utc>);
into_rust_by_name!(Udt, BigInt);
