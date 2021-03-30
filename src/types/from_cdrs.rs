use std::net::IpAddr;
use std::num::{NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8};

use chrono::prelude::*;
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::error::Result as CdrsResult;
use crate::types::blob::Blob;
use crate::types::decimal::Decimal;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::tuple::Tuple;
use crate::types::udt::Udt;
use crate::types::{AsRustType, ByName, IntoRustByName};

pub trait FromCdrs {
    fn from_cdrs<T>(cdrs_type: T) -> CdrsResult<Option<Self>>
    where
        Self: Sized,
        T: AsRustType<Self>,
    {
        cdrs_type.as_rust_type()
    }

    fn from_cdrs_r<T>(cdrs_type: T) -> CdrsResult<Self>
    where
        Self: Sized,
        T: AsRustType<Self>,
    {
        cdrs_type.as_r_type()
    }
}

impl FromCdrs for Blob {}
impl FromCdrs for String {}
impl FromCdrs for bool {}
impl FromCdrs for i64 {}
impl FromCdrs for i32 {}
impl FromCdrs for i16 {}
impl FromCdrs for i8 {}
impl FromCdrs for f64 {}
impl FromCdrs for f32 {}
impl FromCdrs for IpAddr {}
impl FromCdrs for Uuid {}
impl FromCdrs for List {}
impl FromCdrs for Map {}
impl FromCdrs for Udt {}
impl FromCdrs for Tuple {}
impl FromCdrs for PrimitiveDateTime {}
impl FromCdrs for Decimal {}
impl FromCdrs for NonZeroI8 {}
impl FromCdrs for NonZeroI16 {}
impl FromCdrs for NonZeroI32 {}
impl FromCdrs for NonZeroI64 {}
impl FromCdrs for NaiveDateTime {}
impl<Tz: TimeZone> FromCdrs for DateTime<Tz> {}

pub trait FromCdrsByName {
    fn from_cdrs_by_name<T>(cdrs_type: &T, name: &str) -> CdrsResult<Option<Self>>
    where
        Self: Sized,
        T: ByName + IntoRustByName<Self>,
    {
        cdrs_type.by_name(name)
    }

    fn from_cdrs_r<T>(cdrs_type: &T, name: &str) -> CdrsResult<Self>
    where
        Self: Sized,
        T: ByName + IntoRustByName<Self> + ::std::fmt::Debug,
    {
        cdrs_type.r_by_name(name)
    }
}

impl FromCdrsByName for Blob {}
impl FromCdrsByName for String {}
impl FromCdrsByName for bool {}
impl FromCdrsByName for i64 {}
impl FromCdrsByName for i32 {}
impl FromCdrsByName for i16 {}
impl FromCdrsByName for i8 {}
impl FromCdrsByName for f64 {}
impl FromCdrsByName for f32 {}
impl FromCdrsByName for IpAddr {}
impl FromCdrsByName for Uuid {}
impl FromCdrsByName for List {}
impl FromCdrsByName for Map {}
impl FromCdrsByName for Udt {}
impl FromCdrsByName for Tuple {}
impl FromCdrsByName for PrimitiveDateTime {}
impl FromCdrsByName for Decimal {}
impl FromCdrsByName for NonZeroI8 {}
impl FromCdrsByName for NonZeroI16 {}
impl FromCdrsByName for NonZeroI32 {}
impl FromCdrsByName for NonZeroI64 {}
impl FromCdrsByName for NaiveDateTime {}
impl<Tz: TimeZone> FromCdrsByName for DateTime<Tz> {}
