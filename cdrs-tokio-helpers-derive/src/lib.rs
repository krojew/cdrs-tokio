//! This trait provides functionality for derivation  `IntoCDRSBytes` trait implementation
//! for underlying
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, Error};

mod common;
mod db_mirror;
mod into_cdrs_value;
mod try_from_row;
mod try_from_udt;

use crate::db_mirror::impl_db_mirror;
use crate::into_cdrs_value::impl_into_cdrs_value;
use crate::try_from_row::impl_try_from_row;
use crate::try_from_udt::impl_try_from_udt;

#[proc_macro_derive(DbMirror)]
pub fn db_mirror(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_db_mirror(&ast)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Implements `From<Struct> for Bytes`, which serializes the struct as a user defined type (UDT)
/// value, so it can be bound as a query value.
///
/// # Field order
///
/// A UDT value carries no field names: fields are written in the struct's declaration order, and
/// Cassandra assigns them to the fields of the type in the order of its `CREATE TYPE` statement
/// (including fields added later with `ALTER TYPE ... ADD`). The struct must therefore declare its
/// fields in exactly that order. A different order is not detected; values end up in the wrong
/// fields or are rejected by the server. Trailing fields of the type may be left out of the
/// struct, in which case they are null.
///
/// `TryFromUdt` reads fields by name, so the order does not matter when decoding.
///
/// `None` fields are written as null.
#[proc_macro_derive(IntoCdrsValue)]
pub fn into_cdrs_value(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_into_cdrs_value(&ast)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Implements `TryFromRow`, which decodes each field from the row column with the same name.
///
/// # Generic structs
///
/// The derive adds no trait bounds: the generated impl carries exactly the generic parameters and
/// where clause written on the struct, so every bound needed to decode a field must be declared
/// there.
///
/// Field types are recognized by the last segment of their path, so a field whose type is a bare
/// type parameter, such as `value: T`, is always decoded as a UDT and requires `T: TryFromUdt`.
/// The same holds for a type parameter used as the element type of a `Vec` or `Option` field, or
/// as the value type of a `HashMap` field. Such a parameter cannot stand for a primitive type like
/// `i32`.
#[proc_macro_derive(TryFromRow)]
pub fn try_from_row(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_try_from_row(&ast)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

/// Implements `TryFromUdt`, which decodes each field from the UDT field with the same name.
///
/// # Generic structs
///
/// The derive adds no trait bounds: the generated impl carries exactly the generic parameters and
/// where clause written on the struct, so every bound needed to decode a field must be declared
/// there.
///
/// Field types are recognized by the last segment of their path, so a field whose type is a bare
/// type parameter, such as `value: T`, is always decoded as a UDT and requires `T: TryFromUdt`.
/// The same holds for a type parameter used as the element type of a `Vec` or `Option` field, or
/// as the value type of a `HashMap` field. Such a parameter cannot stand for a primitive type like
/// `i32`.
#[proc_macro_derive(TryFromUdt)]
pub fn try_from_udt(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_try_from_udt(&ast)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}
