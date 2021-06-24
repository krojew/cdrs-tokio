//! This trait provides functionality for derivation  `IntoCDRSBytes` trait implementation
//! for underlying
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod common;
mod db_mirror;
mod into_cdrs_value;
mod try_from_row;
mod try_from_udt;

use crate::db_mirror::impl_db_mirror;
use crate::into_cdrs_value::impl_into_cdrs_value;
use crate::try_from_row::impl_try_from_row;
use crate::try_from_udt::impl_try_from_udt;

#[proc_macro_derive(DBMirror)]
pub fn db_mirror(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_db_mirror(&ast).into()
}

#[proc_macro_derive(IntoCdrsValue)]
pub fn into_cdrs_value(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_into_cdrs_value(&ast).into()
}

#[proc_macro_derive(TryFromRow)]
pub fn try_from_row(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_try_from_row(&ast).into()
}

#[proc_macro_derive(TryFromUdt)]
pub fn try_from_udt(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the impl
    impl_try_from_udt(&ast).into()
}
