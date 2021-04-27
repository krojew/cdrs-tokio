//! This trait provides functionality for derivation  `IntoCDRSBytes` trait implementation
//! for underlying

extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate rand;
extern crate syn;

mod common;
mod db_mirror;
mod into_cdrs_value;
mod try_from_row;
mod try_from_udt;

use db_mirror::impl_db_mirror;
use into_cdrs_value::impl_into_cdrs_value;
use proc_macro::TokenStream;
use try_from_row::impl_try_from_row;
use try_from_udt::impl_try_from_udt;

#[proc_macro_derive(DBMirror)]
pub fn db_mirror(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_db_mirror(&ast);

    // Return the generated impl
    gen.parse().unwrap()
}

#[proc_macro_derive(IntoCdrsValue)]
pub fn into_cdrs_value(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_into_cdrs_value(&ast);

    // Return the generated impl
    gen.parse().unwrap()
}

#[proc_macro_derive(TryFromRow)]
pub fn try_from_row(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_try_from_row(&ast);

    // Return the generated impl
    gen.parse().unwrap()
}

#[proc_macro_derive(TryFromUdt)]
pub fn try_from_udt(input: TokenStream) -> TokenStream {
    // Construct a string representation of the type definition
    let s = input.to_string();

    // Parse the string representation
    let ast = syn::parse_derive_input(&s).unwrap();

    // Build the impl
    let gen = impl_try_from_udt(&ast);

    // Return the generated impl
    gen.parse().unwrap()
}
