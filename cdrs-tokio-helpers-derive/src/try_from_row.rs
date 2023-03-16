use proc_macro2::TokenStream;
use quote::*;
use syn::{DeriveInput, Result};

use crate::common::get_struct_fields;

pub fn impl_try_from_row(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let fields = get_struct_fields(ast)?;

    Ok(quote! {
        #[automatically_derived]
        impl cdrs_tokio::frame::TryFromRow for #name {
            fn try_from_row(cdrs: cdrs_tokio::types::rows::Row) -> cdrs_tokio::Result<Self> {
                use cdrs_tokio::frame::TryFromUdt;
                use cdrs_tokio::types::from_cdrs::FromCdrsByName;
                use cdrs_tokio::types::IntoRustByName;
                use cdrs_tokio::types::AsRustType;

                Ok(#name {
                  #(#fields),*
                })
            }
        }
    })
}
