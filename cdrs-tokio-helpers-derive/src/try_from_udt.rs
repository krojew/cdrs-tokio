use proc_macro2::TokenStream;
use quote::*;
use syn::{DeriveInput, Result};

use crate::common::get_struct_fields;

pub fn impl_try_from_udt(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let fields = get_struct_fields(ast)?;

    Ok(quote! {
        impl cdrs_tokio::frame::TryFromUdt for #name {
            fn try_from_udt(cdrs: cdrs_tokio::types::udt::Udt) -> cdrs_tokio::Result<Self> {
                #[allow(unused_imports)]
                use cdrs_tokio::frame::TryFromUdt;
                #[allow(unused_imports)]
                use cdrs_tokio::types::from_cdrs::FromCdrsByName;
                #[allow(unused_imports)]
                use cdrs_tokio::types::IntoRustByName;
                #[allow(unused_imports)]
                use cdrs_tokio::types::AsRustType;

                Ok(#name {
                  #(#fields),*
                })
            }
        }
    })
}
