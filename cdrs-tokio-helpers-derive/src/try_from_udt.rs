use proc_macro2::TokenStream;
use quote::*;
use syn::{DeriveInput, Result};

use crate::common::get_struct_fields;

pub fn impl_try_from_udt(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let fields = get_struct_fields(ast)?;

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics cdrs_tokio::frame::TryFromUdt for #name #ty_generics #where_clause {
            fn try_from_udt(cdrs: cdrs_tokio::types::udt::Udt) -> cdrs_tokio::Result<Self> {
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
