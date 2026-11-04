use proc_macro2::TokenStream;
use quote::*;
use syn::{DeriveInput, Result};

use crate::common::get_struct_fields;

pub fn impl_try_from_row(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let fields = get_struct_fields(ast)?;

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cdrs_tokio::frame::TryFromRow for #name #ty_generics #where_clause {
            fn try_from_row(cdrs: ::cdrs_tokio::types::rows::Row) -> ::cdrs_tokio::Result<Self> {
                ::core::result::Result::Ok(#name {
                  #(#fields),*
                })
            }
        }
    })
}
