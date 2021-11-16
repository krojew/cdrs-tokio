use proc_macro2::TokenStream;
use quote::*;
use syn::DeriveInput;

use crate::common::get_struct_fields;

pub fn impl_try_from_row(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = get_struct_fields(ast);

    quote! {
        impl TryFromRow for #name {
          fn try_from_row(cdrs: cdrs_tokio::types::rows::Row) -> cdrs_tokio::Result<Self> {
            Ok(#name {
              #(#fields),*
            })
          }
        }
    }
}
