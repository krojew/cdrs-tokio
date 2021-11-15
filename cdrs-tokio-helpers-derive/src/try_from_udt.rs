use proc_macro2::TokenStream;
use quote::*;
use syn::DeriveInput;

use crate::common::get_struct_fields;

pub fn impl_try_from_udt(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = get_struct_fields(ast);
    quote! {
        impl TryFromUdt for #name {
          fn try_from_udt(cdrs: cassandra_protocol::types::udt::Udt) -> cassandra_protocol::Result<Self> {
            Ok(#name {
              #(#fields),*
            })
          }
        }
    }
}
