use proc_macro2::TokenStream;
use quote::*;
use syn::{Data, DataStruct, DeriveInput};

use crate::common::get_ident_string;

pub fn impl_into_cdrs_value(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    if let Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let convert_into_bytes = fields.iter().map(|field| {
      let field_ident = field.ident.clone().unwrap();
      return if get_ident_string(&field.ty).as_str() == "Option" {
        quote! {
          match value.#field_ident {
            Some(ref val) => {
              let field_bytes: Self = val.clone().into();
              cassandra_protocol::types::value::Value::new_normal(field_bytes).serialize(&mut cursor);
            },
            None => {
              cassandra_protocol::types::value::Value::new_not_set().serialize(&mut cursor);
            }
          }
        }
      } else {
        quote! {
          let field_bytes: Self = value.#field_ident.into();
          cassandra_protocol::types::value::Value::new_normal(field_bytes).serialize(&mut cursor);
        }
      }
    });
        // As Value has following implementation impl<T: Into<Bytes>> From<T> for Value
        // for a struct it's enough to implement Into<Bytes> in order to be convertible into Value
        // which is used for making queries
        quote! {
            impl From<#name> for cassandra_protocol::types::value::Bytes {
              fn from(value: #name) -> Self {
                let mut bytes: Vec<u8> = Vec::new();
                let mut cursor = std::io::Cursor::new(&mut bytes);
                #(#convert_into_bytes)*
                Self::new(bytes)
              }
            }
        }
    } else {
        panic!("#[derive(IntoCdrsValue)] is only defined for structs, not for enums!");
    }
}
