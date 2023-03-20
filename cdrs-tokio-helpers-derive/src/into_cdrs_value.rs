use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::*;
use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Error, Result};

use crate::common::get_ident_string;

pub fn impl_into_cdrs_value(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    if let Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let convert_into_bytes: Vec<_> = fields.iter().map(|field| {
            let field_ident = field.ident.clone().ok_or_else(|| Error::new(field.span(), "IntoCdrsValue requires all fields be named!"))?;
            get_ident_string(&field.ty, &field_ident.to_string()).map(|ident| {
                if ident == "Option" {
                    // We are assuming here primitive value serialization will not change across protocol
                    // versions, which gives us simpler user API.
                    quote! {
                  match value.#field_ident {
                    Some(ref val) => {
                      let field_bytes: Self = val.clone().into();
                      cdrs_tokio::types::value::Value::new(field_bytes).serialize(&mut cursor, cdrs_tokio::frame::Version::V4);
                    },
                    None => {
                      cdrs_tokio::types::value::Value::NotSet.serialize(&mut cursor, cdrs_tokio::frame::Version::V4);
                    }
                  }
                }
                } else {
                    quote! {
                  let field_bytes: Self = value.#field_ident.into();
                  cdrs_tokio::types::value::Value::new(field_bytes).serialize(&mut cursor, cdrs_tokio::frame::Version::V4);
                }
                }
            })
        }).try_collect()?;
        // As Value has following implementation impl<T: Into<Bytes>> From<T> for Value
        // for a struct it's enough to implement Into<Bytes> in order to be convertible into Value
        // which is used for making queries
        Ok(quote! {
            impl From<#name> for cdrs_tokio::types::value::Bytes {
              fn from(value: #name) -> Self {
                #[allow(unused_imports)]
                use cdrs_tokio::frame::Serialize;

                let mut bytes: Vec<u8> = Vec::new();
                let mut cursor = std::io::Cursor::new(&mut bytes);
                #(#convert_into_bytes)*
                Self::new(bytes)
              }
            }
        })
    } else {
        Err(Error::new(
            ast.span(),
            "#[derive(IntoCdrsValue)] can only be defined for structs!",
        ))
    }
}
