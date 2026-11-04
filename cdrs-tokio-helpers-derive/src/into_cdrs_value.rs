use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::*;
use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Error, Result};

use crate::common::get_ident_string;

pub fn impl_into_cdrs_value(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    if let Data::Struct(DataStruct { ref fields, .. }) = ast.data {
        let convert_into_bytes: Vec<_> = fields
            .iter()
            .map(|field| {
                let field_ident = field.ident.clone().ok_or_else(|| {
                    Error::new(field.span(), "IntoCdrsValue requires all fields be named!")
                })?;
                get_ident_string(&field.ty, &field_ident.to_string()).map(|ident| {
                    if ident == "Option" {
                        // We are assuming here primitive value serialization will not change across protocol
                        // versions, which gives us simpler user API.
                        quote! {
                          match value.#field_ident {
                            ::core::option::Option::Some(val) => {
                              let field_bytes: Self = ::core::convert::Into::into(val);
                              cdrs_tokio::frame::Serialize::serialize(
                                &cdrs_tokio::types::value::Value::new(field_bytes),
                                &mut cursor,
                                cdrs_tokio::frame::Version::V4,
                              );
                            },
                            ::core::option::Option::None => {
                              cdrs_tokio::frame::Serialize::serialize(
                                &cdrs_tokio::types::value::Value::Null,
                                &mut cursor,
                                cdrs_tokio::frame::Version::V4,
                              );
                            }
                          }
                        }
                    } else {
                        quote! {
                          let field_bytes: Self = ::core::convert::Into::into(value.#field_ident);
                          cdrs_tokio::frame::Serialize::serialize(
                            &cdrs_tokio::types::value::Value::new(field_bytes),
                            &mut cursor,
                            cdrs_tokio::frame::Version::V4,
                          );
                        }
                    }
                })
            })
            .try_collect()?;
        // As Value has following implementation impl<T: Into<Bytes>> From<T> for Value
        // for a struct it's enough to implement Into<Bytes> in order to be convertible into Value
        // which is used for making queries
        Ok(quote! {
            #[automatically_derived]
            impl #impl_generics ::core::convert::From<#name #ty_generics> for cdrs_tokio::types::value::Bytes #where_clause {
              fn from(value: #name #ty_generics) -> Self {
                let mut bytes: ::std::vec::Vec<u8> = ::std::vec::Vec::new();
                let mut cursor = ::std::io::Cursor::new(&mut bytes);
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
