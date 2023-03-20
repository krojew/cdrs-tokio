use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::*;
use syn::spanned::Spanned;
use syn::{DeriveInput, Error, Result};

use crate::common::struct_fields;

pub fn impl_db_mirror(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let idents: Vec<_> = struct_fields(ast)?
        .named
        .iter()
        .map(|f| {
            f.ident
                .clone()
                .ok_or_else(|| Error::new(f.span(), "Expected a named field!"))
        })
        .try_collect()?;
    let idents_copy = idents.clone();

    let fields = idents
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<String>>();
    let names = fields.join(", ");
    let question_marks = fields
        .iter()
        .map(|_| "?".to_string())
        .collect::<Vec<String>>()
        .join(", ");

    Ok(quote! {
        impl #name {
            pub fn insert_query() -> &'static str {
                concat!("insert into ", stringify!(#name), "(",
                  #names,
                 ") values (",
                 #question_marks,
                 ")")
            }

            pub fn into_query_values(self) -> cdrs_tokio::query::QueryValues {
                use std::collections::HashMap;
                let mut values: HashMap<String, cdrs_tokio::types::value::Value> = HashMap::new();

                #(
                    values.insert(stringify!(#idents).to_string(), self.#idents_copy.into());
                )*

                cdrs_tokio::query::QueryValues::NamedValues(values)
            }
        }
    })
}
