use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::*;
use syn::spanned::Spanned;
use syn::{DeriveInput, Error, Result};

use crate::common::struct_fields;

pub fn impl_db_mirror(ast: &DeriveInput) -> Result<TokenStream> {
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
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
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn insert_query() -> &'static str {
                concat!("insert into ", stringify!(#name), "(",
                  #names,
                 ") values (",
                 #question_marks,
                 ")")
            }

            pub fn into_query_values(self) -> ::cdrs_tokio::query::QueryValues {
                let mut values: ::std::collections::HashMap<::std::string::String, ::cdrs_tokio::types::value::Value> =
                    ::std::collections::HashMap::new();

                #(
                    values.insert(
                        ::std::string::String::from(stringify!(#idents)),
                        ::core::convert::Into::into(self.#idents_copy),
                    );
                )*

                ::cdrs_tokio::query::QueryValues::NamedValues(values)
            }
        }
    })
}
