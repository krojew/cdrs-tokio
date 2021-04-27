use syn;
use quote;

use common::get_ident_string;

pub fn impl_into_cdrs_value(ast: &syn::DeriveInput) -> quote::Tokens {
  let name = &ast.ident;
  if let syn::Body::Struct(syn::VariantData::Struct(ref fields)) = ast.body {
    let convert_into_bytes = fields.iter().map(|field| {
      let field_ident = field.ident.clone().unwrap();
      return if get_ident_string(field.ty.clone()).as_str() == "Option" {
        quote! {
          match value.#field_ident {
            Some(ref val) => {
              let field_bytes: Self = val.clone().into();
              bytes.append(&mut cdrs_tokio::types::value::Value::new_normal(field_bytes).as_bytes());
            },
            None => {
              bytes.append(&mut cdrs_tokio::types::value::Value::new_not_set().as_bytes());
            }
          }
        }
      } else {
        quote! {
          let field_bytes: Self = value.#field_ident.into();
          bytes.append(&mut cdrs_tokio::types::value::Value::new_normal(field_bytes).as_bytes());
        }
      }
    });
    // As Value has following implementation impl<T: Into<Bytes>> From<T> for Value
    // for a struct it's enough to implement Into<Bytes> in order to be convertible into Value
    // which is used for making queries
    quote! {
        impl From<#name> for cdrs_tokio::types::value::Bytes {
          fn from(value: #name) -> Self {
            let mut bytes: Vec<u8> = vec![];
            #(#convert_into_bytes)*
            Self::new(bytes)
          }
        }
    }
  } else {
    panic!("#[derive(IntoCdrsValue)] is only defined for structs, not for enums!");
  }
}
