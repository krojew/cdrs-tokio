use quote;
use syn;
use syn::Field;

pub fn get_struct_fields(ast: &syn::DeriveInput) -> Vec<quote::Tokens> {
    struct_fields(ast)
        .iter()
        .map(|field| {
            let name = field.ident.clone().unwrap();
            let value = convert_field_into_rust(field.clone());
            quote! {
              #name: #value
            }
        })
        .collect()
}

pub fn struct_fields(ast: &syn::DeriveInput) -> &Vec<Field> {
    if let syn::Body::Struct(syn::VariantData::Struct(ref fields)) = ast.body {
        fields
    } else {
        panic!("The derive macro is defined for structs with named fields, not for enums or unit structs");
    }
}

pub fn get_map_params_string(ty: syn::Ty) -> (syn::Ty, syn::Ty) {
    match ty {
        syn::Ty::Path(_, syn::Path { segments, .. }) => match segments.last() {
            Some(&syn::PathSegment {
                parameters: syn::PathParameters::AngleBracketed(ref angle_bracketed_data),
                ..
            }) => {
                let bracket_types = angle_bracketed_data.types.clone();
                (
                    bracket_types
                        .first()
                        .expect("Cannot define Option type")
                        .clone(),
                    bracket_types
                        .last()
                        .expect("Cannot define Option type")
                        .clone(),
                )
            }
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {:?}", ty),
    }
}

fn convert_field_into_rust(field: syn::Field) -> quote::Tokens {
    let mut string_name = quote! {};
    string_name.append("\"");
    string_name.append(field.ident.clone().unwrap());
    string_name.append("\".trim()");
    let arguments = get_arguments(string_name);

    into_rust_with_args(field.ty, arguments)
}

fn get_arguments(name: quote::Tokens) -> quote::Tokens {
    quote! {
      &cdrs, #name
    }
}

fn into_rust_with_args(field_type: syn::Ty, arguments: quote::Tokens) -> quote::Tokens {
    let field_type_ident = get_cdrs_type_ident(field_type.clone());
    match field_type_ident.as_ref() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "Decimal"
        | "IpAddr" | "Uuid" | "Timespec" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime" => {
            quote! {
              #field_type_ident::from_cdrs_r(#arguments)?
            }
        }
        "cdrs_tokio::types::list::List" => {
            let list_as_rust = as_rust(field_type, quote! {list});

            quote! {
              match cdrs_tokio::types::list::List::from_cdrs_r(#arguments) {
                Ok(ref list) => {
                  #list_as_rust
                },
                _ => return Err("List should not be empty".into())
              }
            }
        }
        "cdrs_tokio::types::map::Map" => {
            let map_as_rust = as_rust(field_type, quote! {map});
            quote! {
              match cdrs_tokio::types::map::Map::from_cdrs_r(#arguments) {
                Ok(map) => {
                  #map_as_rust
                },
                _ => return Err("Map should not be empty".into())
              }
            }
        }
        "Option" => {
            let opt_type = get_ident_params_string(field_type);
            let opt_type_rustified = get_cdrs_type_ident(opt_type.clone());
            let opt_value_as_rust = as_rust(opt_type, quote! {opt_value});

            if is_non_zero_primitive(&opt_type_rustified) {
                quote! {
                  #opt_type_rustified::from_cdrs_by_name(#arguments)?
                }
            } else {
                quote! {
                  {
                    match #opt_type_rustified::from_cdrs_by_name(#arguments)? {
                      Some(opt_value) => {
                        let decoded = #opt_value_as_rust;
                        Some(decoded)
                      },
                      _ => None
                    }
                  }
                }
            }
        }
        _ => {
            quote! {
              #field_type::try_from_udt(cdrs_tokio::types::udt::Udt::from_cdrs_r(#arguments)?)?
            }
        }
    }
}

fn is_non_zero_primitive(ident: &syn::Ident) -> bool {
    matches!(
        ident.as_ref(),
        "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64"
    )
}

fn get_cdrs_type_ident(ty: syn::Ty) -> syn::Ident {
    let type_string = get_ident_string(ty);
    match type_string.as_str() {
        "Blob" => "Blob".into(),
        "String" => "String".into(),
        "bool" => "bool".into(),
        "i64" => "i64".into(),
        "i32" => "i32".into(),
        "i16" => "i16".into(),
        "i8" => "i8".into(),
        "f64" => "f64".into(),
        "f32" => "f32".into(),
        "Decimal" => "Decimal".into(),
        "IpAddr" => "IpAddr".into(),
        "Uuid" => "Uuid".into(),
        "Timespec" => "Timespec".into(),
        "PrimitiveDateTime" => "PrimitiveDateTime".into(),
        "Vec" => "cdrs_tokio::types::list::List".into(),
        "HashMap" => "cdrs_tokio::types::map::Map".into(),
        "Option" => "Option".into(),
        "NonZeroI8" => "NonZeroI8".into(),
        "NonZeroI16" => "NonZeroI16".into(),
        "NonZeroI32" => "NonZeroI32".into(),
        "NonZeroI64" => "NonZeroI64".into(),
        "NaiveDateTime" => "NaiveDateTime".into(),
        "DateTime" => "DateTime".into(),
        _ => "cdrs_tokio::types::udt::Udt".into(),
    }
}

fn get_ident(ty: syn::Ty) -> syn::Ident {
    match ty {
        syn::Ty::Path(_, syn::Path { segments, .. }) => match segments.last() {
            Some(&syn::PathSegment { ref ident, .. }) => ident.clone(),
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {:?}", ty),
    }
}

// returns single value decoded and optionally iterative mapping that uses decoded value
fn as_rust(ty: syn::Ty, val: quote::Tokens) -> quote::Tokens {
    let cdrs_type = get_cdrs_type_ident(ty.clone());
    match cdrs_type.as_ref() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "IpAddr"
        | "Uuid" | "Timespec" | "Decimal" | "PrimitiveDateTime" => val,
        "cdrs_tokio::types::list::List" => {
            let vec_type = get_ident_params_string(ty);
            let inter_rust_type = get_cdrs_type_ident(vec_type.clone());
            let decoded_item = as_rust(vec_type.clone(), quote! {item});
            quote! {
              {
                let inner: Vec<#inter_rust_type> = #val.as_rust_type()?.unwrap();
                let mut decoded: Vec<#vec_type> = Vec::with_capacity(inner.len());
                for item in inner {
                  decoded.push(#decoded_item);
                }
                decoded
              }
            }
        }
        "cdrs_tokio::types::map::Map" => {
            let (map_key_type, map_value_type) = get_map_params_string(ty);
            let inter_rust_type = get_cdrs_type_ident(map_value_type.clone());
            let decoded_item = as_rust(map_value_type.clone(), quote! {val});
            quote! {
              {
                let inner: std::collections::HashMap<#map_key_type, #inter_rust_type> = #val.as_rust_type()?.unwrap();
                let mut decoded: std::collections::HashMap<#map_key_type, #map_value_type> = std::collections::HashMap::with_capacity(inner.len());
                for (key, val) in inner {
                  decoded.insert(key, #decoded_item);
                }
                decoded
              }
            }
        }
        "Option" => {
            let opt_type = get_ident_params_string(ty);
            as_rust(opt_type, val)
        }
        _ => {
            quote! {
              #ty::try_from_udt(#val)?
            }
        }
    }
}

pub fn get_ident_string(ty: syn::Ty) -> String {
    get_ident(ty).as_ref().into()
}

pub fn get_ident_params_string(ty: syn::Ty) -> syn::Ty {
    match ty {
        syn::Ty::Path(_, syn::Path { segments, .. }) => match segments.last() {
            Some(&syn::PathSegment {
                parameters: syn::PathParameters::AngleBracketed(ref angle_bracketed_data),
                ..
            }) => angle_bracketed_data
                .types
                .last()
                .expect("Cannot define Option type")
                .clone(),
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {:?}", ty),
    }
}
