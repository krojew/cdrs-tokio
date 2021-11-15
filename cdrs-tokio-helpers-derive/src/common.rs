use proc_macro2::{Literal, TokenStream};
use quote::*;
use syn::{
    parse_str, Data, DataStruct, DeriveInput, Field, Fields, GenericArgument, Ident, Path,
    PathArguments, PathSegment, Type, TypePath,
};

pub fn get_struct_fields(ast: &DeriveInput) -> Vec<TokenStream> {
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

pub fn struct_fields(ast: &DeriveInput) -> &Fields {
    if let Data::Struct(DataStruct { fields, .. }) = &ast.data {
        fields
    } else {
        panic!("The derive macro is defined for structs with named fields, not for enums or unit structs");
    }
}

fn extract_type(arg: &GenericArgument) -> Type {
    match arg {
        GenericArgument::Type(ty) => ty.clone(),
        _ => panic!("Expected type argument!"),
    }
}

pub fn get_map_params_string(ty: &Type) -> (Type, Type) {
    match ty {
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => match segments.last() {
            Some(&PathSegment {
                arguments: PathArguments::AngleBracketed(ref angle_bracketed_data),
                ..
            }) => (
                extract_type(
                    angle_bracketed_data
                        .args
                        .first()
                        .expect("Cannot extract map key type"),
                ),
                extract_type(
                    angle_bracketed_data
                        .args
                        .last()
                        .expect("Cannot extract map value type"),
                ),
            ),
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {}", get_ident_string(ty)),
    }
}

fn remove_r(s: String) -> String {
    if let Some(s) = s.strip_prefix("r#") {
        s.to_string()
    } else {
        s
    }
}

fn convert_field_into_rust(field: Field) -> TokenStream {
    let mut string_name = quote! {};
    let s = remove_r(field.ident.unwrap().to_string());
    string_name.append(Literal::string(s.trim()));
    let arguments = get_arguments(string_name);

    into_rust_with_args(&field.ty, arguments)
}

fn get_arguments(name: TokenStream) -> TokenStream {
    quote! {
      &cdrs, #name
    }
}

fn into_rust_with_args(field_type: &Type, arguments: TokenStream) -> TokenStream {
    let field_type_ident = get_cdrs_type(field_type);
    match get_ident_string(&field_type_ident).as_str() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "Decimal"
        | "IpAddr" | "Uuid" | "Timespec" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime" => {
            quote! {
              #field_type_ident::from_cdrs_r(#arguments)?
            }
        }
        "List" => {
            let list_as_rust = as_rust(field_type, quote! {list});

            quote! {
              match cassandra_protocol::types::list::List::from_cdrs_r(#arguments) {
                Ok(ref list) => {
                  #list_as_rust
                },
                _ => return Err("List should not be empty".into())
              }
            }
        }
        "Map" => {
            let map_as_rust = as_rust(field_type, quote! {map});
            quote! {
              match cassandra_protocol::types::map::Map::from_cdrs_r(#arguments) {
                Ok(map) => {
                  #map_as_rust
                },
                _ => return Err("Map should not be empty".into())
              }
            }
        }
        "Option" => {
            let opt_type = get_ident_params_string(field_type);
            let opt_type_rustified = get_cdrs_type(&opt_type);
            let opt_value_as_rust = as_rust(&opt_type, quote! {opt_value});

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
              #field_type::try_from_udt(cassandra_protocol::types::udt::Udt::from_cdrs_r(#arguments)?)?
            }
        }
    }
}

fn is_non_zero_primitive(ty: &Type) -> bool {
    matches!(
        get_ident_string(ty).as_str(),
        "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64"
    )
}

fn get_cdrs_type(ty: &Type) -> Type {
    let type_string = get_ident_string(ty);
    match type_string.as_str() {
        "Blob" => parse_str("Blob").unwrap(),
        "String" => parse_str("String").unwrap(),
        "bool" => parse_str("bool").unwrap(),
        "i64" => parse_str("i64").unwrap(),
        "i32" => parse_str("i32").unwrap(),
        "i16" => parse_str("i16").unwrap(),
        "i8" => parse_str("i8").unwrap(),
        "f64" => parse_str("f64").unwrap(),
        "f32" => parse_str("f32").unwrap(),
        "Decimal" => parse_str("Decimal").unwrap(),
        "IpAddr" => parse_str("IpAddr").unwrap(),
        "Uuid" => parse_str("Uuid").unwrap(),
        "Timespec" => parse_str("Timespec").unwrap(),
        "PrimitiveDateTime" => parse_str("PrimitiveDateTime").unwrap(),
        "Vec" => parse_str("cassandra_protocol::types::list::List").unwrap(),
        "HashMap" => parse_str("cassandra_protocol::types::map::Map").unwrap(),
        "Option" => parse_str("Option").unwrap(),
        "NonZeroI8" => parse_str("NonZeroI8").unwrap(),
        "NonZeroI16" => parse_str("NonZeroI16").unwrap(),
        "NonZeroI32" => parse_str("NonZeroI32").unwrap(),
        "NonZeroI64" => parse_str("NonZeroI64").unwrap(),
        "NaiveDateTime" => parse_str("NaiveDateTime").unwrap(),
        "DateTime" => parse_str("DateTime").unwrap(),
        _ => parse_str("cassandra_protocol::types::udt::Udt").unwrap(),
    }
}

fn get_ident(ty: &Type) -> &Ident {
    match ty {
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => match segments.last() {
            Some(&PathSegment { ref ident, .. }) => ident,
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {}", get_ident_string(ty)),
    }
}

// returns single value decoded and optionally iterative mapping that uses decoded value
fn as_rust(ty: &Type, val: TokenStream) -> TokenStream {
    let cdrs_type = get_cdrs_type(ty);
    match get_ident_string(&cdrs_type).as_str() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "IpAddr"
        | "Uuid" | "Timespec" | "Decimal" | "PrimitiveDateTime" => val,
        "List" => {
            let vec_type = get_ident_params_string(ty);
            let inter_rust_type = get_cdrs_type(&vec_type);
            let decoded_item = as_rust(&vec_type, quote! {item});
            quote! {
              {
                let inner: Vec<#inter_rust_type> = #val.as_r_type()?;
                let mut decoded: Vec<#vec_type> = Vec::with_capacity(inner.len());
                for item in inner {
                  decoded.push(#decoded_item);
                }
                decoded
              }
            }
        }
        "Map" => {
            let (map_key_type, map_value_type) = get_map_params_string(ty);
            let inter_rust_type = get_cdrs_type(&map_value_type);
            let decoded_item = as_rust(&map_value_type, quote! {val});
            quote! {
              {
                let inner: std::collections::HashMap<#map_key_type, #inter_rust_type> = #val.as_r_type()?;
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
            as_rust(&opt_type, val)
        }
        _ => {
            quote! {
              #ty::try_from_udt(#val)?
            }
        }
    }
}

pub fn get_ident_string(ty: &Type) -> String {
    get_ident(ty).to_string()
}

pub fn get_ident_params_string(ty: &Type) -> Type {
    match ty {
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => match segments.last() {
            Some(&PathSegment {
                arguments: PathArguments::AngleBracketed(ref angle_bracketed_data),
                ..
            }) => match angle_bracketed_data.args.last() {
                Some(GenericArgument::Type(ty)) => ty.clone(),
                _ => panic!("Cannot infer field type"),
            },
            _ => panic!("Cannot infer field type"),
        },
        _ => panic!("Cannot infer field type {}", get_ident_string(ty)),
    }
}
