use itertools::Itertools;
use proc_macro2::{Literal, TokenStream};
use quote::*;
use syn::spanned::Spanned;
use syn::{
    parse_str, Data, DataStruct, DeriveInput, Error, Field, Fields, FieldsNamed, GenericArgument,
    Ident, Path, PathArguments, PathSegment, Result, Type, TypePath, TypeReference,
};

pub fn get_struct_fields(ast: &DeriveInput) -> Result<Vec<TokenStream>> {
    struct_fields(ast)?
        .named
        .iter()
        .map(|field| {
            let name = field
                .ident
                .clone()
                .ok_or_else(|| Error::new(field.span(), "Expected a named field!"))?;
            let value = convert_field_into_rust(field.clone())?;
            Ok(quote! {
              #name: #value
            })
        })
        .try_collect()
}

pub fn struct_fields(ast: &DeriveInput) -> Result<&FieldsNamed> {
    if let Data::Struct(DataStruct {
        fields: Fields::Named(fields),
        ..
    }) = &ast.data
    {
        Ok(fields)
    } else {
        Err(Error::new(ast.span(), "The derive macro is defined for structs with named fields, not for enums or unit structs"))
    }
}

fn extract_type(arg: &GenericArgument) -> Result<Type> {
    match arg {
        GenericArgument::Type(ty) => Ok(ty.clone()),
        _ => Err(Error::new(arg.span(), "Expected type argument!")),
    }
}

pub fn get_map_params_string(ty: &Type, name: &str) -> Result<(Type, Type)> {
    match ty {
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => {
            match segments.last() {
                Some(&PathSegment {
                    arguments: PathArguments::AngleBracketed(ref angle_bracketed_data),
                    ..
                }) => {
                    Ok((
                        extract_type(angle_bracketed_data.args.first().ok_or_else(|| {
                            Error::new(ty.span(), "Cannot extract map key type")
                        })?)?,
                        extract_type(angle_bracketed_data.args.last().ok_or_else(|| {
                            Error::new(ty.span(), "Cannot extract map value type")
                        })?)?,
                    ))
                }
                _ => Err(Error::new(ty.span(), "Cannot infer field type")),
            }
        }
        _ => Err(Error::new(
            ty.span(),
            format!("Cannot infer field type {}", get_ident_string(ty, name)?),
        )),
    }
}

fn remove_r(s: String) -> String {
    if let Some(s) = s.strip_prefix("r#") {
        s.to_string()
    } else {
        s
    }
}

fn convert_field_into_rust(field: Field) -> Result<TokenStream> {
    let mut string_name = quote! {};
    let span = field.span();
    let s = remove_r(
        field
            .ident
            .ok_or_else(|| Error::new(span, "Expected named field!"))?
            .to_string(),
    );
    string_name.append(Literal::string(s.trim()));
    let arguments = get_arguments(string_name);

    into_rust_with_args(&field.ty, arguments, &s)
}

fn get_arguments(name: TokenStream) -> TokenStream {
    quote! {
      &cdrs, #name
    }
}

fn into_rust_with_args(
    field_type: &Type,
    arguments: TokenStream,
    name: &str,
) -> Result<TokenStream> {
    let field_type_ident = get_cdrs_type(field_type, name)?;
    Ok(match get_ident_string(&field_type_ident, name)?.as_str() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "Decimal"
        | "IpAddr" | "Uuid" | "Timespec" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime" => {
            quote! {
              #field_type_ident::from_cdrs_r(#arguments)?
            }
        }
        "List" => {
            let list_as_rust = as_rust(field_type, quote! {list}, name)?;

            quote! {
              match cdrs_tokio::types::list::List::from_cdrs_r(#arguments) {
                Ok(ref list) => {
                  #list_as_rust
                },
                _ => return Err("List should not be empty".into())
              }
            }
        }
        "Map" => {
            let map_as_rust = as_rust(field_type, quote! {map}, name)?;
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
            let opt_type = get_ident_params_string(field_type, name)?;
            let opt_type_rustified = get_cdrs_type(&opt_type, name)?;
            let opt_value_as_rust = as_rust(&opt_type, quote! {opt_value}, name)?;

            if is_non_zero_primitive(&opt_type_rustified, name)? {
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
        _ => quote! {
          #field_type::try_from_udt(cdrs_tokio::types::udt::Udt::from_cdrs_r(#arguments)?)?
        },
    })
}

fn is_non_zero_primitive(ty: &Type, name: &str) -> Result<bool> {
    get_ident_string(ty, name).map(|ident| {
        matches!(
            ident.as_str(),
            "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64"
        )
    })
}

fn get_cdrs_type(ty: &Type, name: &str) -> Result<Type> {
    let type_string = get_ident_string(ty, name)?;
    Ok(match type_string.as_str() {
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
        "Vec" => parse_str("cdrs_tokio::types::list::List").unwrap(),
        "HashMap" => parse_str("cdrs_tokio::types::map::Map").unwrap(),
        "Option" => parse_str("Option").unwrap(),
        "NonZeroI8" => parse_str("NonZeroI8").unwrap(),
        "NonZeroI16" => parse_str("NonZeroI16").unwrap(),
        "NonZeroI32" => parse_str("NonZeroI32").unwrap(),
        "NonZeroI64" => parse_str("NonZeroI64").unwrap(),
        "NaiveDateTime" => parse_str("NaiveDateTime").unwrap(),
        "DateTime" => parse_str("DateTime").unwrap(),
        _ => parse_str("cdrs_tokio::types::udt::Udt").unwrap(),
    })
}

fn get_ident<'a>(ty: &'a Type, name: &str) -> Result<&'a Ident> {
    match ty {
        Type::Reference(TypeReference { elem, .. }) => get_ident(elem, name),
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => match segments.last() {
            Some(PathSegment { ident, .. }) => Ok(ident),
            _ => Err(Error::new(
                ty.span(),
                format!("Cannot infer field type: {}", name),
            )),
        },
        _ => Err(Error::new(
            ty.span(),
            format!("Cannot infer field type: {}", name),
        )),
    }
}

// returns single value decoded and optionally iterative mapping that uses decoded value
fn as_rust(ty: &Type, val: TokenStream, name: &str) -> Result<TokenStream> {
    let cdrs_type = get_cdrs_type(ty, name)?;
    Ok(match get_ident_string(&cdrs_type, name)?.as_str() {
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "IpAddr"
        | "Uuid" | "Timespec" | "Decimal" | "PrimitiveDateTime" => val,
        "List" => {
            let vec_type = get_ident_params_string(ty, name)?;
            let inter_rust_type = get_cdrs_type(&vec_type, name)?;
            let decoded_item = as_rust(&vec_type, quote! {item}, name)?;
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
            let (map_key_type, map_value_type) = get_map_params_string(ty, name)?;
            let inter_rust_type = get_cdrs_type(&map_value_type, name)?;
            let decoded_item = as_rust(&map_value_type, quote! {val}, name)?;
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
            let opt_type = get_ident_params_string(ty, name)?;
            as_rust(&opt_type, val, name)?
        }
        _ => {
            quote! {
              #ty::try_from_udt(#val)?
            }
        }
    })
}

pub fn get_ident_string(ty: &Type, name: &str) -> Result<String> {
    get_ident(ty, name).map(|ident| ident.to_string())
}

pub fn get_ident_params_string(ty: &Type, name: &str) -> Result<Type> {
    match ty {
        Type::Path(TypePath {
            path: Path { segments, .. },
            ..
        }) => match segments.last() {
            Some(&PathSegment {
                arguments: PathArguments::AngleBracketed(ref angle_bracketed_data),
                ..
            }) => match angle_bracketed_data.args.last() {
                Some(GenericArgument::Type(ty)) => Ok(ty.clone()),
                _ => Err(Error::new(ty.span(), "Cannot infer field type")),
            },
            _ => Err(Error::new(ty.span(), "Cannot infer field type")),
        },
        _ => Err(Error::new(
            ty.span(),
            format!("Cannot infer field type {}", get_ident_string(ty, name)?),
        )),
    }
}
