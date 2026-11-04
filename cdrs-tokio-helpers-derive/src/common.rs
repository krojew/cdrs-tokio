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

/// Returns the key, value and optional hasher types of a `HashMap<K, V, S>`.
pub fn get_map_params_string(ty: &Type, name: &str) -> Result<(Type, Type, Option<Type>)> {
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
                    let mut args = angle_bracketed_data.args.iter();
                    Ok((
                        extract_type(args.next().ok_or_else(|| {
                            Error::new(ty.span(), "Cannot extract map key type")
                        })?)?,
                        extract_type(args.next().ok_or_else(|| {
                            Error::new(ty.span(), "Cannot extract map value type")
                        })?)?,
                        args.next().map(extract_type).transpose()?,
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
        | "IpAddr" | "Uuid" | "Timespec" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime"
        | "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64" => {
            quote! {
              <#field_type_ident as cdrs_tokio::types::from_cdrs::FromCdrsByName>::from_cdrs_r(#arguments)?
            }
        }
        // Cassandra stores an empty non-frozen collection as null.
        "List" => {
            let list_as_rust = as_rust(field_type, quote! {list}, name)?;

            quote! {
              match cdrs_tokio::types::list::List::from_cdrs_by_name(#arguments)? {
                Some(ref list) => {
                  #list_as_rust
                },
                None => Default::default()
              }
            }
        }
        "Map" => {
            let map_as_rust = as_rust(field_type, quote! {map}, name)?;
            quote! {
              match cdrs_tokio::types::map::Map::from_cdrs_by_name(#arguments)? {
                Some(map) => {
                  #map_as_rust
                },
                None => Default::default()
              }
            }
        }
        "Option" => {
            let opt_type = get_ident_params_string(field_type, name)?;
            let opt_type_rustified = get_cdrs_type(&opt_type, name)?;
            let opt_value_as_rust = as_rust(&opt_type, quote! {opt_value}, name)?;

            if is_non_zero_primitive(&opt_type_rustified, name)? {
                quote! {
                  <#opt_type_rustified as cdrs_tokio::types::from_cdrs::FromCdrsByName>::from_cdrs_by_name(#arguments)?
                }
            } else {
                quote! {
                  {
                    match <#opt_type_rustified as cdrs_tokio::types::from_cdrs::FromCdrsByName>::from_cdrs_by_name(#arguments)? {
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
          <#field_type as cdrs_tokio::frame::TryFromUdt>::try_from_udt(cdrs_tokio::types::udt::Udt::from_cdrs_r(#arguments)?)?
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
    if !matches!(ty, Type::Path(_)) {
        return Err(Error::new(
            ty.span(),
            format!(
                "Unsupported type of field `{}`: expected a primitive type, Vec, HashMap, Option \
                 or a type implementing TryFromUdt",
                name
            ),
        ));
    }

    let type_string = get_ident_string(ty, name)?;
    Ok(match type_string.as_str() {
        // the original type keeps any path and generic arguments needed to resolve it
        "Blob" | "String" | "bool" | "i64" | "i32" | "i16" | "i8" | "f64" | "f32" | "Decimal"
        | "IpAddr" | "Uuid" | "Timespec" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime"
        | "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64" => ty.clone(),
        "Vec" => parse_str("cdrs_tokio::types::list::List").unwrap(),
        "HashMap" => parse_str("cdrs_tokio::types::map::Map").unwrap(),
        "Option" => parse_str("Option").unwrap(),
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
        | "Uuid" | "Timespec" | "Decimal" | "PrimitiveDateTime" | "NaiveDateTime" | "DateTime"
        | "NonZeroI8" | "NonZeroI16" | "NonZeroI32" | "NonZeroI64" => val,
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
            let (map_key_type, map_value_type, map_hasher_type) = get_map_params_string(ty, name)?;
            let inter_rust_type = get_cdrs_type(&map_value_type, name)?;
            let decoded_item = as_rust(&map_value_type, quote! {val}, name)?;
            let map_hasher_type = map_hasher_type.map(|hasher| quote! {, #hasher});
            quote! {
              {
                let inner: std::collections::HashMap<#map_key_type, #inter_rust_type> = #val.as_r_type()?;
                let mut decoded: std::collections::HashMap<#map_key_type, #map_value_type #map_hasher_type> = std::collections::HashMap::with_capacity_and_hasher(inner.len(), Default::default());
                for (key, val) in inner {
                  decoded.insert(key, #decoded_item);
                }
                decoded
              }
            }
        }
        "Option" => {
            return Err(Error::new(
                ty.span(),
                format!(
                    "Unsupported type of field `{}`: Option is only supported as the outermost \
                     type of a field",
                    name
                ),
            ))
        }
        _ => {
            quote! {
              <#ty as cdrs_tokio::frame::TryFromUdt>::try_from_udt(#val)?
            }
        }
    })
}

pub fn get_ident_string(ty: &Type, name: &str) -> Result<String> {
    get_ident(ty, name).map(|ident| ident.to_string())
}

#[cfg(test)]
mod tests {
    use super::get_struct_fields;
    use syn::{parse_quote, DeriveInput};

    fn field_error(ast: DeriveInput) -> String {
        match get_struct_fields(&ast) {
            Ok(fields) => panic!("expected an error, got {}", quote::quote! { #(#fields),* }),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn rejects_option_nested_in_collection_or_option() {
        let inputs: Vec<DeriveInput> = vec![
            parse_quote! { struct S { field: Option<Option<i32>> } },
            parse_quote! { struct S { field: Vec<Option<i32>> } },
            parse_quote! { struct S { field: HashMap<String, Option<i32>> } },
            parse_quote! { struct S { field: Option<Vec<Option<i32>>> } },
        ];

        for input in inputs {
            let error = field_error(input);
            assert!(error.contains("Option"), "{}", error);
            assert!(error.contains("`field`"), "{}", error);
        }
    }

    #[test]
    fn rejects_non_path_types() {
        let inputs: Vec<DeriveInput> = vec![
            parse_quote! { struct S { field: &'static str } },
            parse_quote! { struct S { field: (i32, String) } },
            parse_quote! { struct S { field: [u8; 4] } },
            parse_quote! { struct S { field: Vec<&'static str> } },
            parse_quote! { struct S { field: Option<(i32, i32)> } },
        ];

        for input in inputs {
            let error = field_error(input);
            assert!(error.contains("Unsupported type"), "{}", error);
            assert!(error.contains("`field`"), "{}", error);
        }
    }
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
