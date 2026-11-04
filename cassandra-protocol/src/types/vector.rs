use crate::error::{Error, Result};
use crate::frame::message_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::frame::Version;
use crate::types::data_serialization_types::*;
use crate::types::value::Bytes;
use crate::types::{AsRust, AsRustType, CBytes};
use derive_more::Constructor;
use itertools::Itertools;

// TODO: consider using pointers to ColTypeOption and Vec<CBytes> instead of owning them.
#[derive(Debug, Constructor)]
pub struct Vector {
    /// column spec of the list, i.e. id should be List as it's a list and value should contain
    /// a type of list items.
    metadata: ColTypeOption,
    data: Vec<CBytes>,
    protocol_version: Version,
}

impl Vector {
    fn try_map<T, F>(&self, f: F) -> Result<Vec<T>>
    where
        F: FnMut(&CBytes) -> Result<T>,
    {
        self.data.iter().map(f).try_collect()
    }
}

pub struct VectorInfo {
    pub internal_type: String,
    pub count: usize,
}

/// A `vector<float, n>` value for binding to a query. Unlike a `Vec<f32>`, which binds as a
/// `list<float>`, the elements are encoded back to back without a count or element lengths.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FloatVector(pub Vec<f32>);

impl From<Vec<f32>> for FloatVector {
    #[inline]
    fn from(value: Vec<f32>) -> Self {
        FloatVector(value)
    }
}

impl From<FloatVector> for Bytes {
    fn from(value: FloatVector) -> Self {
        Bytes::new(
            value
                .0
                .into_iter()
                .flat_map(|element| element.to_be_bytes())
                .collect(),
        )
    }
}

const VECTOR_TYPE_CLASS: &str = "org.apache.cassandra.db.marshal.VectorType";

/// Checks if a custom type option names the vector marshal type.
pub(crate) fn is_vector_type(option_value: &ColTypeOptionValue) -> bool {
    matches!(
        option_value,
        ColTypeOptionValue::CString(class_name)
            if class_name.split('(').next().map(str::trim) == Some(VECTOR_TYPE_CLASS)
    )
}

pub fn get_vector_type_info(option_value: &ColTypeOptionValue) -> Result<VectorInfo> {
    let input = match option_value {
        ColTypeOptionValue::CString(ref s) => s,
        _ => return Err(Error::General("Option value must be a string!".into())),
    };

    if !is_vector_type(option_value) {
        return Err(Error::General(format!("{input} is not a vector type!")));
    }

    let vector_type = input
        .split('(')
        .nth(1)
        .and_then(|s| s.split(',').next())
        .and_then(|s| s.rsplit('.').next())
        .map(|s| s.trim())
        .ok_or_else(|| Error::General("Cannot parse vector type!".into()))?;

    let count: usize = input
        .split('(')
        .nth(1)
        .and_then(|s| s.rsplit(',').next())
        .and_then(|s| s.split(')').next())
        .map(|s| s.trim().parse())
        .transpose()
        .map_err(|_| Error::General("Cannot parse vector count!".to_string()))?
        .ok_or_else(|| Error::General("Cannot parse vector count!".into()))?;

    Ok(VectorInfo {
        internal_type: vector_type.to_string(),
        count,
    })
}

impl AsRust for Vector {}

vector_as_rust!(f32);

vector_as_cassandra_type!();

#[cfg(test)]
mod tests {
    use super::*;

    fn float_vector_type(count: usize) -> ColTypeOption {
        ColTypeOption {
            id: ColType::Custom,
            value: Some(ColTypeOptionValue::CString(format!(
                "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, {count})"
            ))),
        }
    }

    #[test]
    fn converts_float_vector() {
        // IEEE 754 binary32 big-endian: 1.0, 2.5, -2.0
        let vector = Vector::new(
            float_vector_type(3),
            vec![
                CBytes::new(vec![0x3F, 0x80, 0x00, 0x00]),
                CBytes::new(vec![0x40, 0x20, 0x00, 0x00]),
                CBytes::new(vec![0xC0, 0x00, 0x00, 0x00]),
            ],
            Version::V5,
        );

        let converted: Option<Vec<f32>> = vector.as_rust_type().unwrap();
        assert_eq!(converted, Some(vec![1.0, 2.5, -2.0]));
    }

    #[test]
    fn null_float_vector_element_is_error() {
        let vector = Vector::new(
            float_vector_type(2),
            vec![
                CBytes::new(vec![0x3F, 0x80, 0x00, 0x00]),
                CBytes::new_null(),
            ],
            Version::V5,
        );

        let converted: Result<Option<Vec<f32>>> = vector.as_rust_type();
        assert!(converted.is_err());
    }

    #[test]
    fn float_vector_binds_as_concatenated_elements() {
        use crate::frame::Serialize;
        use crate::types::value::Value;

        // vector<float, 3> [1.0, 2.5, -2.0]: IEEE 754 binary32 big-endian elements concatenated
        // without a count or per-element lengths
        let expected = vec![
            0x3F, 0x80, 0x00, 0x00, 0x40, 0x20, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00,
        ];

        let bytes: Bytes = FloatVector::from(vec![1.0, 2.5, -2.0]).into();
        assert_eq!(bytes.into_inner(), expected);

        let value = Value::from(FloatVector(vec![1.0, 2.5, -2.0]));
        assert_eq!(
            value.serialize_to_vec(Version::V5),
            [vec![0x00, 0x00, 0x00, 0x0C], expected].concat()
        );
    }

    #[test]
    fn parses_vector_type_info() {
        let info = get_vector_type_info(&ColTypeOptionValue::CString(
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)"
                .into(),
        ))
        .unwrap();

        assert_eq!(info.internal_type, "FloatType");
        assert_eq!(info.count, 3);
    }

    #[test]
    fn rejects_non_vector_custom_type_info() {
        assert!(get_vector_type_info(&ColTypeOptionValue::CString(
            "com.example.Pair(com.example.FloatType, 3)".into(),
        ))
        .is_err());
    }
}
