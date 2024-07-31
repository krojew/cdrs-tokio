use crate::error::{Error, Result};
use crate::frame::message_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::frame::Version;
use crate::types::data_serialization_types::*;
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

pub fn get_vector_type_info(option_value: &ColTypeOptionValue) -> Result<VectorInfo> {
    let input = match option_value {
        ColTypeOptionValue::CString(ref s) => s,
        _ => return Err(Error::General("Option value must be a string!".into())),
    };

    let _custom_type = input.split('(').next().unwrap().rsplit('.').next().unwrap();

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
