use crate::error::{Error, Result};
use crate::frame::message_result::{ColType, ColTypeOption, ColTypeOptionValue};
use crate::frame::Version;
use crate::types::data_serialization_types::*;
use crate::types::{AsRust, AsRustType, CBytes};
use derive_more::Constructor;

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
    fn map<T, F>(&self, f: F) -> Vec<T>
    where
        F: FnMut(&CBytes) -> T,
    {
        self.data.iter().map(f).collect()
    }
}

pub struct VectorInfo {
    pub internal_type: String,
    pub count: usize,
}

pub fn get_vector_type_info(option_value: &ColTypeOptionValue) -> Result<VectorInfo> {
    let input = match option_value {
        ColTypeOptionValue::CString(ref s) => s,
        _ => return Err(Error::General("option value must be a string".into())),
    };

    let _custom_type = input.split('(').next().unwrap().rsplit('.').next().unwrap();

    let vector_type = input
        .split('(')
        .nth(1)
        .unwrap()
        .split(',')
        .next()
        .unwrap()
        .rsplit('.')
        .next()
        .unwrap()
        .trim();

    let count: usize = input
        .split('(')
        .nth(1)
        .unwrap()
        .rsplit(',')
        .next()
        .unwrap()
        .split(')')
        .next()
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    Ok(VectorInfo {
        internal_type: vector_type.to_string(),
        count,
    })
}

impl AsRust for Vector {}

vector_as_rust!(f32);

vector_as_cassandra_type!();
