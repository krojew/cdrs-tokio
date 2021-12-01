use itertools::Itertools;
use std::collections::HashMap;
use std::io::Cursor;

use crate::frame::Serialize;
use crate::types::serialize_str;
use crate::types::value::Value;

/// Enum that represents two types of query values:
/// * values without name
/// * values with names
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValues {
    SimpleValues(Vec<Value>),
    NamedValues(HashMap<String, Value>),
}

impl QueryValues {
    /// Returns `true` if query values is with names and `false` otherwise.
    #[inline]
    pub fn has_names(&self) -> bool {
        !matches!(*self, QueryValues::SimpleValues(_))
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        match *self {
            QueryValues::SimpleValues(ref v) => v.len(),
            QueryValues::NamedValues(ref m) => m.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Into<Value>> From<Vec<T>> for QueryValues {
    /// Converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
    fn from(values: Vec<T>) -> QueryValues {
        let vals = values.into_iter().map_into();
        QueryValues::SimpleValues(vals.collect())
    }
}

impl<T: Into<Value> + Clone> From<&[T]> for QueryValues {
    /// Converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
    fn from(values: &[T]) -> QueryValues {
        let values = values.iter().map(|v| v.clone().into());
        QueryValues::SimpleValues(values.collect())
    }
}

impl<S: ToString, V: Into<Value>> From<HashMap<S, V>> for QueryValues {
    /// Converts values from `HashMap` to query values with names `QueryValues::NamedValues`.
    fn from(values: HashMap<S, V>) -> QueryValues {
        let mut map = HashMap::with_capacity(values.len());
        for (name, val) in values {
            map.insert(name.to_string(), val.into());
        }

        QueryValues::NamedValues(map)
    }
}

impl Serialize for QueryValues {
    fn serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            QueryValues::SimpleValues(v) => {
                for value in v {
                    value.serialize(cursor);
                }
            }
            QueryValues::NamedValues(v) => {
                for (key, value) in v {
                    serialize_str(cursor, key);
                    value.serialize(cursor);
                }
            }
        }
    }
}
