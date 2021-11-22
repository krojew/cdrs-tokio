use std::collections::HashMap;
use std::hash::Hash;
use std::io::Cursor;

use crate::frame::Serialize;
use crate::types::value::Value;
use crate::types::CIntShort;

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

impl<T: Into<Value> + Clone> From<Vec<T>> for QueryValues {
    /// It converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
    fn from(values: Vec<T>) -> QueryValues {
        let vals = values.iter().map(|v| v.clone().into());
        QueryValues::SimpleValues(vals.collect())
    }
}

impl<'a, T: Into<Value> + Clone> From<&'a [T]> for QueryValues {
    /// It converts values from `Vec` to query values without names `QueryValues::SimpleValues`.
    fn from(values: &'a [T]) -> QueryValues {
        let values = values.iter().map(|v| v.clone().into());
        QueryValues::SimpleValues(values.collect())
    }
}

impl<S: ToString + Hash + Eq, V: Into<Value> + Clone> From<HashMap<S, V>> for QueryValues {
    /// It converts values from `HashMap` to query values with names `QueryValues::NamedValues`.
    fn from(values: HashMap<S, V>) -> QueryValues {
        let map: HashMap<String, Value> = HashMap::with_capacity(values.len());
        let _values = values.iter().fold(map, |mut acc, v| {
            let name = v.0;
            let val = v.1;
            acc.insert(name.to_string(), val.clone().into());
            acc
        });
        QueryValues::NamedValues(_values)
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
                    let len = key.len() as CIntShort;
                    len.serialize(cursor);
                    key.serialize(cursor);
                    value.serialize(cursor);
                }
            }
        }
    }
}
