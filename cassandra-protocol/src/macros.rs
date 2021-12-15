#[macro_export]

/// Transforms arguments to values consumed by queries.
macro_rules! query_values {
    ($($value:expr),*) => {
        {
            use cassandra_protocol::types::value::Value;
            use cassandra_protocol::query::QueryValues;
            let mut values: Vec<Value> = Vec::new();
            $(
                values.push($value.into());
            )*
            QueryValues::SimpleValues(values)
        }
    };
    ($($name:expr => $value:expr),*) => {
        {
            use cassandra_protocol::types::value::Value;
            use cassandra_protocol::query::QueryValues;
            use std::collections::HashMap;
            let mut values: HashMap<String, Value> = HashMap::new();
            $(
                values.insert($name.to_string(), $value.into());
            )*
            QueryValues::NamedValues(values)
        }
    };
}

macro_rules! list_as_rust {
    ($($into_type:tt)+) => (
        impl AsRustType<Vec<$($into_type)+>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<$($into_type)+>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, $($into_type)+)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
}

macro_rules! list_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for List {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::error::Error;
                use crate::types::cassandra_type::CassandraType;
                use std::ops::Deref;

                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option))
                    | Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.deref().clone();
                        let wrapper = self.get_wrapper_fn(type_option_ref);
                        let convert = self.map(|bytes| wrapper(bytes));
                        Ok(Some(CassandraType::List(convert)))
                    }
                    _ => Err(Error::General(format!(
                        "Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                        self.metadata.value
                    ))),
                }
            }
        }
    };
}

macro_rules! map_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for Map {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::types::cassandra_type::CassandraType;
                use std::ops::Deref;

                if let Some(ColTypeOptionValue::CMap(
                    ref key_col_type_option,
                    ref value_col_type_option,
                )) = self.metadata.value
                {
                    let key_col_type_option = key_col_type_option.deref().clone();
                    let value_col_type_option = value_col_type_option.deref().clone();

                    let key_wrapper = self.get_wrapper_fn(key_col_type_option);
                    let value_wrapper = self.get_wrapper_fn(value_col_type_option);

                    let map = self
                        .data
                        .iter()
                        .map(|(key, value)| (key_wrapper(key), value_wrapper(value)))
                        .collect::<Vec<(CassandraType, CassandraType)>>();

                    return Ok(Some(CassandraType::Map(map)));
                } else {
                    panic!("not  amap")
                }
            }
        }
    };
}

macro_rules! map_as_rust {
    ({ $($key_type:tt)+ }, { $($val_type:tt)+ }) => (
        impl AsRustType<HashMap<$($key_type)+, $($val_type)+>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, $($val_type)+>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, $($val_type)+)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
}

macro_rules! into_rust_by_name {
    (Row, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
    (Udt, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
}

macro_rules! into_rust_by_index {
    (Tuple, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Row, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
}

macro_rules! as_res_opt {
    ($data_value:ident, $deserialize:expr) => {
        match $data_value.as_slice() {
            Some(ref bytes) => ($deserialize)(bytes).map(Some).map_err(Into::into),
            None => Ok(None),
        }
    };
}

/// Decodes any Cassandra data type into the corresponding Rust type,
/// given the column type as `ColTypeOption` and the value as `CBytes`
/// plus the matching Rust type.
macro_rules! as_rust_type {
    ($data_type_option:ident, $data_value:ident, Blob) => {

        match $data_type_option.id {
            ColType::Blob => as_res_opt!($data_value, decode_blob),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(crate::frame::frame_result::ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.BytesType" {
                            return as_res_opt!($data_value, decode_blob);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into Vec<u8> (valid types: org.apache.cassandra.db.marshal.BytesType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Vec<u8> (valid types: Blob).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, String) => {
        match $data_type_option.id {
            ColType::Custom => as_res_opt!($data_value, decode_custom),
            ColType::Ascii => as_res_opt!($data_value, decode_ascii),
            ColType::Varchar => as_res_opt!($data_value, decode_varchar),
            // TODO: clarify when to use decode_text.
            // it's not mentioned in
            // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // ColType::XXX => decode_text($data_value)?
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into String (valid types: Custom, Ascii, Varchar).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, bool) => {
        match $data_type_option.id {
            ColType::Boolean => as_res_opt!($data_value, decode_boolean),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.BooleanType" {
                            return as_res_opt!($data_value, decode_boolean);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into bool (valid types: org.apache.cassandra.db.marshal.BooleanType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into bool (valid types: Boolean).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i64) => {
        match $data_type_option.id {
            ColType::Bigint => as_res_opt!($data_value, decode_bigint),
            ColType::Timestamp => as_res_opt!($data_value, decode_timestamp),
            ColType::Time => as_res_opt!($data_value, decode_time),
            ColType::Counter => as_res_opt!($data_value, decode_bigint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.LongType" | "org.apache.cassandra.db.marshal.CounterColumnType" => return as_res_opt!($data_value, decode_bigint),
                            "org.apache.cassandra.db.marshal.TimestampType" => return as_res_opt!($data_value, decode_timestamp),
                            "org.apache.cassandra.db.marshal.TimeType" => return as_res_opt!($data_value, decode_time),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i64 (valid types: org.apache.cassandra.db.marshal.{{LongType|IntegerType|CounterColumnType|TimestampType|TimeType}}).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant,\
                 Counter).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i32) => {
        match $data_type_option.id {
            ColType::Int => as_res_opt!($data_value, decode_int),
            ColType::Date => as_res_opt!($data_value, decode_date),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(crate::frame::frame_result::ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.Int32Type" => return as_res_opt!($data_value, decode_int),
                            "org.apache.cassandra.db.marshal.SimpleDateType" => return as_res_opt!($data_value, decode_date),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i32 (valid types: org.apache.cassandra.db.marshal.Int32Type).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i32 (valid types: Int, Date).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i16) => {
        match $data_type_option.id {
            ColType::Smallint => as_res_opt!($data_value, decode_smallint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ShortType" {
                            return as_res_opt!($data_value, decode_smallint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i16 (valid types: org.apache.cassandra.db.marshal.ShortType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i16 (valid types: Smallint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i8) => {
        match $data_type_option.id {
            ColType::Tinyint => as_res_opt!($data_value, decode_tinyint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ByteType" {
                            return as_res_opt!($data_value, decode_tinyint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i8 (valid types: org.apache.cassandra.db.marshal.ByteType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i8 (valid types: Tinyint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI64) => {
        match $data_type_option.id {
            ColType::Bigint => {
                as_res_opt!($data_value, decode_bigint).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Timestamp => as_res_opt!($data_value, decode_timestamp)
                .map(|value| value.and_then(NonZeroI64::new)),
            ColType::Time => {
                as_res_opt!($data_value, decode_time).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Counter => {
                as_res_opt!($data_value, decode_bigint).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.LongType" | "org.apache.cassandra.db.marshal.CounterColumnType" => return as_res_opt!($data_value, decode_bigint),
                            "org.apache.cassandra.db.marshal.TimestampType" => return as_res_opt!($data_value, decode_timestamp),
                            "org.apache.cassandra.db.marshal.TimeType" => return as_res_opt!($data_value, decode_time),
                            _ => {}
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i64 (valid types: org.apache.cassandra.db.marshal.{{LongType|IntegerType|CounterColumnType|TimestampType|TimeType}}).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI64::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant,\
                 Counter).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI32) => {
        match $data_type_option.id {
            ColType::Int => {
                as_res_opt!($data_value, decode_int).map(|value| value.and_then(NonZeroI32::new))
            }
            ColType::Date => {
                as_res_opt!($data_value, decode_date).map(|value| value.and_then(NonZeroI32::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.Int32Type" => return as_res_opt!($data_value, decode_int),
                            "org.apache.cassandra.db.marshal.SimpleDateType" => return as_res_opt!($data_value, decode_date),
                            _ => {}
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i32 (valid types: org.apache.cassandra.db.marshal.Int32Type).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI32::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i32 (valid types: Int, Date).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI16) => {
        match $data_type_option.id {
            ColType::Smallint => as_res_opt!($data_value, decode_smallint)
                .map(|value| value.and_then(NonZeroI16::new)),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ShortType" {
                            return as_res_opt!($data_value, decode_smallint);
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i16 (valid types: org.apache.cassandra.db.marshal.ShortType).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI16::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i16 (valid types: Smallint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI8) => {
        match $data_type_option.id {
            ColType::Tinyint => {
                as_res_opt!($data_value, decode_tinyint).map(|value| value.and_then(NonZeroI8::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ByteType" {
                            return as_res_opt!($data_value, decode_tinyint);
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i8 (valid types: org.apache.cassandra.db.marshal.ByteType).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI8::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i8 (valid types: Tinyint).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f64) => {
        match $data_type_option.id {
            ColType::Double => as_res_opt!($data_value, decode_double),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.DoubleType" {
                            return as_res_opt!($data_value, decode_double);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into f64 (valid types: org.apache.cassandra.db.marshal.DoubleType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f64 (valid types: Double).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f32) => {
        match $data_type_option.id {
            ColType::Float => as_res_opt!($data_value, decode_float),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.FloatType" {
                            return as_res_opt!($data_value, decode_float);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into f32 (valid types: org.apache.cassandra.db.marshal.FloatType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f32 (valid types: Float).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, IpAddr) => {
        match $data_type_option.id {
            ColType::Inet => as_res_opt!($data_value, decode_inet),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.InetAddressType" {
                            return as_res_opt!($data_value, decode_inet);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into IpAddr (valid types: org.apache.cassandra.db.marshal.InetAddressType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into IpAddr (valid types: Inet).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Uuid) => {
        match $data_type_option.id {
            ColType::Uuid | ColType::Timeuuid => as_res_opt!($data_value, decode_timeuuid),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.UUIDType" | "org.apache.cassandra.db.marshal.TimeUUIDType" => return as_res_opt!($data_value, decode_timeuuid),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into Uuid (valid types: org.apache.cassandra.db.marshal.{{UUIDType|TimeUUIDType}}).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Uuid (valid types: Uuid, Timeuuid).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, List) => {
        match $data_type_option.id {
            ColType::List | ColType::Set => match $data_value.as_slice() {
                Some(ref bytes) => decode_list(bytes)
                    .map(|data| Some(List::new($data_type_option.clone(), data)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into List (valid types: List, Set).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Map) => {
        match $data_type_option.id {
            ColType::Map => match $data_value.as_slice() {
                Some(ref bytes) => decode_map(bytes)
                    .map(|data| Some(Map::new(data, $data_type_option.clone())))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Map (valid types: Map).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Udt) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Udt,
                value: Some(ColTypeOptionValue::UdtType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_udt(bytes, list_type_option.descriptions.len())
                    .map(|data| Some(Udt::new(data, list_type_option)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Udt (valid types: Udt).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Tuple) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Tuple,
                value: Some(ColTypeOptionValue::TupleType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_tuple(bytes, list_type_option.types.len())
                    .map(|data| Some(Tuple::new(data, list_type_option)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Tuple (valid types: tuple).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, PrimitiveDateTime) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        let unix_epoch = time::macros::date!(1970 - 01 - 01).midnight();
                        let tm = unix_epoch
                            + time::Duration::new(ts / 1_000, (ts % 1_000 * 1_000_000) as i32);
                        Some(tm)
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into PrimitiveDateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Decimal) => {
        match $data_type_option.id {
            ColType::Decimal => match $data_value.as_slice() {
                Some(ref bytes) => decode_decimal(bytes).map(Some).map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Decimal (valid types: Decimal).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NaiveDateTime) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        NaiveDateTime::from_timestamp_opt(ts / 1000, (ts % 1000 * 1_000_000) as u32)
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NaiveDateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, DateTime<Utc>) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        Some(DateTime::from_utc(
                            NaiveDateTime::from_timestamp_opt(
                                ts / 1000,
                                (ts % 1000 * 1_000_000) as u32,
                            )?,
                            Utc,
                        ))
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into DateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, BigInt) => {
        match $data_type_option.id {
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.IntegerType" {
                            return as_res_opt!($data_value, decode_varint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into BigInt (valid types: org.apache.cassandra.db.marshal.IntegerType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant,\
                 Counter).",
                $data_type_option.id
            ))),
        }
    };
}
