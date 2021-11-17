#[macro_export]

/// Transforms arguments to values consumed by queries.
macro_rules! query_values {
    ($($value:expr),*) => {
        {
            use cdrs_tokio::types::value::Value;
            use cdrs_tokio::query::QueryValues;
            let mut values: Vec<Value> = Vec::new();
            $(
                values.push($value.into());
            )*
            QueryValues::SimpleValues(values)
        }
    };
    ($($name:expr => $value:expr),*) => {
        {
            use cdrs_tokio::types::value::Value;
            use cdrs_tokio::query::QueryValues;
            use std::collections::HashMap;
            let mut values: HashMap<String, Value> = HashMap::new();
            $(
                values.insert($name.to_string(), $value.into());
            )*
            QueryValues::NamedValues(values)
        }
    };
}
