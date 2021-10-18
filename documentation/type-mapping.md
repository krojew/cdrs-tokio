### Type relations between Rust (in CDRS approach) and Apache Cassandra

#### primitive types (`T`)

| Cassandra | Rust | Feature
|-----------|-------|-------|
| tinyint | i8 | v4, v5 |
| smallint | i16 | v4, v5 |
| int | i32 | all |
| bigint | i64 | all |
| ascii | String | all |
| text | String | all |
| varchar | String | all |
| boolean | bool | all |
| time | i64 | all |
| timestamp | i64 | all |
| float | f32 | all |
| double | f64 | all |
| uuid | [Uuid](https://doc.rust-lang.org/uuid/uuid/struct.Uuid.html) | all |
| counter | i64 | all |

#### complex types
| Cassandra | Rust + CDRS |
|-----------|-------------|
| blob | `Blob -> Vec<>` |
| list | `List -> Vec<T>` |
| set | `List -> Vec<T>` |
| map | `Map -> HashMap<String, T>` |
| udt | Rust struct |
