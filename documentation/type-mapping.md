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
| blob | `Blob -> Vec<u8>` |
| list | `List -> Vec<T>` |
| set | `List -> Vec<T>` |
| map | `Map -> HashMap<String, T>` |
| udt | Rust struct |
| vector&lt;float, n&gt; | `FloatVector` (binding), `Vector -> Vec<f32>` (reading) |

Binding a `Vec<T>` produces a CQL collection: a 4-byte element count followed by
length-prefixed elements. The exception is `Vec<u8>`, which binds as a blob: its bytes are sent as
they are, the same as for `&[u8]` and `Blob`. A `list<tinyint>` binds from a `Vec<i8>`, and a
`Vec<Vec<u8>>` binds as a `list<blob>`. Since a `Vec<f32>` binds as a collection, bind
`vector<float, n>` values with `FloatVector`.
