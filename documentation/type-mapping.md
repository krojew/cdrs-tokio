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

Binding a `Vec<T>` always produces a CQL collection: a 4-byte element count followed by
length-prefixed elements. A `Vec<u8>` is therefore encoded as a `list<tinyint>`, not as a blob.
Since a blob column accepts arbitrary bytes, binding a `Vec<u8>` to it does not fail, but stores
the collection framing as part of the data. Wrap raw bytes in `Blob` to bind them to a blob
column, e.g. `Blob::new(bytes)`. For the same reason, bind `vector<float, n>` values with
`FloatVector` rather than `Vec<f32>`.
