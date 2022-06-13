### Batch queries

CDRS `Session` supports batching few queries in a single request to Apache Cassandra:

```rust
// batch two queries
use cdrs_tokio::query::{BatchQueryBuilder, QueryBatch};

let mut queries = BatchQueryBuilder::new();
queries = queries.add_query_prepared(&prepared_query);
queries = queries.add_query("INSERT INTO my.store (my_int) VALUES (?)", query_values!(1 as i32));
session.batch_with_params(queries.finalyze()).await;
```
