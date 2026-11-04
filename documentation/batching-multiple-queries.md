### Batch queries

CDRS `Session` supports batching few queries in a single request to Apache Cassandra:

```rust
// batch two queries
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::query_values;

let batch = BatchQueryBuilder::new()
    .add_query_prepared(&prepared_query, query_values!(1 as i32))
    .add_query("INSERT INTO my.store (my_int) VALUES (?)", query_values!(2 as i32))
    .build()
    .expect("batch builder");
session.batch(batch).await.unwrap();
```

`build()` validates the batch and returns a `Result`. To execute a batch with statement parameters, such as tracing or a retry policy, use `session.batch_with_params(batch, &params)` with `StatementParams` created by `cdrs_tokio::statement::StatementParamsBuilder`.
