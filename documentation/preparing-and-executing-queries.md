### Preparing queries

During preparing a query a server parses the query, saves parsing result into cache and returns to a client an ID that could be further used for executing prepared statement with different parameters (such as values, consistency etc.). When a server executes prepared query it doesn't need to parse it so parsing step will be skipped.

```rust
let prepared_query = session.prepare("INSERT INTO my.store (my_int, my_bigint) VALUES (?, ?)").await.unwrap();
```

### Executing prepared queries

When query is prepared on the server client gets prepared query id of type `cdrs_tokio::query::PreparedQuery`. Having such id it's possible to execute prepared query using session methods:

```rust
// execute prepared query without specifying any extra parameters or values
session.exec(&preparedQuery).await.unwrap();

// to execute prepared query with bound values, use exec_with_values()
// to execute prepared query with advanced parameters, use exec_with_params()
