# Query `Value`

Query `Value`-s can be used along with query string templates. Query string templates is a special sort of query string that contains `?` sign. `?` will be substituted by CDRS driver with query `Values`.

For instance:

```rust
const INSERT_NUMBERS_QUERY: &'static str = "INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)";
let values = query_values!(1 as i32, 1 as i64);

session.query_with_values(INSERT_NUMBERS_QUERY, values).await.unwrap();
```

`INSERT_NUMBERS_QUERY` is a typical query template. `session::query_with_values` method provides an API for using such query strings along with query values.

There is full list of `Session` methods that allow using values and query templates:

- `exec_with_values` - executes previously prepared query with provided values (see [example](../examples/prepare_batch_execute.rs) and/or [Preparing and Executing](./preparing-and-executing-queries.md) section);

- `query_with_params_tw` - immediately executes a query using provided values (see [example](../examples/crud_operations.rs))

## Simple `Value` and `Value` with names

There are two type of query values supported by CDRS:

- simple `Value`-s may be imagined as a tuple of actual values. This values will be inserted instead of a `?` that has the same index number as a `Value` within a tuple. To easily create `Value`-s CDRS provides `query_values!` macro:

```rust
let values = query_values!(1 as i32, 1 as i64);
```

- `Value`-s with names may be imagined as a `Map` that links a table column name with a value that should be inserted in a column. It means that `Value`-s with maps should not necessarily have the same order as a corresponded `?` in a query template:

```rust
const INSERT_NUMBERS_QUERY: &'static str = "INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)";
let values = query_values!(my_bigint => 1 as i64, my_int => 1 as i64);

session.query_with_values(INSERT_NUMBERS_QUERY, values).await.unwrap();
```

What kind of values can be used as `query_values!` arguments? All types that have implementations of `Into<Bytes>`.

For Rust structs represented by [Cassandra User Defined types](http://cassandra.apache.org/doc/4.0/cql/types.html#grammar-token-user_defined_type) `#[derive(IntoCdrsValue)]` can be used for recursive implementation. See [CRUD example](../examples/crud_operations.rs).

### Reference

1. Cassandra official docs - User Defined Types http://cassandra.apache.org/doc/4.0/cql/types.html#grammar-token-user_defined_type.

2. Datastax - User Defined Types https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateUDT.html.

3. ScyllaDB - User Defined Types https://docs.scylladb.com/getting-started/types/

4. [CDRS CRUD Example](../examples/crud_operations.rs)
