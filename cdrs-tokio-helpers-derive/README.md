# cdrs-tokio-helpers-derive

Procedural macros that derive helper traits for CDRS Cassandra to Rust types conversion back and forth

Features:

* convert Cassandra primitive types (not lists, sets, maps, UDTs) into Rust
* recursively convert Cassandra "collection" types (lists, sets, maps) into Rust
* recursively convert Cassandra UDTs into Rust
* recursively convert optional fields into Rust
* convert Rust primitive types into Cassandra query values
* convert Rust "collection" types into Cassandra query values
* convert Rust structures into Cassandra query values
* convert `Option<T>` into Cassandra query value
* generates an insert method for a Rust struct type
