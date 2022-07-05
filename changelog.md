## 7.0.0

### Fixed

* `ExponentialReconnectionSchedule` duration overflow.

### New

* Protocol V5 support. Please look at official changelog for more information: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L1419.
* Support for beta protocols - possibility to connect to beta clusters.
* `From<Decimal>` for `BigInt`.
* `check_envelope_size` for `Evelope`.
* `Error` is now `Clone`.
* `FrameEncoder`, `FrameDecoder` and `FrameEncodingFactory` responsible for encoding/decoding frames on the wire.
* `with_frame_encoder_factory` Session build option. 
* `Error` impl for `CheckEnvelopeSizeError` and `ParseEnvelopeError`.
* New `Error` variants for more granular error handling.

### Changed

* Due to naming changes in V5, frame have been renamed to message, `Frame` to `Envelope` and a frame now
  corresponds to wrapped envelopes, as defined by the protocol.
* `Serialize` and `FromCursor` traits now pass protocol version to implementations.
* `Row::from_frame_body` renamed to `from_body`.
* `QueryFlags` got extended for V5 and now supports `Serialize` and `FromCursor`.
* Session builders now validate given configuration and return a `Result`.
* Transport startup now fails gracefully on unexpected server response.
* `CdrsTransport` now requires explicit information if messages are a part of initial handshake.
* `ResResultBody::as_rows_metadata` and `ResponseBody::as_rows_metadata` now return a reference to the data.
* `Hash`, `PartialEq` and `PartialOrd` for `PreparedQuery` only take `id` and `result_metadata_id` into account,
  since those define equivalence.

## 6.2.0

### New

* `derive` feature built into the main crate - no need to explicitly `use cdrs_tokio_helpers_derive::*` anymore.

## 6.1.0

### New

* `#[must_use]` on some functions.

### Fixed

* Fixed parsing `NetworkTopologyStrategy`.

## 6.0.0

This version is a departure from legacy API design, stemming from the sync version migration. Due to large
performance issues and lack of dynamic topology handling in earlier versions, a decision has been made to cut
the ties and focus on delivering the best functionality without legacy burden. The API surface changes are
quite large, but everyone is encouraged to update - the performance improvements and new features cannot be
understated.

### New

* Topology-aware load balancing: `TopologyAwareNodeDistanceEvaluator` and `TopologyAwareLoadBalancingStrategy`.
* New `ReconnectionPolicy` used when trying to re-establish connections to downed nodes.
* `Error` now implements standard `Error`.
* `SessionBuilder` introduced as the preferred way to create a session.
* Added missing traits for `BatchType` and `QueryFlags`.
* `ToString` implementation for `SimpleServerEvent`.
* Standard trait implementations for event frames.
* `contains_column`, `is_empty_by_name` and `is_empty` functions for `Row`.
* `Display` implementation for public enums.
* Missing traits for `PreparedMetadata`, `Value`, `Consistency` and `ColType`.
* New `PreparedMetadataFlags`.
* New `ClusterMetadata` representing information about a cluster.
* Extracted protocol functionality to separate `cassandra-protocol` crate.
* Passing final auth data from the server to `SaslAuthenticator`.
* `SpeculativeExecutionPolicy` for speculative execution control.

### Changed

* All `with_name` fields or args in the query API are now `bool` instead of `Option<bool>`
* `flags` field removed from `QueryParams` (flags are now derived from the other fields at serialization time)
* Rewritten transport layer for massive performance improvements (including removing `bb8`). This
  involves changing a large portion of public API related to transport and server events.
* Rewritten event mechanism - now you can subscribe to server events via `create_event_receiver()` in `Session`.
* Replaced `RowsMetadataFlag`, `QueryFlags` and `frame::Flags` vectors with bitflags.
* Changed `Target` and `ChangeType` enums to `SchemaChangeTarget` and `SchemaChangeType`.
* The `varint` type now uses `num::BigInt` representation (this implies `Decimal` also uses "big" types).
* Removed `unstable-dynamic-cluster` feature, since it wasn't working as expected and introduced performance  
  penalty. Dynamic topology handling is now built-in. 
* Removed `AsBytes` in favor of new `Serialize` trait due to performance penalty.
* Removed `FromSingleByte` and `AsByte` in favor of `From`/`TryFrom`.
* Removed traits along with `async-trait` dependency: `BatchExecutor`, `ExecExecutor`, `PrepareExecutor`, 
  `QueryExecutor`, `GetConnection` and `CdrsSession`. Everything is now embedded directly in `Session`.
* Load balancing strategy now returns query plans, rather than individual nodes, and operates on cluster metadata.
* Removed `SingleNode` load balancing strategy.
* Removed empty `SimpleError`.
* Renamed `connect_generic_static` to `connect_generic`.
* Removed `GetRetryPolicy`.
* Renamed `ChangeSchemeOptions` to `SchemaChangeOptions`.
* Protocol version can now be selected at run time.
* `Value` now directly contains the value in the `Some` variant instead of a separate body field.
* Consistent naming convention in all builders.
* Split protocol-level parameters from high-level statement parameters (`QueryParams` vs `StatementParams`) and
  simplified API.
* `add_query_prepared` for batch queries now takes `PreparedQuery` by reference.

## 5.0.0

### New

* Support for stateful SASL authenticators.

### Changed

* Using up-to-date lz4 crate (no more unmaintained dependency alerts).

## 4.0.0

### Fixed

* Build problems with Rustls.
* TLS connections sometimes not flushing all data.
* Not setting current namespace when not using an authenticator.

### New

* New `connect_generic_*` functions allowing custom connection configurations (see `generic_connection.rs`
  for example usage).
* Possibility to use custom error types which implement `FromCdrsError` throughout the crate.
* `Consistency` now implements `FromStr`.
* Pagers can be converted into `PagerState`.
* Support for v4 marshaled types.
* `Copy`, `Clone`, `Ord`, `PartialOrd`, `Eq`, `Hash` for `Opcode`.
* Customizable query retry policies with built-in `FallthroughRetrySession` and `DefaultRetryPolicy`.

### Changed

* TCP configuration now owns contained data - no need to keep it alive while the config is alive.
* `ExecPager` is now public.
* `Bytes` now implements `From` for supported types, instead of `Into`.
* Moved some generic types to associated types, thus removing a lot of type passing.
* `SessionPager` no longer needs mutable session.
* A lot of names have been migrated to idiomatic Rust (mainly upper camel case abbreviations).

## 3.0.0

### Fixed

* Remembering `USE`d keyspaces across connections.
* Race condition on query id overflow.

### Changed

* Removed deprecated `PasswordAuthenticator`.
* Removed unused `Compressor` trait.
* Large API cleanup.
* Renamed `IntoBytes` to `AsBytes`.
* `Authenticator` can now be created at runtime - removed static type parameter.
* Removed unneeded memory allocations when parsing data.

## 2.1.0

### Fixed

* Recreation of forgotten prepared statements. 

### New

* `rustls` sessions constructors.

### Changed

* Updated `tokio` to 1.1.

## 2.0.0

### New

* Support for `NonZero*` types.
* Support for `chrono` `NaiveDateTime` and `DateTime<Utc>`.
* Update `tokio` to 1.0.
* `Pager` supporting `QueryValues` and consistency.

## 1.0.0

* Initial release.
