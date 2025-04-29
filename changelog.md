## 8.1.7

### Fixed

* Not recreating connections on down event if there are still apparently open
  ones.

## 8.1.6

### Fixed

* Refreshing node information can preserve invalid `Down` state (by Denis
  Kosenkov).

## 8.1.5

### Fixed

* Race condition when reconnecting to a cluster with all nodes down (by Denis
  Kosenkov).

## 8.1.4

### Fixed

* CPU spike after some time running.

## 8.1.3

* Dependency updates.

## 8.1.2

### Changed

* Dependency updates.

## 8.1.1

### Fixed

* Non-fatal errors closing connections.

## 8.1.0

### Fixed

* Sending envelopes now properly jumps to next node in query plan, if current
  one is unreachable.

### New

* `InvalidProtocol` special error for a case when a node doesn't accept
  requested protocol during handshake.
* `ConnectionPoolConfigBuilder` for building configuration easily.
* Configurable heartbeat messages to keep connection alive in the pool.

### Changed

* Due to an edge case with reconnecting to a seemingly downed node, internal
  reconnection handling mechanism has been improved.
* Hidden internal structures, which were public but not usable in any way.

## 8.0.0 (unavailable)

### Changed

* Removed `Ord, ParialOrd` from `QueryFlags`.
* Using `rustls` types exported from `tokio-rustls`, rather than depending
  on `rustls` directly.

## 8.0.0-beta.1

### Fixed

* Fixed stack overflow when cannot determine field type during struct
  serialization.
* Properly supporting references during struct serialization.

### New

* Many types are now `Debug`.
* HTTP proxy support via the `http-proxy` feature.

### Changed

* Made protocol enums non-exhaustive for future compatibility.
* Session builders are now async and wait for control connection to be ready
  before returning a session.
* `CBytes::new_empty()` -> `CBytes::new_null()`, `CBytes::is_empty()` ->
  `CBytes::is_null_or_empty()`.

## 7.0.4

### Fixed

* Invalid Murmur3 hash for keys longer than 15 bytes.

## 7.0.3

### Fixed

* Fixed serialization of routing key with known indexes.

### Changed

* Deprecated `query_with_param()` in `Pager`, in favor of `query_with_params()`.

## 7.0.2

### Fixed

* Serializing single PK routing keys by index.
* Encoding envelopes with tracing/warning flags.

## 7.0.1

### Fixed

* Overflow when compressed envelope payload exceeds max payload size.
* Integer overflow when not received at least 8 header bytes.

## 7.0.0

### New

* `Clone` implemented for `BodyResReady` and `BodyReqExecute`.

### Changed

* Control connection errors are now logged as warnings, since they're
  recoverable.
* Exposed fields of `BodyReqAuthResponse` and `BodyReqExecute`.
* Replaced `CInet` type with `SocketAddr`, since it was nothing more than a
  wrapper.

## 7.0.0-beta.2

### Fixed

* Constant control connection re-establishing with legacy clusters.

### New

* `ResponseBody::into_error` function.

## 7.0.0-beta.1

### Fixed

* `ExponentialReconnectionSchedule` duration overflow.
* Forgetting real error type in certain transport error situations.
* Not sending re-preparation statements to correct nodes.
* Infinite set keyspace notification loop.

### New

* Protocol V5 support. Please look at official changelog for more
  information: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L1419.
* Support for beta protocols - possibility to connect to beta clusters.
* `From<Decimal>` for `BigInt`.
* `check_envelope_size` for `Evelope`.
* `Error` is now `Clone`.
* `FrameEncoder`, `FrameDecoder` and `FrameEncodingFactory` responsible for
  encoding/decoding frames on the wire.
* `with_frame_encoder_factory` Session build option.
* `Error` impl for `CheckEnvelopeSizeError` and `ParseEnvelopeError`.
* New `Error` variants for more granular error handling.
* Node address in `Error::Server` variant.

### Changed

* Due to naming changes in V5, frame have been renamed to message, `Frame`
  to `Envelope` and a frame now
  corresponds to wrapped envelopes, as defined by the protocol.
* `Serialize` and `FromCursor` traits now pass protocol version to
  implementations.
* `Row::from_frame_body` renamed to `from_body`.
* `ClusterMetadataManager::find_node` renamed to `find_node_by_rpc_address` for
  consistency.
* `QueryFlags` got extended for V5 and now supports `Serialize`
  and `FromCursor`.
* Session builders now validate given configuration and return a `Result`.
* Transport startup now fails gracefully on unexpected server response.
* `CdrsTransport` now requires explicit information if messages are a part of
  initial handshake.
* `ResResultBody::as_rows_metadata` and `ResponseBody::as_rows_metadata` now
  return a reference to the data.
* `Hash`, `PartialEq` and `PartialOrd` for `PreparedQuery` only take `id`
  and `result_metadata_id` into account,
  since those define equivalence.
* Updated `chrono` dependency to work around found CVE.

## 6.2.0

### New

* `derive` feature built into the main crate - no need to
  explicitly `use cdrs_tokio_helpers_derive::*` anymore.

## 6.1.0

### New

* `#[must_use]` on some functions.

### Fixed

* Fixed parsing `NetworkTopologyStrategy`.

## 6.0.0

This version is a departure from legacy API design, stemming from the sync
version migration. Due to large
performance issues and lack of dynamic topology handling in earlier versions, a
decision has been made to cut
the ties and focus on delivering the best functionality without legacy burden.
The API surface changes are
quite large, but everyone is encouraged to update - the performance improvements
and new features cannot be
understated.

### New

* Topology-aware load balancing: `TopologyAwareNodeDistanceEvaluator`
  and `TopologyAwareLoadBalancingStrategy`.
* New `ReconnectionPolicy` used when trying to re-establish connections to
  downed nodes.
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

* All `with_name` fields or args in the query API are now `bool` instead
  of `Option<bool>`
* `flags` field removed from `QueryParams` (flags are now derived from the other
  fields at serialization time)
* Rewritten transport layer for massive performance improvements (including
  removing `bb8`). This
  involves changing a large portion of public API related to transport and
  server events.
* Rewritten event mechanism - now you can subscribe to server events
  via `create_event_receiver()` in `Session`.
* Replaced `RowsMetadataFlag`, `QueryFlags` and `frame::Flags` vectors with
  bitflags.
* Changed `Target` and `ChangeType` enums to `SchemaChangeTarget`
  and `SchemaChangeType`.
* The `varint` type now uses `num::BigInt` representation (this
  implies `Decimal` also uses "big" types).
* Removed `unstable-dynamic-cluster` feature, since it wasn't working as
  expected and introduced performance  
  penalty. Dynamic topology handling is now built-in.
* Removed `AsBytes` in favor of new `Serialize` trait due to performance
  penalty.
* Removed `FromSingleByte` and `AsByte` in favor of `From`/`TryFrom`.
* Removed traits along with `async-trait`
  dependency: `BatchExecutor`, `ExecExecutor`, `PrepareExecutor`,
  `QueryExecutor`, `GetConnection` and `CdrsSession`. Everything is now embedded
  directly in `Session`.
* Load balancing strategy now returns query plans, rather than individual nodes,
  and operates on cluster metadata.
* Removed `SingleNode` load balancing strategy.
* Removed empty `SimpleError`.
* Renamed `connect_generic_static` to `connect_generic`.
* Removed `GetRetryPolicy`.
* Renamed `ChangeSchemeOptions` to `SchemaChangeOptions`.
* Protocol version can now be selected at run time.
* `Value` now directly contains the value in the `Some` variant instead of a
  separate body field.
* Consistent naming convention in all builders.
* Split protocol-level parameters from high-level statement
  parameters (`QueryParams` vs `StatementParams`) and
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

* New `connect_generic_*` functions allowing custom connection configurations (
  see `generic_connection.rs`
  for example usage).
* Possibility to use custom error types which implement `FromCdrsError`
  throughout the crate.
* `Consistency` now implements `FromStr`.
* Pagers can be converted into `PagerState`.
* Support for v4 marshaled types.
* `Copy`, `Clone`, `Ord`, `PartialOrd`, `Eq`, `Hash` for `Opcode`.
* Customizable query retry policies with built-in `FallthroughRetrySession`
  and `DefaultRetryPolicy`.

### Changed

* TCP configuration now owns contained data - no need to keep it alive while the
  config is alive.
* `ExecPager` is now public.
* `Bytes` now implements `From` for supported types, instead of `Into`.
* Moved some generic types to associated types, thus removing a lot of type
  passing.
* `SessionPager` no longer needs mutable session.
* A lot of names have been migrated to idiomatic Rust (mainly upper camel case
  abbreviations).

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
