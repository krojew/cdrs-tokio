## cdrs-tokio 10.0.0

### Changed

* Uses `cassandra-protocol` 5.0.0 and `cdrs-tokio-helpers-derive` 6.0.0.
  Their breaking changes apply to the re-exported modules as well.
* Node connection pools are kept across cluster metadata refreshes and
  status events, as long as the node's address, host id and distance stay
  the same, instead of being recreated and reconnected on every refresh.
  Node state changes made by a pool are visible on the node in the current
  metadata, a node marked up by an event or a refresh makes its pool
  reconnect right away, and contact points no longer share their pools
  with metadata nodes.
* `load_balancing::Request`, `retry::QueryInfo`,
  `speculative_execution::Context`, `StatementParams`, `KeyspaceMetadata`,
  `DatacenterMetadata` and `NodeInfo` are `#[non_exhaustive]`, so they can
  no longer be built with a struct literal outside the crate. Build
  `Request` from `Request::default()` (`Request::new` is removed) and
  `StatementParams` with `StatementParamsBuilder` or from
  `StatementParams::default()`; the others have `new` constructors, which
  give default values to fields added later.
* `RetryDecision`, `SessionBuildError`, `ReplicationStrategy`,
  `NodeDistance` and `NodeState` are `#[non_exhaustive]`, so matching on
  them outside the crate needs a wildcard arm.

### Fixed

* Background tasks of a connection pool stop as soon as the pool is
  dropped, instead of lingering until the next keyspace change or
  reconnection attempt.

### Removed

* `StatementParamsBuilder::with_flags` and `StatementParamsBuilder::with_names`.

## cassandra-protocol 5.0.0

### Changed

* `FailureInfo::ReasonMap` is keyed by `IpAddr`, since the protocol sends
  no port for the endpoints.
* The body of a decoded envelope holds only the message. A custom payload
  is kept in a private field, so encoding the envelope reproduces it, and
  the custom payload flag is encoded only together with such a payload.
  `Envelope` can no longer be built with a struct literal outside the
  crate; use `Envelope::new`.
* `Vec<u8>` and `&[u8]` bind as blobs, and `u8` no longer converts into
  `Bytes`.
* `Decimal` implements `TryFrom<f32>` and `TryFrom<f64>` instead of
  `From`, returning `NonFiniteFloatError` for NaN and infinities.
* `CompressionError` is `#[non_exhaustive]`, and decompressed size and
  length checks report `DecompressedSizeExceedsLimit` and
  `DecompressedLengthMismatch`.
* `QueryParams`, `PreparedQuery`, `ErrorBody` and the error detail structs
  are `#[non_exhaustive]`. Build `QueryParams` with `QueryParamsBuilder`
  or from its default value, and the others with their `new`
  constructors.
* `BodyReqExecute`, `BodyReqExecuteOwned`, `BodyReqBatch`, `BatchQuery`,
  `BodyReqPrepare`, `BodyReqStartup`, `BodyResResultRows`,
  `BodyResResultPrepared`, `RowsMetadata`, `PreparedMetadata`, `ColSpec`,
  `TableSpec`, `SchemaChange`, `StatusChange` and `TopologyChange` are
  `#[non_exhaustive]`. Build them with their `new` constructors, or from
  their default value where they have one.
* `CompressionError::Snappy` holds the opaque `SnappyError` instead of
  `snap::Error`.
* `PreparedQuery::result_metadata_id` is private. Read and replace it with
  the `result_metadata_id` and `set_result_metadata_id` methods.

### Added

* `PreparedQuery::new`, `ErrorBody::new` and `new` constructors for the
  error detail structs.
* `new` constructors for `BodyResResultRows`, `BodyResResultPrepared`,
  `RowsMetadata`, `ColSpec`, `TableSpec`, `SchemaChange`, `StatusChange`
  and `TopologyChange`, and `Default` for `PreparedMetadata`.
* `compression::SnappyError`.
* `PreparedQuery::result_metadata_id` and
  `PreparedQuery::set_result_metadata_id`.
* `ParseEnvelopeError::InvalidCustomPayload`.
* `types::decimal::NonFiniteFloatError`.
* `Duration` accessors: by name and by index on `Row`, by name on `Udt`,
  by index on `Tuple`, as `List` elements and as `Map` values, plus
  `FromCdrs` and `FromCdrsByName` for `Duration`. They read both the native
  `duration` type and the custom `DurationType` sent on protocol v4.

### Fixed

* The generic `CassandraType` conversion decodes `duration` values on
  protocol v4, where they arrive as the custom `DurationType`, into
  `CassandraType::Duration` instead of a blob.

### Removed

* `Envelope::message_body`.
* `QueryParams::with_names`, `QueryParamsBuilder::with_flags`,
  `QueryParamsBuilder::with_names` and the ignored `with_names` parameter
  of `Envelope::new_req_query`.
* `Error::UnexpectedErrorCode`.
* `From<u8>` for `Bytes`.

## cdrs-tokio-helpers-derive 6.0.0

### Changed

* Generated code refers to `::cdrs_tokio` by an absolute path, so
  `cdrs-tokio` has to be a direct dependency.
* `IntoCdrsValue` moves `Option` field values instead of cloning them, so
  field types no longer need to implement `Clone`, but a struct
  implementing `Drop` can no longer derive it.

## cdrs-tokio 9.1.0

### Added

* `StatementParamsBuilder::with_tracing` and `with_warnings`.
* `SessionPager::exec_with_params` and `exec_with_pager_state_params` for
  paging prepared statements with bound values and statement parameters.
* `ConnectionPoolConfigBuilder::with_heartbeat_timeout` (10 s by default).
* `NodeRustlsConfigBuilder::with_server_name_resolver` and
  `RustlsConnectionManager::with_server_name_resolver` for resolving the
  TLS server name separately for each node.
* `with_beta_protocol` on `TcpConnectionManager`, `RustlsConnectionManager`,
  `TransportTcp` and `TransportRustls`.
* `CdrsTransport::write_internal_envelope` and `CdrsTransport::mark_broken`,
  both with default implementations.

### Changed

* Topology-aware query plans are ordered local replicas, local
  non-replicas, remote replicas, then remote nodes. Dc-local consistency
  levels no longer use remote nodes unless DC failover for local
  consistency is allowed.
* `ClusterMetadata::unignored_remote_nodes_capped` caps nodes per
  datacenter and picks them randomly.
* Building a session returns `SessionBuildError::SessionInitFailed` when
  no contact point can be connected, instead of retrying indefinitely.
* Nodes discovered during a metadata refresh, and nodes whose address
  changes, are considered up.
* Read timeouts are retried when the data was not retrieved, matching the
  Java driver.
* With beta protocol enabled, every frame carries the beta flag, including
  startup, authentication, heartbeats and `USE` statements.
* `ConnectionPoolConfigBuilder` panics on zero pool sizes and zero
  heartbeat interval or timeout, and `ExponentialReconnectionPolicy::new`
  panics when the base delay exceeds the max delay, instead of failing
  later in background tasks.

### Fixed

* Stream ids of requests in flight are never reused, so a response can no
  longer be delivered to the wrong caller. When all ids are in use,
  requests wait for a free one.
* A connection whose processing task panics is marked broken and
  reconnected.
* Pool heartbeats time out without holding the pool lock, and requests in
  flight on an unresponsive connection fail instead of hanging.
* Broken pool connections are reconnected without holding the pool lock,
  and connections breaking during a reconnection trigger another pass.
* Internal `USE` statements no longer override a newer keyspace set by the
  user.
* The control connection sends heartbeats using the pool heartbeat
  settings, ignores errors of connections it replaced, and retries
  initialization when nodes are reachable but the metadata refresh fails.
* Node connections enable TCP keepalive, including connections through an
  HTTP proxy, and `connect_timeout` applies to control connections.
* Reconnection delays are jittered on every attempt.
* IPv6 node addresses are bracketed in HTTP proxy `CONNECT` requests.
* Proxy passwords are redacted from `Debug` output.
* Speculative execution no longer panics when the only running execution
  fails, and no longer gives up while other executions are still running.
* Reprepared statements are resent to the node which reported them
  unprepared.
* The last node error is returned when the query plan is exhausted.
* Explicit routing keys are used for prepared statements regardless of how
  values are bound, and routing hints are used for batches.
* Pagers carry all statement parameters across pages.
* The new result metadata id of a protocol v5 response is read without
  decoding the rows.
* Nodes added by events, contact points and nodes with a changed address
  get a computed distance instead of being ignored.
* A metadata refresh which is not full keeps the keyspaces it fetched.
* Adding a node no longer overwrites concurrent metadata changes, and a
  `NEW_NODE` event no longer demotes a node which is already up.
* `MOVED_NODE` topology changes are handled, and missed events trigger a
  full metadata refresh.
* Replicas are placed on the full token ring before skipping ignored
  nodes, local replicas are shuffled in single-datacenter clusters, and
  replica lookup stops walking the ring once enough replicas are found.
* Documentation examples match the current API, and the multi-threaded
  example awaits its inserts.

### Deprecated

* `StatementParamsBuilder::with_flags`, which has no effect.
* `StatementParamsBuilder::with_names`; the flag is derived from the
  values.

## cassandra-protocol 4.1.0

### Added

* `Envelope::message_body`, which returns the message part of the body,
  skipping a custom payload.
* `ErrorType::CdcWriteFailure`, `ErrorType::CasWriteUnknown` and
  `ErrorType::Unknown`, which keeps the code and body of unknown errors.
* `TopologyChangeType::MovedNode`.
* `types::vector::FloatVector` for binding `vector<float, n>` values.
* `MAX_ENVELOPE_BODY_SIZE`, `ParseEnvelopeError::InvalidBodyLength`,
  `ParseEnvelopeError::InvalidTracingId` and
  `CheckEnvelopeSizeError::InvalidBodyLength`.
* List and map conversions into `PrimitiveDateTime`, `NaiveDateTime`,
  `DateTime<Utc>` and the `NonZero` integers.

### Changed

* `From<f32>` and `From<f64>` for `Decimal` produce the shortest decimal
  which round-trips to the same float, e.g. `19.99` has unscaled value
  `1999` and scale `2`. NaN becomes zero, and infinities become `i64::MAX`
  and `i64::MIN`.
* Custom types other than float vectors decode as `CassandraType::Blob`.
* The "with names for values" flag is derived from the query values.
* The per-statement keyspace is sent on all protocol versions, while
  `now_in_seconds` is sent only on protocol v5.
* Envelopes whose body exceeds `MAX_ENVELOPE_BODY_SIZE` (256 MiB), before
  or after decompression, are rejected.
* Removed the unused `integer-encoding` dependency; `float_eq` is now a
  dev dependency.

### Fixed

* `duration` values are encoded and decoded as CQL vints.
* Malformed input no longer causes panics or unbounded allocations:
  negative or oversized body lengths, truncated tracing ids, string and
  collection lengths (now read as unsigned `[short]` values), reason map
  and row counts, deeply nested column types, and lz4 or snappy bodies
  declaring huge decompressed sizes.
* Protocol v5 framing keeps the read buffer capacity, rejects
  self-contained frames with an incomplete envelope, and verifies the
  decompressed length.
* Custom payloads no longer break request and response parsing. The
  payload stays in the envelope body, so decoding and encoding an envelope
  preserves it.
* Reason maps of protocol v5 `READ_FAILURE` and `WRITE_FAILURE` errors are
  decoded correctly. Endpoints carry no port, so every key's port is 0.
* Protocol v4 CAS write timeouts no longer read the v5-only contentions
  field.
* The partition key count of prepared metadata is serialized only for
  protocol v4 and later.
* `NaiveDateTime` and `DateTime<Utc>` conversions handle timestamps before
  1970 with milliseconds, and out-of-range timestamps return errors.
* List conversions return errors instead of panicking on mismatched or
  malformed elements.
* `Decimal::as_plain` works for scales of 19 and above, and returns zero
  right away for a zero value.
* Typed vector reads work, and float vectors require an exact payload
  length.
* A UDT value truncated in the middle of a field is an error.
* Murmur3 tokens follow Cassandra's partitioner for empty keys and the
  `i64::MIN` hash.
* Batches with named values are rejected.
* `MAX_FRAME_SIZE` is the largest possible frame size.
* Passwords are redacted from `StaticPasswordAuthenticator` and
  `BodyReqAuthResponse` `Debug` output.

### Deprecated

* `QueryParams::with_names`, which is ignored when serializing.
* `QueryParamsBuilder::with_flags` and `QueryParamsBuilder::with_names`.

## cdrs-tokio-helpers-derive 5.1.0

### Added

* Support for generic structs.
* Support for `NaiveDateTime`, `DateTime<Utc>` and `NonZero` integer
  fields.

### Fixed

* A null collection decodes as an empty `Vec` or `HashMap` for fields
  which are not `Option`, and other errors are reported as they are.
* The value type of a `HashMap` is taken from its second generic
  argument, so maps with a custom hasher work.
* Field types are emitted as written, so types named by a path, like
  `uuid::Uuid`, work without importing them.
* Standard library items are referenced by absolute paths, so user types
  named like `Option` or `Vec` no longer break generated code.
* `DbMirror` strips the `r#` prefix from raw identifier column names.
* `None` UDT fields are written as null.
* Documented that `IntoCdrsValue` writes UDT fields in declaration order,
  which has to match the order in the UDT definition.

## cdrs-tokio 9.0.2

### Fixed

* Capped envelope body length before allocating, so a hostile or
  malfunctioning server cannot trigger huge allocations from a single
  4-byte length field.
* Tracing UUID is now read only on response envelopes, matching the
  protocol specification.
* Replaced an `unreachable!()` in the envelope parser with a regular
  error.
* Stream id counter now wraps cleanly back to the initial value instead
  of yielding negative or zero ids.
* Transport write failures only notify handlers for the failed frame,
  not previously sent frames in the same batch.
* Heartbeat interval skips missed ticks instead of bursting after a
  slow round.
* Metadata updates use `rcu` so concurrent topology, status and schema
  events are no longer silently overwritten.
* Control-connection task is aborted when session initialization fails,
  preventing background task leaks.
* Bounded the reprepare retry loop in `exec_with_params` and
  `batch_with_params` to avoid unbounded recursion on persistent
  `Unprepared` responses.
* `ExponentialReconnectionPolicy` saturation hardened with a `>=`
  bounds check.
* `TokenMap::nodes_for_token` and `nodes_for_token_capped` deduplicate
  by endpoint, so vnode-owning nodes appear once per query plan instead
  of filling consecutive replica slots.
* Topology-aware load balancer filters ignored nodes before counting
  per-DC quotas, and before capping the simple-strategy replica list.
* `ConnectionPool` clones the keyspace receiver before opening
  connections, so fresh connections cannot miss a keyspace change that
  landed during pool initialization.
* `serialize_routing_value` precondition is now asserted.

## cassandra-protocol 4.0.1

### Fixed

* Validated lz4 uncompressed length before decoding, rejecting negative
  and oversized values.
* `decode_list` and `decode_map` no longer pre-allocate based on the
  wire-stated count.
* Replaced panics on truncated input with errors in `decode_decimal`,
  `decode_tinyint` and `decode_float_vector`.
* `decode_text`, `decode_varchar`, `decode_ascii` and `decode_custom`
  now surface invalid UTF-8 as an error instead of silently substituting
  replacement characters.
* `Value::from_cursor` now accepts zero-length values.
* `Decimal::as_plain` no longer panics on negative scale.
* `From<f32>`/`From<f64>` for `Decimal` no longer loop indefinitely on
  inexact floats.
* Frame decoder correctly assembles multiple non-self-contained envelopes
  in sequence, and preserves trailing bytes between extracts.

## 9.0.1

### Fixed

* Fixe querying peers in `yugabytedb` (by Andries Hiemstra).

## 9.0.0

### Fixed

* Fixed not re-preparing statements in batch queries.

### New

* New `Session::prepare_raw_tw_with_query_plan()` function.

### Changed

* Removed deprecated functions.

## 8.1.9

### Fixed

* Fixed pool leak (by jojoxhsieh).

## 8.1.8

### Fixed

* Fixed deadlock on transport error (by jojoxhsieh).

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
