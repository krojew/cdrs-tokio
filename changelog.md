## x.y.z

## New

* New `listen_tls` and `listen_tls_blocking` functions for listening for server events.
* New `ReconnectionPolicy` used when trying to re-establish connections to downed nodes.
* `Error` now implements standard `Error`.
* `SessionBuilder` introduced as the preferred way to create a session.
* Added missing traits for `BatchType` and `QueryFlags`.

### Changed

* Rewritten transport layer for massive performance improvements (including removing `bb8`). This
  involves changing a large portion of public API related to transport and server events.
* Changed `Target` and `ChangeType` enums to `SchemaChangeTarget` and `SchemaChangeType`.
* The `varint` type now uses `num::BigInt` representation (this implies `Decimal` also uses "big" types).
* Removed `unstable-dynamic-cluster` feature, since it wasn't working as expected and introduced performance penalty.
* Removed `AsBytes` in favor of new `Serialize` trait due to performance penalty.
* Removed `FromSingleByte` and `AsByte` in favor of `From`/`TryFrom`.
* Removed traits along with `async-trait` dependency: `BatchExecutor`, `ExecExecutor`, `PrepareExecutor`, 
  `QueryExecutor`, `GetConnection` and `CdrsSession`. Everything is now embedded directly in `Session`.
* Load balancing strategy now returns query plans, rather than individual nodes.
* Removed `SingleNode` load balancing strategy.

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
