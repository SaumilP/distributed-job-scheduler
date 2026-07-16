//! gRPC driving adapter.
//!
//! A peer of `api-rest` and `api-graphql` over the same ports.
//!
//! The `.proto` is compiled at build time by **protox**, a pure-Rust protobuf
//! compiler, rather than by shelling out to `protoc`. That is deliberate: a
//! reference repository people clone should build with nothing but a Rust
//! toolchain, and requiring a system protobuf compiler is a setup step that
//! will silently break for someone.

pub mod pb {
    tonic::include_proto!("scheduler.v1");
}

pub mod service;

pub use service::SchedulerService;
