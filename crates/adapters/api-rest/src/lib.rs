//! REST driving adapter.
//!
//! A peer of `api-grpc` and `api-graphql`: it translates HTTP into calls on
//! the repositories and owns no domain logic. It lives here rather than inside
//! the binary so all three transports are structurally the same kind of thing
//! — in a repository whose subject is architecture, having one surface be a
//! module of the composition root while the others are adapters would teach
//! the wrong lesson.
//!
//! The routes are a library so tests can build the real `Router` and drive it
//! through `tower::ServiceExt::oneshot` — no listener, no port allocation,
//! nothing to be flaky about.

pub mod routes;
