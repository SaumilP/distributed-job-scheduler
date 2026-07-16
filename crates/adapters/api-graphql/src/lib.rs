//! GraphQL driving adapter.
//!
//! A peer of `api-rest` and `api-grpc`, over the same ports. The part worth
//! reading is [`loader`]: nested `runs` resolve through a `DataLoader` so N
//! jobs cost one lookup, not N.

pub mod loader;
pub mod schema;

pub use schema::{MAX_COMPLEXITY, MAX_DEPTH, MAX_PAGE, SchedulerSchema, build};

/// The axum service that answers `POST /graphql`.
///
/// Re-exported so the composition root can mount the schema without naming
/// async-graphql itself. Both async-graphql crates are pinned to `=7.0.17`
/// here for MSRV reasons (see `Cargo.toml`); a third pin site in the binary
/// would be a third thing to forget when that pin is finally lifted.
pub use async_graphql_axum::GraphQL;

/// Request types, for callers that execute against the schema directly rather
/// than over HTTP -- the end-to-end test does, to query a real database
/// through the real resolvers without standing up a server.
///
/// Re-exported for the same reason as [`GraphQL`]: both async-graphql crates
/// are pinned to `=7.0.17` here for MSRV reasons (see `Cargo.toml`), and every
/// additional crate that names `async-graphql` in its own manifest is another
/// pin to find and update when that pin is finally lifted.
pub use async_graphql::{Request, Variables};

/// The GraphiQL playground page, pointed at `endpoint`.
///
/// Served on `GET /graphql` -- the same path the queries POST to, which is the
/// convention every GraphQL client already assumes.
pub fn playground_html(endpoint: &str) -> String {
    async_graphql::http::GraphiQLSource::build()
        .endpoint(endpoint)
        .title("scheduler")
        .finish()
}
