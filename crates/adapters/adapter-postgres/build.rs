//! Forces a rebuild when the migration set changes.
//!
//! `sqlx::migrate!("./migrations")` embeds the migrations into the binary at
//! *compile* time. Cargo, on its own, has no idea that `src/lib.rs` depends on
//! the contents of `migrations/` -- so adding a new `.sql` file does not
//! invalidate the cached build, and the new migration is silently omitted from
//! the embedded migrator. The failure mode is nasty: tests pass against a
//! schema that no longer matches the repository, and the omission only shows up
//! later as "the constraint I added isn't there".
//!
//! This was not hypothetical -- migration 0002 was invisible to the test suite
//! until this file existed.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
