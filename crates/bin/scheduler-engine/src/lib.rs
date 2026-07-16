//! Library half of `scheduler-engine`, so the loops are testable and the
//! end-to-end test can compose the engine in-process.

pub mod drain;
pub mod loops;
