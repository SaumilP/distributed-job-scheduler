//! Library half of `scheduler-worker`, so the delivery handling is testable
//! without a broker and the end-to-end test can compose the worker in-process.

pub mod handler;
