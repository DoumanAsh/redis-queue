//! Redis queue

#![warn(missing_docs)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::style))]

pub use redis;

pub mod types;
mod queue;
pub use queue::{Queue, QueueConfig};
pub mod utils;
