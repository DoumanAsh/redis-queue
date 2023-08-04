//! Redis queue
//!
//! ## Features
//!
//! - `manager` - Enables `manager` module to provide utilities to process tasks within queue

#![warn(missing_docs)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::style))]

pub use redis;

pub mod iters;
pub mod types;
mod queue;
pub use queue::{Queue, QueueConfig};
#[cfg(feature = "manager")]
pub mod manager;
pub mod utils;
