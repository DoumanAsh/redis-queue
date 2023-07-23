//! Redis queue

#![warn(missing_docs)]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::style))]

pub use redis;

mod types;
pub use types::{TimestampId, StreamId, TrimMethod, GroupInfo, PendingStats, EntryValue, PendingParams, PendingEntry, FetchType, FetchParams, FetchResult, Entry, RangeIdx, Range};
mod queue;
pub use queue::{Queue, QueueConfig};
pub mod utils;
