//! Pseudo iterators types to query Queue

use core::time;

use crate::types::{RangeIdx, TimestampId, FetchType, FetchParams, FetchResult, PendingParams, PendingEntry, StreamId, Entry};
use crate::queue::Queue;

use redis::{RedisError, FromRedisValue};

///Iterator to fetch messages from within queue.
///
///If fetch parameters are to query new messages, it will only return freshly added messages.
///Once these messages are fetched, they are considered pending and no longer returned by this
///iterator
///
///If fetch parameter specifies to return pending messages within queue, then iterator will resume
///after last message id.
pub struct FetchIter<'a> {
    params: FetchParams<'a>,
    queue: Queue,
}

impl<'a> FetchIter<'a> {
    #[inline(always)]
    ///Creates new iterator
    pub fn new(params: FetchParams<'a>, queue: Queue) -> Self {
        Self {
            params,
            queue
        }
    }

    #[inline(always)]
    ///Sets number of items to fetch
    pub fn set_count(&mut self, count: usize) {
        self.params.count = count;
    }

    #[inline(always)]
    ///Sets cursor to position where to iterate from.
    pub fn set_cursor(&mut self, typ: FetchType) {
        self.params.typ = typ;
    }

    #[inline(always)]
    ///Sets time waiting to fetch data.
    ///
    ///Redis will return reply within this time, not necessary immediately even if data is available.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.params.timeout = Some(timeout);
    }

    ///Performs fetch, returning messages depending on `FetchParams::typ`.
    ///
    ///If pending messages are fetched, moves cursor to last pending message after fetched messages.
    ///
    ///We fetch our task as raw bytes to ensure we can always get it, regardless how it was
    ///serialized.
    pub async fn next<T: FromRedisValue>(&mut self) -> Result<FetchResult<T>, RedisError> {
        let result = self.queue.fetch(&self.params).await?;
        match self.params.typ {
            FetchType::New => (),
            FetchType::Pending | FetchType::After(_) => {
                if let Some(entry) = result.entries.last() {
                    self.params.typ = FetchType::After(entry.id);
                }
            }
        }
        Ok(result)
    }

    ///Performs fetch, returning messages depending on `FetchParams::typ`.
    ///
    ///If pending messages are fetched, moves cursor to last pending message after fetched messages.
    ///
    ///We fetch our task as raw bytes to ensure we can always get it, regardless how it was
    ///serialized.
    ///
    ///Differently from `next` it returns only list of entries
    pub async fn next_entries<T: FromRedisValue>(&mut self) -> Result<Vec<Entry<T>>, RedisError> {
        let result = self.queue.fetch_entries(&self.params).await?;
        match self.params.typ {
            FetchType::New => (),
            FetchType::Pending | FetchType::After(_) => {
                if let Some(entry) = result.entries.last() {
                    self.params.typ = FetchType::After(entry.id);
                }
            }
        }

        Ok(result.entries)
    }
}

///Iterator over expired pending messages.
pub struct PendingIter<'a> {
    params: PendingParams<'a>,
    queue: Queue,
}

impl<'a> PendingIter<'a> {
    #[inline(always)]
    ///Creates new iterator
    pub fn new(params: PendingParams<'a>, queue: Queue) -> Self {
        Self {
            params,
            queue
        }
    }

    #[inline(always)]
    ///Sets cursor position to start after specified `id`
    pub fn set_cursor(&mut self, id: StreamId) {
        self.params.range.start = RangeIdx::ExcludeId(id);
    }

    #[inline(always)]
    ///Sets IDLE timeout, essentially filtering out messages not older than specified `duration`
    pub fn set_idle(&mut self, duration: time::Duration) {
        self.params.idle = Some(duration);
    }

    ///Sets end range to task scheduled for execution right now.
    pub async fn set_range_until_now(&mut self) -> Result<(), RedisError> {
        let timestamp = self.queue.time().await?;
        self.params.range.end = RangeIdx::Timestamp(TimestampId::new(timestamp));
        Ok(())
    }

    ///Attempts to peek at pending messages.
    pub async fn next(&mut self) -> Result<Vec<PendingEntry>, RedisError> {
        let result = self.queue.pending(&self.params).await?;

        if let Some(entry) = result.last() {
            self.params.range.start = RangeIdx::ExcludeId(entry.id);
        }

        Ok(result)
    }
}
