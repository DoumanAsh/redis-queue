//!Redis queue utilities

use crate::{Queue, TrimMethod};

use core::time;

const SLEEP_INTERVAL: time::Duration = time::Duration::from_secs(1);

#[tracing::instrument(skip(queue))]
///Utility to trim redis stream of all messages that are consumed by everyone
///
///## Algorithm:
///
///- Enumerate all groups using the queue to find `last-delivered-id` which will show ID of last
///messaged which was read from queue;
///- Enumerate all pending messages within group to find lower consumed id of every consumer;
///- If no message delivered yet, then do nothing
///- Else If no pending message is present, then perform `XTRIM MINID min(last_delivered_ids) + 1`
///resulting in all messages, including `min(last_delivered_ids)` deleted from queue
///- Otherwise if least on pending message is present, then perform `XTRIM MINID min(lowest_ids)`
///resulting in all messages except `min(lowest_ids)`
///
///## Re-try mechanism
///
///if `max_retry` is above 1, then, if redis temporary unavailable, task will sleep `1s * try_number` and try again.
pub async fn queue_trim_consumed(queue: Queue, max_retry: u32) {
    match queue.len().await {
        //Nothing to do as there is no entries
        Ok(0) => return,
        Err(error) => {
            tracing::warn!("redis len failed: {error}");
            return;
        }
        Ok(_) => (),
    }

    let mut retries = 1;

    let groups = loop {
        match queue.groups_info().await {
            Ok(groups) => break groups,
            Err(error) => {
                if retries <= max_retry {
                    if error.is_timeout() || error.is_connection_dropped() || error.is_connection_refusal() {
                        tracing::info!("redis temp. unavailable: {error}");
                        tokio::time::sleep(SLEEP_INTERVAL * retries).await;
                        retries += 1;
                        continue;
                    }
                }
                tracing::warn!("group info failed: {error}");
                return;
            }
        }
    };

    if groups.is_empty() {
        return;
    }

    let mut lowest_ids = Vec::with_capacity(groups.len());
    let mut last_delivered_ids = Vec::with_capacity(groups.len());
    for group in groups.iter() {
        //If id is 0-0 it means there is yet to be a single message delivery
        //as such we cannot even consider this ID for use, so skip it and do not try to fetch
        //pending status
        if group.last_delivered_id.is_nil() {
            continue;
        }

        last_delivered_ids.push(group.last_delivered_id);

        retries = 1;
        let stats = loop {
            match queue.pending_stats(&group.name).await {
                Ok(stats) => break stats,
                Err(error) => {
                    if retries <= max_retry {
                        if error.is_timeout() || error.is_connection_dropped() || error.is_connection_refusal() {
                            tracing::info!("redis temp. unavailable: {error}");
                            tokio::time::sleep(SLEEP_INTERVAL * retries).await;
                            retries += 1;
                            continue;
                        }
                    }
                    tracing::warn!("group pending stats failed: {error}");
                    return;
                }
            }
        };
        //if length is 0, there is no ID of interest, skip it
        if stats.len != 0 {
            lowest_ids.push(stats.lowest_id);
        }
    }

    if last_delivered_ids.is_empty() {
        //No message delivered yet, skip
        return;
    }

    //We sort ids in order to get common ID between all groups that consumes messages from this
    //stream, so that we only delete message only once all groups consumed message.
    lowest_ids.sort_unstable();
    last_delivered_ids.sort_unstable();

    let trim = match lowest_ids.get(0) {
        None => {
            //If there are no pending message at all it means that user consumed all delivered messages
            //hence trim queue of messages with id <= last_delivered_id
            let last_delivered_id = last_delivered_ids[0];
            TrimMethod::MinId(last_delivered_id.next())
        }
        Some(lowest_pending_id) => {
            //if lowest_ids present, it means there are pending messages, so we need to limit deletion to
            //only removing messages with id < lowest_pending_id
            TrimMethod::MinId(*lowest_pending_id)
        }
    };

    retries = 0;
    loop {
        match queue.trim(trim).await {
            Ok(number) => {
                tracing::info!("Removed {number} entries");
                return;
            }
            Err(error) => {
                if retries <= max_retry {
                    if error.is_timeout() || error.is_connection_dropped() || error.is_connection_refusal() {
                        tracing::info!("redis temp. unavailable: {error}");
                        tokio::time::sleep(SLEEP_INTERVAL * retries).await;
                        retries += 1;
                        continue;
                    }
                }
                tracing::warn!("stream trim failed: {error}");
                return;
            }
        }
    }
}
