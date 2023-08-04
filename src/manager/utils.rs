use core::sync::atomic::{AtomicU32, Ordering};

use crate::Queue;
use crate::utils::queue_trim_consumed;
use crate::manager::ConsumerKind;

///Wrapper for `queue_trim_consumed` to perform consuming logic depending on `ConsumerKind` of `manager`
///
///This function is also guarded against performing multiple concurrent trims.
///If task is ongoing, it will skip trim and exit task early.
///Otherwise it starts trim with number of retries as specified via `retry_num`
pub async fn trim_queue_task(queue: Queue, kind: ConsumerKind, retry_num: u32) {
    static STATE: AtomicU32 = AtomicU32::new(0);

    if let ConsumerKind::Extra = kind {
        return;
    }

    let state = STATE.fetch_add(1, Ordering::SeqCst);
    if state == 0 {
        //Only call trim if no previous call exists.
        //If trimming is ongoing there is no need to call again, wait for next opportunity
        queue_trim_consumed(queue, retry_num).await
    }
    STATE.fetch_sub(1, Ordering::SeqCst);
}
