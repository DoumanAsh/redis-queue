use crate::manager::Manager;
use crate::manager::dispatch::{Dispatch, TaskResult};

use core::future::Future;
use core::{cmp, time};
use std::time::Instant;

use tokio::sync::oneshot;
use redis::RedisError;

const TIMEOUT_INTERVAL: time::Duration = time::Duration::from_secs(5);

///Scheduler to control manager loop
struct TimeScheduler {
    timeout_interval: time::Duration,
    timeout_limit: time::Duration,
    on_error_timeout: time::Duration,
}

impl TimeScheduler {
    #[inline(always)]
    ///Creates new instance with specified `timeout_limit`
    ///
    ///All timeouts returned by this scheduler will be limited to this limit
    pub fn new(timeout_limit: time::Duration) -> Self {
        Self {
            timeout_interval: TIMEOUT_INTERVAL,
            on_error_timeout: TIMEOUT_INTERVAL,
            timeout_limit,
        }
    }

    #[inline(always)]
    ///Reports Redis working
    pub fn on_redis_recovery(&mut self) {
        self.on_error_timeout = self.timeout_interval;
    }

    #[inline(always)]
    ///Reports new redis error, returning timeout to sleep for future retry if re-try is possible.
    pub fn next_redis_error(&mut self, error: RedisError) -> Result<time::Duration, RedisError> {
        if error.is_timeout() || error.is_connection_refusal() || error.is_connection_dropped() {
            tracing::info!("Redis temporary unavailable: {error}");
            //increase by `timeout_interval` and cap by `timeout_limit`
            self.on_error_timeout = cmp::min(self.timeout_limit, self.on_error_timeout + self.timeout_interval);
            Ok(self.on_error_timeout)
        } else {
            Err(error)
        }
    }
}

///Parameters for `manage` function
pub struct RunParams<T> {
    ///Manager
    pub manager: Manager,
    ///Shutdown channel
    pub shutdown_recv: oneshot::Receiver<()>,
    ///Maximum number of new tasks to add for execution.
    ///
    ///If queue has more tasks than this number, it will try to complete these tasks first before
    ///trying to fetch again.
    ///Once it exceeds poll_time, it stops fetching and goes for next iteration.
    pub max_task_count: usize,
    ///Dispatcher for incoming messages
    pub dispatcher: T,
}

#[tracing::instrument(skip(params), fields(consumer = params.manager.config().consumer.as_ref()))]
///Starts main loop using provided parameters.
pub async fn manage<T: Dispatch>(params: RunParams<T>) where T::Future: Future<Output = TaskResult<T::PayloadType>> {
    let RunParams { manager, mut shutdown_recv, max_task_count, dispatcher } = params;

    let mut scheduler = TimeScheduler::new(manager.config().poll_time);

    let max_retry = manager.max_pending_retry_count();

    let mut expired_tasks = manager.expired_pending_tasks(max_task_count, None);
    let mut fetch_new_tasks = manager.fetch_new_tasks(max_task_count);

    let mut ongoing_tasks = Vec::new();
    let mut completed_tasks = Vec::new();
    let mut consumed_tasks_number = 0usize;

    //Do initial cleanup before starting processing tasks
    manager.trim_queue(10).await;

    'main: loop {
        ///Generates error handling code which uses `TimeScheduler` to decide next timeout if redis
        ///error indicates ability to retry.
        ///
        ///If error cannot be handled, it breaks 'main loop, exiting this function
        ///
        ///- `error` is identifier with variable of error
        ///- `ok` optional label to specify loop label to continue after sleep.
        macro_rules! on_redis_error {
            ($error:ident where OK=$($ok:tt)*) => {
                match scheduler.next_redis_error($error) {
                    Ok(sleep) => {
                        tracing::info!("Retry in {}s", sleep.as_secs());
                        tokio::time::sleep(sleep).await;
                        continue $($ok)*;
                    }
                    Err(error) => {
                        tracing::error!("Redis queue cannot be processed: {error}");
                        //We always exit loop on fatal error as it means Redis is not usable
                        break 'main;
                    }
                }
            }
        }

        if let Err(error) = expired_tasks.set_range_until_now().await {
            on_redis_error!(error where OK='main);
        } else {
            scheduler.on_redis_recovery();
        }

        //Consume expired tasks, if any
        'expired_tasks: loop {
            match expired_tasks.next().await {
                Ok(tasks) if tasks.is_empty() => {
                    break 'expired_tasks;
                }
                Ok(tasks) => {
                    //filter out all tasks that are not tried enough times for whatever reason.
                    //Generally we should allow following number of tries: max_pending_time / poll_time or at least 1+ attempt
                    let tasks = tasks.iter().filter(|entry| entry.count > max_retry).map(|entry| entry.id).collect::<Vec<_>>();
                    if tasks.is_empty() {
                        break 'expired_tasks;
                    }

                    if let Err(error) = manager.consume_tasks(&tasks).await {
                        on_redis_error!(error where OK=);
                    } else {
                        break 'expired_tasks;
                    }
                }
                Err(error) => on_redis_error!(error where OK=),
            }
        } //'expired_tasks

        scheduler.on_redis_recovery();

        //Re-visit failed tasks to see if we should re-try new ones
        'failed_tasks: loop {
            let mut pending = manager.pending_tasks(max_task_count, None);
            //IDLE time should be limited to avoid re-trying too much
            pending.set_idle(manager.config().poll_time);
            'failed_tasks_end_range: loop {
                if let Err(error) = pending.set_range_until_now().await {
                    on_redis_error!(error where OK='failed_tasks_end_range);
                } else {
                    scheduler.on_redis_recovery();
                    break 'failed_tasks_end_range;
                }
            }

            'failed_tasks_fetch: loop {
                match pending.next().await {
                    Ok(tasks) if tasks.is_empty() => break 'failed_tasks,
                    Ok(tasks) => {
                        for task in tasks {
                            match manager.get_pending_by_id(task.id).await {
                                Ok(Some(task)) => ongoing_tasks.push(dispatcher.send(task)),
                                Ok(None) => (),
                                Err(error) => on_redis_error!(error where OK=),
                            }
                        }
                    }
                    Err(error) => on_redis_error!(error where OK='failed_tasks_fetch),
                }
            }
        } //'failed_tasks

        scheduler.on_redis_recovery();

        let new_tasks_started = Instant::now();
        fetch_new_tasks.set_timeout(manager.config().poll_time);
        fetch_new_tasks.set_count(max_task_count);
        let mut new_tasks_cap = max_task_count;

        macro_rules! process_tasks {
            () => {
                for ongoing in ongoing_tasks.drain(..) {
                    let result = ongoing.await;
                    tracing::debug!("task(redis={}, user_id={}): {:?}", result.data.id, result.data.value.id, result.kind);
                    if !result.kind.is_need_retry() {
                        completed_tasks.push(result.data.id);
                    }
                }
                //Clean up all completed tasks
                if !completed_tasks.is_empty() {
                    'completed_tasks: loop {
                        match manager.consume_tasks(&completed_tasks).await {
                            Ok(_) => {
                                tracing::info!("Completed {} tasks", completed_tasks.len());
                                consumed_tasks_number = consumed_tasks_number.saturating_add(completed_tasks.len());
                                completed_tasks.clear();
                                break 'completed_tasks;
                            }
                            Err(error) => on_redis_error!(error where OK='completed_tasks),
                        }
                    }
                    scheduler.on_redis_recovery();
                }
            };
        }

        //Fetch all new tasks available
        'new_tasks: loop {
            match fetch_new_tasks.next_entries().await {
                Ok(tasks) if tasks.is_empty() => {
                    break;
                }
                Ok(tasks) => {
                    tracing::info!("Fetched {} tasks", tasks.len());
                    let timestamp = 'new_tasks_now: loop {
                        match manager.queue().time().await {
                            Ok(timestamp) => break timestamp,
                            Err(error) => on_redis_error!(error where OK='new_tasks_now),
                        }
                    };

                    for task in tasks.into_iter() {
                        //If task is scheduler in future, it's timestamp of id will be greater than
                        //current redis time
                        if task.id.as_timestamp() <= timestamp {
                            new_tasks_cap = new_tasks_cap.saturating_sub(1);
                            ongoing_tasks.push(dispatcher.send(task));
                        } else {
                            tracing::debug!("task(id={}) scheduled in future. Current time={}", task.id, timestamp.as_millis());
                        }
                    }

                    if new_tasks_cap == 0 {
                        //Being capped we should start executing tasks immediately
                        process_tasks!();
                        new_tasks_cap = max_task_count;
                    }

                    //After that we check if there is still some time within poll interval to do more work
                    let elapsed = new_tasks_started.elapsed();
                    if let Some(new_timeout) = manager.config().poll_time.checked_sub(elapsed) {
                        //Once left over time is below 1 second, there is no need to poll further, wait for next iteration
                        if new_timeout.as_secs() == 0 {
                            break 'new_tasks;
                        }

                        fetch_new_tasks.set_timeout(new_timeout);
                        fetch_new_tasks.set_count(new_tasks_cap);
                    } else {
                        break 'new_tasks;
                    }
                }
                Err(error) => on_redis_error!(error where OK='new_tasks),
            }
        } //'new_tasks

        //Consume leftovers
        process_tasks!();

        match shutdown_recv.try_recv() {
            Ok(_) => {
                tracing::info!("Shutdown requested");
                if consumed_tasks_number > 0 {
                    manager.trim_queue(1).await;
                }
                break 'main;
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                tracing::info!("Unexpected termination");
                if consumed_tasks_number > 0 {
                    manager.trim_queue(1).await;
                }
                break 'main;
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                if consumed_tasks_number > 0 {
                    manager.trim_queue(10).await;
                    consumed_tasks_number = 0;
                }
                continue 'main;
            }
        }
    } //'main
}
