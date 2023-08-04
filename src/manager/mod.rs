//! Queue processing and management
//!
//! ## Usage
//!
//! This module provides utilities to manage and process queue tasks
//!
//! Example below provides brief explanation on how to use it
//!
//! ```rust,no_run
//! use redis_queue::redis;
//! use redis_queue::{Queue, QueueConfig};
//! use redis_queue::types::Entry;
//! use redis_queue::manager::{Manager, ManagerConfig, ConsumerKind, RunParams, manage};
//! use redis_queue::manager::dispatch::{Dispatch, TaskResult, TaskResultKind};
//!
//! use core::future::Future;
//! use core::pin::Pin;
//! use core::time;
//!
//! use tokio::sync::oneshot;
//!
//! ///This is dispatcher to process your tasks
//! struct TaskDispatcher {
//! }
//!
//! impl Dispatch for TaskDispatcher {
//!     type PayloadType = String;
//!     type Future = Pin<Box<dyn Future<Output = TaskResult<String>> + Send + Sync + 'static>>;
//!     fn send(&self, entry: Entry<Self::PayloadType>) -> Self::Future {
//!         //TaskResultKind enum determines how queue manage loop will handle result
//!         //For now only `TempFail` has special handling to retry task,
//!         //while other variants will just result in log entry
//!         todo!();
//!     }
//! }
//!
//! async fn example() {
//!     //Limit on concurrent task number
//!     const MAX_CONCURRENT_TASK: usize = 1000;
//!     //User of your queue, will also connect to the same redis instance and use the same stream name.
//!     let config = QueueConfig {
//!         stream: "stream_name".into()
//!     };
//!     let client = redis::Client::open("redis://127.0.0.1/").expect("to create redis client");
//!     let conn = client.get_tokio_connection_manager().await.expect("to get connection");
//!     let queue = Queue::new(config, conn);
//!
//!     let config = ManagerConfig {
//!         //Group is shared by all consumers
//!         group: "group".into(),
//!         //Use Single if you only need 1 manager.
//!         //Use 1 Main and Extra when deploying multiple.
//!         //Only Main/Single trims queue
//!         kind: ConsumerKind::Single,
//!         //This is unique consumer name.
//!         //Every instance of manager should have own name to avoid clashes
//!         consumer: "consumer-name".into(),
//!         //This is maximum time manager is allowed to block waiting for new messages in queue
//!         poll_time: time::Duration::from_secs(10),
//!         //This is maximum time task that temporary failed will remain in queue.
//!         //Note that it will remain longer if due to concurrency starvation
//!         //it cannot complete at least max_pending_time / poll_time retries
//!         max_pending_time: time::Duration::from_secs(60),
//!     };
//!     let manager = Manager::new(queue, config).await.expect("to create manager");
//!
//!     let (shutdown, shutdown_recv) = oneshot::channel();
//!     let params = RunParams {
//!         manager,
//!         shutdown_recv,
//!         max_task_count: MAX_CONCURRENT_TASK,
//!         dispatcher: TaskDispatcher {
//!         }
//!     };
//!     let handle = tokio::spawn(manage(params));
//!     //Do whatever you want (like wait some signal to shutdown yourself)
//!
//!     //then if you want to shutdown gracefully:
//!     shutdown.send(()).expect("manager lives");
//!     handle.await.expect("finish successfully");
//! }
//! ```

use core::{fmt, time, cmp};
use core::future::Future;
use std::borrow::Cow;

use redis::{RedisError, FromRedisValue};

use crate::Queue;
use crate::iters::{PendingIter, FetchIter};
use crate::types::{StreamId, Range, RangeIdx, FetchParams, PendingParams, FetchType, Entry};

pub mod dispatch;
mod run;
pub use run::{RunParams, manage};
mod utils;
pub use utils::*;

#[derive(Debug)]
///Possible error creating manager
pub enum ConfigError {
    ///Queue group name is not specified
    QueueGroupNameMissing,
    ///Queue group name is invalid
    QueueGroupNameInvalid,
    ///Queue manager name is not specified
    QueueManagerNameMissing,
    ///Queue manager name is invalid
    QueueManagerNameInvalid,
    ///Queue manager poll time is invalid
    QueueManagerPollTimeInvalid,
    ///Queue manager max pending time is invalid
    QueueManagerMaxPendingTimeInvalid,
    ///Redis error happened
    Redis(RedisError),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueGroupNameMissing => fmt.write_str("Queue group name is empty"),
            Self::QueueGroupNameInvalid => fmt.write_str("Queue group name is not valid ASCII string."),
            Self::QueueManagerNameMissing => fmt.write_str("Queue manager name is empty."),
            Self::QueueManagerNameInvalid => fmt.write_str("Queue manager name is not valid ASCII string."),
            Self::QueueManagerPollTimeInvalid => fmt.write_str("Queue manager poll time is not valid positive integer."),
            Self::QueueManagerMaxPendingTimeInvalid => fmt.write_str("Queue manager max pending time is not valid positive integer."),
            Self::Redis(error) => fmt.write_fmt(format_args!("Redis error: {error}")),
        }
    }
}

impl From<RedisError> for ConfigError {
    #[inline(always)]
    fn from(value: RedisError) -> Self {
        Self::Redis(value)
    }
}

impl std::error::Error for ConfigError {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Redis(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
///Describes type of consumer configured.
///
///Derived from name of consumer.
///
///When you want to scale up manager instances you should use naming scheme `<name>-<pod idx>`.
///Then just use `ConsumerKind::determine` to infer type of consumer.
///
///Otherwise introduce own algorithm to master node selection
pub enum ConsumerKind {
    ///Single instance configuration
    ///
    ///This is equivalent to `Main`
    Single,
    ///Main instance which is configured with pod index 0.
    ///
    ///This instance is responsible for maintenance of queue in additional to serving tasks as
    ///`Extra`
    Main,
    ///Additional instance added for scaling purposes.
    ///
    ///This instance is only responsible for processing queue.
    Extra,
}

impl ConsumerKind {
    #[inline]
    ///Determines consumer type from its name
    pub fn determine(name: &str) -> Self {
        let mut split = name.rsplitn(2, '-');
        match split.next().and_then(|idx| idx.parse::<u32>().ok()) {
            Some(0) => Self::Main,
            Some(_) => Self::Extra,
            None => Self::Single,
        }
    }
}

#[derive(Clone)]
///Manager configuration.
pub struct ManagerConfig {
    ///Group name
    ///
    ///All tasks will belong to this group
    pub group: Cow<'static, str>,
    ///Consumer name
    ///
    ///All tasks managed will be claimed by this consumer
    pub consumer: Cow<'static, str>,
    ///Consumer kind
    pub kind: ConsumerKind,
    ///Blocking time while awaiting for new messages.
    pub poll_time: time::Duration,
    ///Duration for which tasks are allowed to remain in queue in order to retry it later.
    ///
    ///Once tasks expires over this time, it shall be deleted from queue.
    pub max_pending_time: time::Duration,
}

#[derive(Clone)]
///Task Queue manager.
pub struct Manager {
    queue: Queue,
    config: ManagerConfig,
}

impl Manager {
    ///Creates new manager from configuration.
    pub async fn new(queue: Queue, config: ManagerConfig) -> Result<Self, ConfigError> {
        if config.poll_time.is_zero() {
            Err(ConfigError::QueueManagerPollTimeInvalid)
        } else if config.max_pending_time.is_zero() {
            Err(ConfigError::QueueManagerMaxPendingTimeInvalid)
        } else if config.group.as_ref().is_empty() {
            Err(ConfigError::QueueGroupNameMissing)
        } else if !config.group.as_ref().is_ascii() {
            Err(ConfigError::QueueGroupNameInvalid)
        } else if config.consumer.as_ref().is_empty() {
            Err(ConfigError::QueueManagerNameMissing)
        } else if !config.consumer.as_ref().is_ascii() {
            Err(ConfigError::QueueManagerNameInvalid)
        } else {
            //Create group on start to make sure Redis config is valid for use.
            queue.create_group(&config.group).await?;

            Ok(Self {
                queue,
                config
            })
        }
    }

    #[inline(always)]
    ///Access underlying queue
    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    #[inline(always)]
    ///Access manager's configuration
    pub fn config(&self) -> &ManagerConfig {
        &self.config
    }

    ///Returns number of re-tries current configuration should allow.
    ///
    ///Generally it is just `min(self.config.max_pending_time / self.config.poll_time, 1)`
    pub fn max_pending_retry_count(&self) -> u64 {
        if self.config.max_pending_time > self.config.poll_time {
            let result =
                self.config.max_pending_time.as_secs_f64() / self.config.poll_time.as_secs_f64();
            //We always have minimum of 1 retry
            cmp::min(result.round() as u64, 1)
        } else {
            //This is generally invalid configuration, but just for the sake being safe
            1
        }
    }

    ///Creates iterator of pending entries in queue.
    ///
    ///`last_id` can be used to specify from where to continue for iteration purpose.
    pub fn pending_tasks(&self, count: usize, last_id: Option<StreamId>) -> PendingIter<'_> {
        let range = Range {
            start: match last_id {
                Some(last_id) => RangeIdx::ExcludeId(last_id),
                None => RangeIdx::Any,
            },
            end: RangeIdx::Any,
        };
        let params = PendingParams {
            group: self.config.group.as_ref(),
            consumer: Some(self.config.consumer.as_ref()),
            range,
            idle: None,
            count,
        };

        PendingIter::new(params, self.queue.clone())
    }

    ///Creates iterator of expired entries in queue
    ///
    ///`last_id` can be used to specify from where to continue for iteration purpose.
    pub fn expired_pending_tasks(&self, count: usize, last_id: Option<StreamId>) -> PendingIter<'_> {
        let range = Range {
            start: match last_id {
                Some(last_id) => RangeIdx::ExcludeId(last_id),
                None => RangeIdx::Any,
            },
            end: RangeIdx::Any,
        };
        let params = PendingParams {
            group: self.config.group.as_ref(),
            consumer: Some(self.config.consumer.as_ref()),
            range,
            idle: Some(self.config.max_pending_time),
            count,
        };

        PendingIter::new(params, self.queue.clone())
    }

    ///Creates iterator over new tasks within queue
    pub fn fetch_new_tasks(&self, count: usize) -> FetchIter {
        let params = FetchParams {
            group: self.config.group.as_ref(),
            consumer: self.config.consumer.as_ref(),
            typ: FetchType::New,
            count,
            timeout: Some(self.config.poll_time),
        };

        FetchIter::new(params, self.queue.clone())
    }

    ///Retrieves task entry by `id`
    ///
    ///## Implementation
    ///
    ///Due to Redis not providing any method to get message by id, we have to emulate it by doing
    ///query for message after `id - 1` to fetch message by `id`.
    ///
    ///If message is no longer exist, we return `None`.
    ///
    ///Note that when reading pending message data, there is no timeout possible
    ///If there is no message, it will return `None`
    pub async fn get_pending_by_id<T: FromRedisValue>(&self, id: StreamId) -> Result<Option<Entry<T>>, RedisError> {
        let mut iter = self.fetch_new_tasks(1);
        iter.set_cursor(FetchType::After(id.prev()));
        let mut result = iter.next_entries().await?;
        if let Some(item) = result.pop() {
            if item.id != id {
                Ok(None)
            } else {
                Ok(Some(item))
            }
        } else {
            Ok(None)
        }
    }

    #[inline]
    ///Consumes tasks by specified IDs.
    ///
    ///If error is returned, `tasks` modified with cleaned IDs removed.
    pub async fn consume_tasks(&self, tasks: &[StreamId]) -> Result<usize, RedisError> {
        self.queue.consume(&self.config.group, tasks).await
    }

    #[inline(always)]
    ///Performs queue trimming removing all tasks that are consumed
    fn trim_queue(&self, retry_num: u32) -> impl Future<Output = ()> + Send + 'static {
        //This gives this future priority by disable scheduler within tokio.
        //
        //We should finish it as soon as possible and only call on demand
        tokio::task::unconstrained(trim_queue_task(
            self.queue().clone(),
            self.config().kind,
            retry_num,
        ))
    }
}
