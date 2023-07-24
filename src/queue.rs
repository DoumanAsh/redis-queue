use core::{fmt, time};
use core::future::Future;
use std::borrow::Cow;

use redis::{Cmd, ToRedisArgs, RedisError, FromRedisValue};
use redis::aio::ConnectionManager;

use crate::types::{idents, RedisType, TimestampId, StreamId, TrimMethod, GroupInfo, PendingStats, EntryValue, PendingEntry, PendingParams, PendingParamsConfig, FetchParams, FetchParamsConfig, FetchResult};
use crate::iters::{FetchIter, PendingIter};

#[derive(Clone)]
///Queue configuration
pub struct QueueConfig {
    ///Stream name to be used by `Queue`
    pub stream: Cow<'static, str>,
}

#[derive(Clone)]
///Queue
pub struct Queue {
    config: QueueConfig,
    conn: ConnectionManager,
}

impl Queue {
    ///Creates new instance from existing connection
    pub fn new(config: QueueConfig, conn: ConnectionManager) -> Self {
        Self {
            config,
            conn,
        }
    }

    ///Creates command with specified `name` targeting configured `stream`
    fn cmd(&self, name: &str) -> Cmd {
        let mut cmd = Cmd::new();
        cmd.arg(name).arg(self.config.stream.as_ref());
        cmd
    }

    #[inline(always)]
    ///Gets underlying connection
    pub fn connection(&self) -> ConnectionManager {
        self.conn.clone()
    }

    ///Creates group within queue where pending messages are stored.
    ///
    ///User MUST create group before using it.
    ///
    ///If config's `stream` doesn't exist yet, creates it
    pub async fn create_group(&self, group: &str) -> Result<(), RedisError> {
        let mut conn = self.connection();
        let mut cmd = self.cmd(idents::TYPE);
        let typ: RedisType = cmd.query_async(&mut conn).await?;
        match typ {
            //Make sure if key doesn't exist or it is stream, otherwise we should fail immediately
            RedisType::Stream | RedisType::None => (),
            _ => return Err((redis::ErrorKind::ClientError, "key is already used by non-stream type").into()),
        }

        cmd = Cmd::new();
        cmd.arg(idents::XGROUP)
           .arg(idents::CREATE)
           .arg(self.config.stream.as_ref())
           .arg(group)
           .arg("$")
           .arg(idents::MKSTREAM);

        match cmd.query_async(&mut conn).await {
            Ok(()) => Ok(()),
            Err(error) => match error.code() {
                //Group already exists
                Some(idents::BUSYGROUP) => Ok(()),
                _ => Err(error),
            },
        }
    }

    async fn inner_time_ref(conn: &mut ConnectionManager) -> Result<time::Duration, RedisError> {
        let mut cmd = Cmd::new();
        //https://redis.io/commands/time/
        //The TIME command returns the current server time as a two items lists: a Unix timestamp
        //and the amount of microseconds already elapsed in the current second
        cmd.arg(idents::TIME);
        let result: (u64, u64) = cmd.query_async(conn).await?;
        let secs = time::Duration::from_secs(result.0);
        let micros = time::Duration::from_micros(result.1);
        Ok(secs + micros)
    }

    #[inline(always)]
    async fn inner_time(mut conn: ConnectionManager) -> Result<time::Duration, RedisError> {
        Self::inner_time_ref(&mut conn).await
    }

    #[inline(always)]
    ///Returns redis's current time
    pub fn time(&self) -> impl Future<Output = Result<time::Duration, RedisError>> + Send {
        Self::inner_time(self.connection())
    }

    ///Returns number of elements within queue
    pub async fn len(&self) -> Result<usize, RedisError> {
        let mut conn = self.connection();

        let cmd = self.cmd(idents::XLEN);
        cmd.query_async(&mut conn).await
    }

    ///Marks specified `StreamId` as successfully consumed, resulting in corresponding messages' deletion.
    pub async fn consume(&self, group: &str, ids: &[StreamId]) -> Result<usize, RedisError> {
        let mut conn = self.connection();

        let mut cmd = self.cmd(idents::XACK);
        cmd.arg(group).arg(ids);
        cmd.query_async(&mut conn).await
    }

    ///Requests to delete message from the stream.
    pub async fn delete(&self, ids: &[StreamId]) -> Result<usize, RedisError> {
        let mut conn = self.connection();

        let mut cmd = self.cmd(idents::XDEL);
        cmd.arg(ids);
        cmd.query_async(&mut conn).await
    }

    ///Trims elements according to specified `method`
    pub async fn trim(&self, method: TrimMethod) -> Result<u64, RedisError> {
        let mut conn = self.connection();

        let mut cmd = self.cmd(idents::XTRIM);
        cmd.arg(method);
        cmd.query_async(&mut conn).await
    }

    ///Purges whole message stream
    pub async fn purge(&self) -> Result<(), RedisError> {
        let mut conn = self.connection();
        self.cmd("DEL").query_async(&mut conn).await
    }

    ///Retrieves summary of every group existing within stream
    pub async fn groups_info(&self) -> Result<Vec<GroupInfo>, RedisError> {
        let mut conn = self.connection();

        let mut cmd = Cmd::new();
        cmd.arg(idents::XINFO)
           .arg(idents::GROUPS)
           .arg(self.config.stream.as_ref());
        cmd.query_async(&mut conn).await
    }

    ///Retrieves pending messages statistics for `group`
    pub async fn pending_stats(&self, group: &str) -> Result<PendingStats, RedisError> {
        let mut conn = self.connection();

        let mut cmd = self.cmd(idents::XPENDING);
        cmd.arg(&group);

        cmd.query_async(&mut conn).await
    }

    ///Adds item to the queue at the end of queue.
    ///
    ///Returns `StreamId` of newly created item
    pub async fn append<T: ToRedisArgs>(&self, item: &EntryValue<T>) -> Result<StreamId, RedisError> {
        let mut conn = self.connection();

        let mut cmd = self.cmd(idents::XADD);
        cmd.arg("*").arg(item);
        cmd.query_async(&mut conn).await
    }

    ///Adds item to the queue with ID generated from current time plus provided `delay`
    ///
    ///Returns `StreamId` of newly created item
    pub async fn append_delayed<T: ToRedisArgs>(&self, item: &EntryValue<T>, delay: time::Duration) -> Result<StreamId, RedisError> {
        let mut conn = self.connection();
        let now = Self::inner_time_ref(&mut conn).await?.saturating_add(delay);
        let id = TimestampId::new(now);

        let mut cmd = self.cmd(idents::XADD);
        cmd.arg(id).arg(item);
        cmd.query_async(&mut conn).await
    }

    ///Retrieves pending messages within stream.
    pub async fn pending(&self, params: &PendingParams<'_>) -> Result<Vec<PendingEntry>, RedisError> {
        let mut conn = self.connection();

        //Despite its name, `Cmd::arg` can be one or multiple arguments
        //So we can encode everything using ToRedisArgs
        let args = PendingParamsConfig {
            params,
            config: &self.config,
        };
        let mut cmd = Cmd::new();
        cmd.arg(idents::XPENDING).arg(&args);

        cmd.query_async(&mut conn).await
    }

    ///Attempts to fetch message from within queue.
    ///
    ///By new it means messages that are not read yet.
    ///
    ///Once message is read, it is added as pending to group, according to configuration.
    ///
    ///When processing is finished, user must acknowledge ids to remove them from pending group.
    ///Until then these messages can be always re-fetched.
    pub async fn fetch<T: FromRedisValue>(&self, params: &FetchParams<'_>) -> Result<FetchResult<T>, redis::RedisError> {
        let mut conn = self.connection();

        //Despite its name, `Cmd::arg` can be one or multiple arguments
        //So we can encode everything using ToRedisArgs
        let args = FetchParamsConfig {
            params,
            config: &self.config,
        };
        let mut cmd = Cmd::new();
        cmd.arg(idents::XREADGROUP).arg(&args);

        //If there are no messages, it returns NIL so handle it by returning result with empty entires to avoid extra Option indirection
        match cmd.query_async::<_, Option<(FetchResult<T>,)>>(&mut conn).await? {
            Some((res,)) => Ok(res),
            None => Ok(FetchResult {
                stream: args.config.stream.clone().into_owned(),
                entries: Vec::new(),
            }),
        }
    }

    #[inline(always)]
    ///Creates new fetch iterator.
    ///
    ///This is just useful utility when there is no need to change `params` at runtime.
    pub fn fetch_iter<'a>(&self, params: FetchParams<'a>) -> FetchIter<'a> {
        FetchIter::new(params, self.clone())
    }

    #[inline(always)]
    ///Creates new pending info iterator.
    ///
    ///This is just useful utility when there is no need to change `params` at runtime.
    pub fn pending_iter<'a>(&self, params: PendingParams<'a>) -> PendingIter<'a> {
        PendingIter::new(params, self.clone())
    }
}

impl fmt::Debug for Queue {
    #[inline]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Queue").field("name", &self.config.stream).finish()
    }
}
