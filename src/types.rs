use core::{fmt, time, cmp};
use core::str::FromStr;
use core::convert::TryInto;

use redis::{self, FromRedisValue, ErrorKind, Value, RedisResult, RedisError, RedisWrite, ToRedisArgs};

use crate::queue::QueueConfig;

pub mod idents {
    macro_rules! define_term {
        ($($ident:ident),+) => {
            $(
                pub const $ident: &str = stringify!($ident);
            )+
        };
    }

    define_term!(TYPE, XGROUP, CREATE, MKSTREAM, BUSYGROUP, TIME, XLEN, XADD, XREADGROUP, XPENDING, XACK, XDEL);
    define_term!(XINFO, GROUPS, XTRIM);
    define_term!(GROUP, COUNT, BLOCK, STREAMS, IDLE, MAXLEN, MINID);
}

fn parse_redis_key(value: &redis::Value) -> Result<&str, RedisError> {
    match value {
        redis::Value::Data(ref data) => match core::str::from_utf8(data) {
            Ok(key) => Ok(key),
            Err(_) => Err((redis::ErrorKind::TypeError, "Non-UTF8 stream field's name").into()),
        },
        _ => Err((redis::ErrorKind::TypeError, "Invalid stream field's name").into()),
    }
}

macro_rules! assign_field_if {
    ($field:ident = $value:ident IF $key:ident == $expected:expr) => {
        if $field.is_none() && $key.eq_ignore_ascii_case($expected) {
            $field = Some(FromRedisValue::from_redis_value($value)?);
            continue;
        }
    };
}

#[cold]
#[inline(never)]
fn unlikely_redis_error(kind: redis::ErrorKind, text: &'static str) -> RedisError {
    (kind, text).into()
}

macro_rules! unwrap_required_field {
    ($field:ident) => {
        match $field {
            Some(field) => field,
            None => {
                return Err(unlikely_redis_error(
                    redis::ErrorKind::TypeError,
                    concat!("'", stringify!($field), "' is missing"),
                ))
            }
        }
    };
}

#[derive(Debug, Copy, Clone)]
///Trimming method
pub enum TrimMethod {
    ///Request to clean exceeding specified number of elements.
    MaxLen(u64),
    ///Request to clean entries below specified id.
    ///
    ///Supported since Redis 6.2
    MinId(StreamId),
}

impl ToRedisArgs for TrimMethod {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, out: &mut W) {
        match self {
            Self::MaxLen(threshold) => {
                idents::MAXLEN.write_redis_args(out);
                threshold.write_redis_args(out);
            }
            Self::MinId(id) => {
                idents::MINID.write_redis_args(out);
                id.write_redis_args(out);
            }
        }
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        false
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
///Possible types returned by `TYPE` command
pub enum RedisType {
    ///String
    String,
    ///List
    List,
    ///Set
    Set,
    ///Zset
    ZSet,
    ///Hash
    Hash,
    ///Stream
    Stream,
    ///None, which means key doesn't exist
    None,
}

impl RedisType {
    #[inline(always)]
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("string") {
            Some(Self::String)
        } else if value.eq_ignore_ascii_case("list") {
            Some(Self::List)
        } else if value.eq_ignore_ascii_case("set") {
            Some(Self::Set)
        } else if value.eq_ignore_ascii_case("zset") {
            Some(Self::ZSet)
        } else if value.eq_ignore_ascii_case("hash") {
            Some(Self::Hash)
        } else if value.eq_ignore_ascii_case("stream") {
            Some(Self::Stream)
        } else if value.eq_ignore_ascii_case("none") {
            Some(Self::None)
        } else {
            None
        }
    }
}

impl FromRedisValue for RedisType {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Bulk(_) => Err((ErrorKind::TypeError, "Not a single value").into()),
            Value::Data(value) => match core::str::from_utf8(value) {
                Ok(value) => match Self::parse(value) {
                    Some(result) => Ok(result),
                    None => Err((ErrorKind::TypeError, "Not a type").into()),
                },
                Err(_) => Err((ErrorKind::TypeError, "Not a string").into()),
            },
            Value::Nil => Err((ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(response) => match Self::parse(response) {
                Some(result) => Ok(result),
                None => Err((ErrorKind::TypeError, "Not a type").into()),
            },
        }
    }

    fn from_byte_vec(vec: &[u8]) -> Option<Vec<Self>> {
        match core::str::from_utf8(vec) {
            Ok(value) => Self::parse(value).map(|val| vec![val]),
            Err(_) => None,
        }
    }
}

#[derive(Debug)]
///Possible errors parsing Redis Stream ID
pub enum StreamIdParseError {
    ///Not compatible type (non-string or empty)
    InvalidType,
    ///Timestamp is not valid zero or positive integer
    InvalidTimestamp,
    ///Sequence part of Stream's ID is missing
    MissingSequence,
    ///Sequence is not valid zero or positive integer
    InvalidSequence,
}

impl StreamIdParseError {
    #[inline(always)]
    const fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidType => "Not a valid stream id",
            Self::InvalidTimestamp => "Invalid timestamp",
            Self::MissingSequence => "Missing sequence part",
            Self::InvalidSequence => "Invalid sequence number",
        }
    }
}

impl fmt::Display for StreamIdParseError {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str(self.as_str())
    }
}

impl std::error::Error for StreamIdParseError {}

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
///Timestamp component of `StreamId`
///
///When used as argument, written as `{timestamp}`
///
///Second part of id is automatically generated by Queue
pub struct TimestampId {
    ///Timestamp (in milliseconds)
    timestamp: u64,
}

impl TimestampId {
    #[inline]
    ///Creates new id from duration.
    ///
    ///timestamp is limited to `u64::max_value()`
    pub fn new(timestamp: time::Duration) -> Self {
        Self {
            //Limit timestamp to u64::max_value() in milliseconds.
            timestamp: match timestamp.as_millis().try_into() {
                Ok(res) => res,
                Err(_) => u64::max_value(),
            },
        }
    }
}

impl fmt::Debug for TimestampId {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { timestamp } = self;
        fmt::Debug::fmt(timestamp, fmt)
    }
}

impl fmt::Display for TimestampId {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { timestamp } = self;
        fmt.write_fmt(format_args!("{timestamp}-*"))
    }
}

impl ToRedisArgs for TimestampId {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        self.timestamp.write_redis_args(out)
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        true
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
///Auto-generated stream key
pub struct StreamId {
    ///Timestamp (in milliseconds)
    timestamp: u64,
    ///Sequence number within `timestamp,in case of multiple items being placed at the same time
    seq: u64,
}

impl StreamId {
    #[inline(always)]
    ///Default value in case of NULL value.
    pub const fn nil() -> Self {
        Self { timestamp: 0, seq: 0 }
    }

    #[inline(always)]
    ///Checks whether id is nil.
    ///
    ///Both this type and redis itself uses `0-0` to indicate initial null id
    pub const fn is_nil(&self) -> bool {
        self.timestamp == 0 && self.seq == 0
    }

    #[inline(always)]
    ///Returns UNIX timestamp
    pub const fn as_timestamp(&self) -> time::Duration {
        time::Duration::from_millis(self.timestamp)
    }

    ///Returns next id after `self`
    pub const fn next(&self) -> Self {
        if self.timestamp == u64::max_value() {
            Self {
                timestamp: self.timestamp,
                seq: self.seq.saturating_add(1),
            }
        } else if self.seq == u64::max_value() {
            Self {
                timestamp: self.timestamp.saturating_add(1),
                seq: 0,
            }
        } else {
            Self {
                timestamp: self.timestamp,
                seq: self.seq + 1,
            }
        }
    }

    ///Returns next id after `self`
    pub const fn prev(&self) -> Self {
        if self.timestamp == 0 {
            Self {
                timestamp: self.timestamp,
                seq: self.seq.saturating_sub(1),
            }
        } else if self.seq == 0 {
            Self {
                timestamp: self.timestamp.saturating_sub(1),
                seq: 0,
            }
        } else {
            Self {
                timestamp: self.timestamp,
                seq: self.seq - 1,
            }
        }
    }
}

impl fmt::Debug for StreamId {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { timestamp, seq } = self;
        fmt::Debug::fmt(&(timestamp, seq), fmt)
    }
}

impl PartialOrd for StreamId {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        PartialOrd::partial_cmp(&(self.timestamp, self.seq), &(other.timestamp, other.seq))
    }
}

impl Ord for StreamId {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Ord::cmp(&(self.timestamp, self.seq), &(other.timestamp, other.seq))
    }
}

impl FromStr for StreamId {
    type Err = StreamIdParseError;

    fn from_str(data: &str) -> Result<Self, Self::Err> {
        let mut split = data.split('-');
        let timestamp = match split.next() {
            Some(timestamp) => match timestamp.parse() {
                Ok(timestamp) => timestamp,
                Err(_) => {
                    return Err(StreamIdParseError::InvalidTimestamp);
                }
            },
            None => return Err(StreamIdParseError::InvalidType),
        };

        let seq = match split.next() {
            Some(seq) => match seq.parse() {
                Ok(seq) => seq,
                Err(_) => return Err(StreamIdParseError::InvalidSequence),
            },
            None => return Err(StreamIdParseError::MissingSequence),
        };

        Ok(Self { timestamp, seq })
    }
}

impl fmt::Display for StreamId {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { timestamp, seq } = self;
        fmt.write_fmt(format_args!("{timestamp}-{seq}"))
    }
}

impl ToRedisArgs for StreamId {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        const STREAM_MAX_SIZE: usize = 20 + 1 + 20;
        let mut buf = str_buf::StrBuf::<STREAM_MAX_SIZE>::new();
        let _ = fmt::Write::write_fmt(&mut buf, format_args!("{self}"));

        out.write_arg_fmt(buf.as_str())
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        true
    }
}

impl FromRedisValue for StreamId {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Data(data) => match core::str::from_utf8(data) {
                Ok(data) => match data.parse() {
                    Ok(result) => Ok(result),
                    Err(error) => Err((redis::ErrorKind::InvalidClientConfig, error.as_str()).into()),
                },
                Err(_) => Err((redis::ErrorKind::TypeError, "Not a string").into()),
            },
            Value::Bulk(_) => Err((redis::ErrorKind::TypeError, "Not bulk instead of stream id").into()),
            Value::Nil => Ok(StreamId::nil()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }
}

#[derive(Debug)]
///Group information
pub struct GroupInfo {
    ///Group name
    pub name: String,
    ///Number of consumers in the group
    pub consumers: u64,
    ///Number of messages that are read but yet to be acknowledged.
    pub pending: u64,
    ///ID of last message delivered to group's consumers
    pub last_delivered_id: StreamId,
    //Since redis 7.0
    /////Some sort of read counter for last entry delivered to group's consumers
    //pub entries_read: u64,
    /////Number of entries in the stream that are still waiting to be delivered
    //pub lag: u64,
}

impl GroupInfo {
    const USER_FIELD_NAME: &str = "name";
    const USER_FIELD_CONSUMERS: &str = "consumers";
    const USER_FIELD_PENDING: &str = "pending";
    const USER_FIELD_LAST_DELIVERED_ID: &str = "last-delivered-id";
}

impl FromRedisValue for GroupInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            //Always array
            Value::Bulk(values) => {
                let mut name = None;
                let mut consumers = None;
                let mut pending = None;
                let mut last_delivered_id = None;

                if values.len() < 8 {
                    return Err((
                        redis::ErrorKind::TypeError,
                        "Insufficient number of values returned. Need at least 8",
                    )
                        .into());
                }

                for pair in values.chunks(2) {
                    let key = parse_redis_key(&pair[0])?;
                    let value = &pair[1];

                    assign_field_if!(name = value IF key == Self::USER_FIELD_NAME);
                    assign_field_if!(consumers = value IF key == Self::USER_FIELD_CONSUMERS);
                    assign_field_if!(pending = value IF key == Self::USER_FIELD_PENDING);
                    assign_field_if!(last_delivered_id = value IF key == Self::USER_FIELD_LAST_DELIVERED_ID);
                }

                let name = unwrap_required_field!(name);
                let consumers = unwrap_required_field!(consumers);
                let pending = unwrap_required_field!(pending);
                let last_delivered_id = unwrap_required_field!(last_delivered_id);
                Ok(Self {
                    name,
                    consumers,
                    pending,
                    last_delivered_id,
                })
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a pending field").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }
}

///Consumer information in `PendingStats`
pub struct PendingConsumerStat {
    ///Consumer name
    pub name: String,
    ///Number of pending messages to this user.
    pub no_ack_num: u64,
}

impl FromRedisValue for PendingConsumerStat {
    fn from_redis_value(value: &Value) -> Result<Self, RedisError> {
        match value {
            Value::Bulk(values) => {
                if values.len() == 2 {
                    Ok(Self {
                        name: FromRedisValue::from_redis_value(&values[0])?,
                        no_ack_num: FromRedisValue::from_redis_value(&values[1])?,
                    })
                } else {
                    Err((
                        redis::ErrorKind::TypeError,
                        "PendingConsumerStat array requires 2 elements",
                    )
                        .into())
                }
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a PendingConsumerStat array").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }
}

///Brief output of XPENDING
///
///Provided when no ID is specified in arguments
pub struct PendingStats {
    ///Number of pending messages within group
    ///
    ///If len is 0, rest of fields are using default values.
    pub len: u64,
    ///Smallest ID among pending messages
    pub lowest_id: StreamId,
    ///Highest ID among pending messages
    pub highest_id: StreamId,
    ///Stats on every consumer within group
    pub consumers: Vec<PendingConsumerStat>,
}

impl FromRedisValue for PendingStats {
    fn from_redis_value(value: &Value) -> Result<Self, RedisError> {
        match value {
            Value::Bulk(values) => {
                if values.len() == 4 {
                    Ok(Self {
                        len: FromRedisValue::from_redis_value(&values[0])?,
                        lowest_id: FromRedisValue::from_redis_value(&values[1])?,
                        highest_id: FromRedisValue::from_redis_value(&values[2])?,
                        consumers: FromRedisValue::from_redis_value(&values[3])?,
                    })
                } else {
                    Err((redis::ErrorKind::TypeError, "PendingStats array requires 4 elements").into())
                }
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a PendingStats array").into()),
            //If there are no messages pending, redis returns NIL, so handle it in a more casual
            //manner by just setting length to 0
            Value::Nil => Ok(Self {
                len: 0,
                lowest_id: StreamId::nil(),
                highest_id: StreamId::nil(),
                consumers: Vec::new(),
            }),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
///Entry's content
pub struct EntryValue<T> {
    ///Identifier, hopefully unique
    ///
    ///Internally encoded in LE byte order
    pub id: uuid::Uuid,
    ///User supplied data
    pub payload: T,
}

impl<T> EntryValue<T> {
    const USER_FIELD_ID: &str = "id";
    const USER_FIELD_DATA: &str = "payload";
}

impl<T: FromRedisValue> FromRedisValue for EntryValue<T> {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            //Stream's entry values are always map
            Value::Bulk(values) => {
                let mut id = None;
                let mut payload = None;

                for pair in values.chunks(2) {
                    let key = parse_redis_key(&pair[0])?;
                    let value = &pair[1];

                    if id.is_none() && key.eq_ignore_ascii_case(Self::USER_FIELD_ID) {
                        id = Some(value);
                    }
                    assign_field_if!(payload = value IF key == Self::USER_FIELD_DATA);
                }

                let id = match id {
                    Some(id) => match id {
                        Value::Data(data) => {
                            let data: [u8; 16] = match data.as_slice().try_into() {
                                Ok(data) => data,
                                Err(_) => return Err((redis::ErrorKind::TypeError, "id field is not 16 bytes").into()),
                            };
                            let data = u128::from_le_bytes(data);
                            uuid::Uuid::from_u128(data)
                        }
                        _ => return Err((redis::ErrorKind::TypeError, "id field is not bytes").into()),
                    },
                    None => return Err((redis::ErrorKind::TypeError, "Missing id field").into()),
                };
                let payload = unwrap_required_field!(payload);
                Ok(Self { id, payload })
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a stream values").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }
}

impl<T: ToRedisArgs> ToRedisArgs for EntryValue<T> {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        Self::USER_FIELD_ID.write_redis_args(out);
        out.write_arg(&self.id.as_u128().to_le_bytes());

        Self::USER_FIELD_DATA.write_redis_args(out);
        self.payload.write_redis_args(out);
    }

    #[inline(always)]
    //We serialize map, so never single arg.
    fn is_single_arg(&self) -> bool {
        false
    }
}

#[derive(Copy, Clone)]
///Position within redis stream
pub enum RangeIdx {
    ///Special type, meaning open range.
    Any,
    ///Any message with specified timestamp of id.
    Timestamp(TimestampId),
    ///Concrete message id.
    Id(StreamId),
    ///Excluding variant of `Id`.
    ///
    ///I.e. start from `id` but do not include `id` itself.
    ExcludeId(StreamId),
}

impl fmt::Debug for RangeIdx {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => fmt.write_str("Any"),
            Self::Timestamp(time) => fmt::Debug::fmt(time, fmt),
            Self::Id(id) => fmt::Debug::fmt(id, fmt),
            Self::ExcludeId(id) => {
                fmt.write_str("Exclude(")?;
                fmt::Debug::fmt(id, fmt)?;
                fmt.write_str(")")
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
///Inclusive range within stream
pub struct Range {
    ///Starting position
    pub start: RangeIdx,
    ///Ending position
    pub end: RangeIdx,
}

impl ToRedisArgs for Range {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, out: &mut W) {
        let Self { start, end } = self;

        match start {
            RangeIdx::Any => "-".write_redis_args(out),
            RangeIdx::Timestamp(id) => id.write_redis_args(out),
            RangeIdx::Id(id) => id.write_redis_args(out),
            RangeIdx::ExcludeId(id) => id.next().write_redis_args(out),
        }

        match end {
            RangeIdx::Any => "+".write_redis_args(out),
            RangeIdx::Timestamp(id) => id.write_redis_args(out),
            RangeIdx::Id(id) => id.write_redis_args(out),
            RangeIdx::ExcludeId(id) => id.prev().write_redis_args(out),
        }
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        false
    }
}

///Parameters to fetch pending messages (fetched, but not consumed).
pub struct PendingParams<'a> {
    ///Group name.
    ///
    ///This is used as identifier of group of pending messages.
    ///
    ///When message is successfully read, it is moved inside this group.
    ///
    ///At any point user can `XACK` to confirm message is consumed and delete it.
    ///
    ///Otherwise fetch it again (in case of crash or something similar)
    pub group: &'a str,
    ///Range parameter
    pub range: Range,
    ///IDLE time filter.
    ///
    ///When used, filters out messages whose idle time LESS THAN specified duration.
    pub idle: Option<time::Duration>,
    ///Optional filter by consumer name
    pub consumer: Option<&'a str>,
    ///Number of messages to pick.
    pub count: usize,
}

pub(crate) struct PendingParamsConfig<'a> {
    pub config: &'a QueueConfig,
    pub params: &'a PendingParams<'a>,
}

impl<'a> ToRedisArgs for PendingParamsConfig<'a> {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, out: &mut W) {
        let Self { config, params } = self;

        config.stream.as_ref().write_redis_args(out);
        params.group.write_redis_args(out);

        if let Some(idle) = params.idle {
            let idle: u64 = match idle.as_millis().try_into() {
                Ok(idle) => idle,
                Err(_) => u64::max_value(),
            };

            idents::IDLE.as_bytes().write_redis_args(out);
            idle.write_redis_args(out);
        }

        params.range.write_redis_args(out);
        params.count.write_redis_args(out);

        if let Some(consumer) = &params.consumer {
            consumer.write_redis_args(out)
        }
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        false
    }
}

#[derive(Debug)]
///Information about pending message
pub struct PendingEntry {
    ///Entry's id
    pub id: StreamId,
    ///Consumer name
    pub consumer: String,
    ///Duration since message was last delivered to the consumer.
    pub last_delivered: time::Duration,
    ///Number of times message has been delivered
    pub count: u64,
}

impl FromRedisValue for PendingEntry {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            //Always array
            Value::Bulk(values) => {
                if values.len() == 4 {
                    Ok(Self {
                        id: StreamId::from_redis_value(&values[0])?,
                        consumer: String::from_redis_value(&values[1])?,
                        last_delivered: time::Duration::from_millis(u64::from_redis_value(&values[2])?),
                        count: u64::from_redis_value(&values[3])?,
                    })
                } else {
                    Err((
                        redis::ErrorKind::TypeError,
                        "Invalid number of values in PendingEntry, should be 4",
                    )
                        .into())
                }
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a pending field").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }

    #[inline]
    fn from_byte_vec(_vec: &[u8]) -> Option<Vec<Self>> {
        None
    }
}

///Possible ways to read items from queue
pub enum FetchType {
    ///Requests new messages.
    ///
    ///New means not yet read by anyone.
    New,
    ///Attempts to fetch only pending message.
    ///
    ///If returns empty result, it means all messages were successfully consumed.
    Pending,
    ///Fetch all pending messages after specified id.
    After(StreamId),
}

impl ToRedisArgs for FetchType {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        match self {
            Self::New => out.write_arg(b">"),
            Self::Pending => out.write_arg(b"0"),
            Self::After(id) => id.write_redis_args(out),
        }
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        true
    }
}

///Parameters for fetch request.
pub struct FetchParams<'a> {
    ///Group name.
    ///
    ///This is used as identifier of group of pending messages.
    ///
    ///When message is successfully read, it is moved inside this group.
    ///
    ///At any point user can `XACK` to confirm message is consumed and delete it.
    ///
    ///Otherwise fetch it again (in case of crash or something similar)
    pub group: &'a str,
    ///Consumer name
    ///
    ///Used to identifier client reading messages
    ///If message is successfully read from Queue, then message is moved to pending list, belonging
    ///to this consumer.
    ///
    ///From now on, this message can be accessed only by this consumer.
    pub consumer: &'a str,
    ///Fetch type
    pub typ: FetchType,
    ///Number of messages to fetch maximum.
    ///
    ///If 0, fetches all available
    pub count: usize,
    ///Requests to block for specified duration in milliseconds.
    pub timeout: Option<time::Duration>,
}

pub(crate) struct FetchParamsConfig<'a> {
    pub config: &'a QueueConfig,
    pub params: &'a FetchParams<'a>,
}

impl<'a> ToRedisArgs for FetchParamsConfig<'a> {
    #[inline(always)]
    fn write_redis_args<W: ?Sized + RedisWrite>(&self, out: &mut W) {
        let Self { config, params } = self;

        out.write_arg(idents::GROUP.as_bytes());
        params.group.write_redis_args(out);
        params.consumer.write_redis_args(out);
        if params.count > 0 {
            out.write_arg(idents::COUNT.as_bytes());
            params.count.write_redis_args(out);
        }
        if let Some(timeout) = params.timeout {
            out.write_arg(idents::BLOCK.as_bytes());
            (timeout.as_millis() as u64).write_redis_args(out);
        }

        out.write_arg(idents::STREAMS.as_bytes());
        config.stream.as_ref().write_redis_args(out);

        params.typ.write_redis_args(out);
    }

    #[inline(always)]
    fn is_single_arg(&self) -> bool {
        false
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
///Queue's entry
pub struct Entry<T> {
    ///Entry's id
    pub id: StreamId,
    ///Entry's value
    pub value: EntryValue<T>,
}

impl<T: FromRedisValue> FromRedisValue for Entry<T> {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            //Map of values is always encoded as sequence of items
            Value::Bulk(values) => {
                //Stream is always consist of 2 parts:
                //1. Identifier
                //2. Values
                if values.len() == 2 {
                    Ok(Self {
                        id: StreamId::from_redis_value(&values[0])?,
                        value: EntryValue::<T>::from_redis_value(&values[1])?,
                    })
                } else {
                    Err((
                        redis::ErrorKind::TypeError,
                        "Invalid number of values in entry, should be 2",
                    )
                        .into())
                }
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a stream entry").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }

    fn from_byte_vec(_vec: &[u8]) -> Option<Vec<Self>> {
        None
    }
}

///Result of fetch operation.
pub struct FetchResult<T> {
    ///Name of stream from where message comes
    pub stream: String,
    ///Stream's content
    pub entries: Vec<Entry<T>>,
}

impl<T: FromRedisValue> FromRedisValue for FetchResult<T> {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            //Map of values is always encoded as sequence of items
            Value::Bulk(values) => {
                //Stream is always consist of 2 parts:
                //1. Identifier
                //2. Values
                if values.len() == 2 {
                    Ok(Self {
                        stream: String::from_redis_value(&values[0])?,
                        entries: Vec::<Entry<T>>::from_redis_value(&values[1])?,
                    })
                } else {
                    Err((
                        redis::ErrorKind::TypeError,
                        "Invalid number of values in entry, should be 2",
                    )
                        .into())
                }
            }
            Value::Data(_) => Err((redis::ErrorKind::TypeError, "Not a stream entry").into()),
            Value::Nil => Err((redis::ErrorKind::TypeError, "unexpected null").into()),
            Value::Int(_) => Err((redis::ErrorKind::TypeError, "unexpected Integer").into()),
            Value::Okay => Err((redis::ErrorKind::TypeError, "unexpected OK").into()),
            Value::Status(_) => Err((redis::ErrorKind::TypeError, "unexpected status").into()),
        }
    }

    fn from_byte_vec(_vec: &[u8]) -> Option<Vec<Self>> {
        None
    }
}
