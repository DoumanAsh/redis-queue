//! Queue task dispatcher

use redis::FromRedisValue;

use crate::types::Entry;

use core::future::Future;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
///Possible results of task processing
pub enum TaskResultKind {
    ///Task is not recognized or cannot be parsed
    Invalid,
    ///Temporary failure prevents task execution
    ///
    ///Should re-try
    TempFail,
    ///Task failed in a way that makes re-try impossible
    Failure,
    ///All good
    Success,
}

///Result of task dispatch
///
///Type parameter `T` is payload within redis queue `Entry`
pub struct TaskResult<T> {
    ///Task message entry.
    ///
    ///Returned as it was passed
    pub data: Entry<T>,
    ///Operation result
    pub kind: TaskResultKind,
}

impl TaskResultKind {
    ///Returns whether re-try is necessary
    pub const fn is_need_retry(&self) -> bool {
        match self {
            Self::TempFail => true,
            _ => false,
        }
    }
}

///Interface to dispatch raw message with task information
pub trait Dispatch {
    ///Payload type to use.
    type PayloadType: FromRedisValue;
    ///Future that performs send.
    type Future: Future<Output = TaskResult<Self::PayloadType>>;

    ///Starts send.
    fn send(&self, data: Entry<Self::PayloadType>) -> Self::Future;
}

impl<T: Dispatch> Dispatch for std::sync::Arc<T> {
    type PayloadType = T::PayloadType;
    type Future = T::Future;

    #[inline(always)]
    fn send(&self, data: Entry<Self::PayloadType>) -> Self::Future {
        T::send(self, data)
    }
}

impl<T: Dispatch> Dispatch for std::rc::Rc<T> {
    type PayloadType = T::PayloadType;
    type Future = T::Future;

    #[inline(always)]
    fn send(&self, data: Entry<Self::PayloadType>) -> Self::Future {
        T::send(self, data)
    }
}

impl<T: Dispatch> Dispatch for Box<T> {
    type PayloadType = T::PayloadType;
    type Future = T::Future;

    #[inline(always)]
    fn send(&self, data: Entry<Self::PayloadType>) -> Self::Future {
        T::send(self, data)
    }
}
