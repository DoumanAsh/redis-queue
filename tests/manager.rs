use redis_queue::{Queue, QueueConfig};
use redis_queue::types::{StreamId, EntryValue};
use redis_queue::manager::{Manager, ManagerConfig, ConsumerKind, RunParams, manage};
use redis_queue::manager::dispatch::{Dispatch, TaskResult, TaskResultKind};

use tokio::sync::{mpsc, oneshot, Mutex};

use std::time;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use core::future::Future;
use core::pin::Pin;

type Entry = redis_queue::types::Entry<String>;

const MAX_CONCURRENT_TASK: usize = 1000;

struct Messages {
    messages: mpsc::UnboundedSender<Entry>,
    rewrite: Mutex<HashMap<StreamId, TaskResultKind>>,
}

struct MockChannel {
    messages: mpsc::UnboundedReceiver<Entry>,
}

impl MockChannel {
    fn assert_all_empty(&mut self) {
        assert!(self.messages.try_recv().is_err());
    }
}

struct MockDispatcher {
    sender: Arc<Messages>,
}

impl MockDispatcher {
    pub fn new() -> (Self, MockChannel) {
        let (msg_send, msg_recv) = mpsc::unbounded_channel();
        let sender = Messages {
            messages: msg_send,
            rewrite: Mutex::new(HashMap::new()),
        };
        let this = Self {
            sender: Arc::new(sender),
        };
        let channel = MockChannel {
            messages: msg_recv,
        };
        (this, channel)
    }
}

impl Dispatch for MockDispatcher {
    type PayloadType = String;
    type Future = Pin<Box<dyn Future<Output = TaskResult<String>> + Send + Sync + 'static>>;

    fn send(&self, entry: Entry) -> Self::Future {
        tracing::debug!("send message(redis_id={}, payload={})", entry.id, entry.value.payload);

        let sender = self.sender.clone();
        let fut = tokio::spawn(async move {
            let kind = match sender.rewrite.lock().await.get(&entry.id) {
                None => TaskResultKind::Success,
                Some(kind) => *kind,
            };
            if let Err(_) = sender.messages.send(entry.clone()) {
                tracing::error!("failed to send message");
            }
            TaskResult {
                data: entry,
                kind
            }
        });

        //Because we do not spawn future on runtime, it will be resolved only once poll period finishes.
        Box::pin(async move { fut.await.expect("No crash") })
    }
}

async fn create_queue(suffix: &str) -> Queue {
    let stream = format!("stream{suffix}").into();
    let config = QueueConfig { stream };
    let client = redis::Client::open("redis://127.0.0.1/").expect("to create redis client");
    let conn = client.get_tokio_connection_manager().await.expect("to get connection");
    Queue::new(config, conn)
}

async fn create_manager(suffix: String, idx: usize) -> (Manager, Queue) {
    let consumer = format!("consumer{suffix}-{idx}");

    tracing::info!("stream=stream{suffix}, consumer={consumer}");

    let config = ManagerConfig {
        group: "test".into(),
        kind: ConsumerKind::determine(consumer.as_str()),
        consumer: consumer.into(),
        poll_time: time::Duration::from_secs(10),
        max_pending_time: time::Duration::from_secs(60),
    };
    //For fixture requests we should have own connection as all messages is serialized
    //so manager's queue cannot be used by fixture too
    let queue = create_queue(&suffix).await;
    let user_queue = create_queue(&suffix).await;

    if idx == 0 {
        let _ = queue.purge().await;
    }

    let manager = Manager::new(queue, config).await.expect("to create manager");

    (manager, user_queue)
}

struct State {
    mock: Arc<MockDispatcher>,
    queue: Queue,
    config: ManagerConfig,
    shutdown: oneshot::Sender<()>,
    channel: MockChannel,
    suffix: String,
}

pub struct Fixture {
    state: State,
    runner: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn with_suffix(suffix: String, idx: usize) -> Self {
        let _ = tracing_subscriber::fmt().with_file(true).with_level(true).with_line_number(true).with_test_writer().try_init();
        let (mock, channel) = MockDispatcher::new();
        let mock = Arc::new(mock);
        let (shutdown, shutdown_recv) = oneshot::channel();
        let (manager, queue) = create_manager(suffix.clone(), idx).await;

        let config = manager.config().clone();

        let params = RunParams {
            manager,
            shutdown_recv,
            max_task_count: MAX_CONCURRENT_TASK,
            dispatcher: mock.clone(),
        };
        let runner = tokio::spawn(manage(params));

        Self {
            state: State {
                mock,
                queue,
                config,
                shutdown,
                channel,
                suffix,
            },
            runner,
        }
    }

    #[track_caller]
    //Shortcut to automatically generate unique suffix.
    //
    //If you want to create mulple managers targetting the same queue, use `with_suffix`
    fn new() -> impl Future<Output = Self> {
        //track_caller is not working in `async fn`
        let location = core::panic::Location::caller();
        Self::with_suffix(format!("{}:{}", location.line(), location.column()), 0)
    }
}

#[tokio::test]
async fn manager_should_schedule_task_now_and_in_future() {
    let Fixture { state, runner } = Fixture::new().await;
    let mut channel = state.channel;

    let task = EntryValue {
        id: uuid::Uuid::nil(),
        payload: "MyTask1 is fancy".to_owned(),
    };
    let task_id = state.queue.append(&task).await.expect("to send task");
    tracing::info!("Schedule(task_id={task_id}) now");

    let result = channel.messages.recv().await.expect("to have message");
    assert_eq!(task_id, result.id);
    assert_eq!(task, result.value);
    channel.assert_all_empty();

    let task = EntryValue {
        id: uuid::Uuid::nil(),
        payload: "MyTask2 is fancy".to_owned(),
    };

    tracing::info!("Schedule(task_id={task_id}) in 20 seconds");
    let delay = time::Duration::from_secs(20);
    let now = time::Instant::now();
    let task_id = state.queue.append_delayed(&task, delay).await.expect("to send task");

    let result = channel.messages.recv().await.expect("to have message");
    assert_eq!(task_id, result.id);
    assert_eq!(task, result.value);
    assert!(now.elapsed() >= delay);
    channel.assert_all_empty();

    state.shutdown.send(()).expect("manager lives");
    runner.await.expect("finish successfully");
}

#[tokio::test]
async fn manager_should_read_big_chunks() {
    let Fixture { state, runner } = Fixture::new().await;
    let mut channel = state.channel;

    let mut first_chunk = Vec::new();
    for idx in 1..=100 {
        let task = EntryValue {
            id: uuid::Uuid::new_v4(),
            payload: format!("MyFirstChunk(id={idx})"),
        };
        state.queue.append(&task).await.expect("to send task");
        first_chunk.push(task);
    }

    let mut second_chunk = Vec::new();
    for idx in 1..=100 {
        let task = EntryValue {
            id: uuid::Uuid::new_v4(),
            payload: format!("MySecondChunk(id={idx})"),
        };
        state.queue.append(&task).await.expect("to send task");
        second_chunk.push(task);
    }

    tracing::info!("Receive first chunk");
    let mut first_received = Vec::new();
    for _ in 1..=100 {
        let task = channel.messages.recv().await.expect("to have message");
        first_received.push(task.value);
    }
    assert_eq!(first_received, first_chunk);

    tracing::info!("Receive second chunk");
    let now = time::Instant::now();
    let mut second_received = Vec::new();
    for _ in 1..=100 {
        let task = channel.messages.recv().await.expect("to have message");
        second_received.push(task.value);
    }

    channel.assert_all_empty();
    assert_eq!(second_received, second_chunk);
    assert_eq!(now.elapsed().as_secs(), 0);
    state.shutdown.send(()).expect("manager lives");
    runner.await.expect("finish successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn manager_should_send_and_receive_concurrent() {
    let Fixture { state, runner } = Fixture::new().await;
    let poll_time = state.config.poll_time;
    let mut channel = state.channel;

    let receiver = tokio::spawn(async move {
        let mut content_map = HashSet::with_capacity(100);
        for _ in 1..=100 {
            //Every message's content is unique, make sure there is no repetition
            let task = channel.messages.recv().await.expect("to have message");
            assert!(content_map.insert(task.value.payload));
        }
        tokio::time::sleep(poll_time).await;
        assert!(channel.messages.try_recv().is_err());
    });

    for idx in 1..=100 {
        let task = EntryValue {
            id: uuid::Uuid::new_v4(),
            payload: format!("SomePayload(id={idx})"),
        };
        state.queue.append(&task).await.expect("to send task");
    }

    receiver.await.expect("Should complete successfully");
    state.shutdown.send(()).expect("manager lives");
    runner.await.expect("finish successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manager_should_receive_task_after_few_temp_fails() {
    let Fixture { state, runner } = Fixture::new().await;
    let mut channel = state.channel;

    let task = EntryValue {
        id: uuid::Uuid::new_v4(),
        payload: format!("SomePayload1"),
    };
    let id = state.queue.append(&task).await.expect("to send task");
    tracing::info!("Mark task(id={id}) for failure");
    //Now going to retry message until it is not `TempFail`
    state.mock.sender.rewrite.lock().await.insert(id, TaskResultKind::TempFail);

    let mut result = channel.messages.recv().await.expect("to have message");
    assert_eq!(result.value, task);

    result = channel.messages.recv().await.expect("to have message");
    assert_eq!(result.value, task);

    tracing::info!("Mark task(id={id}) for success");
    state.mock.sender.rewrite.lock().await.insert(id, TaskResultKind::Success);

    result = channel.messages.recv().await.expect("to have message");
    assert_eq!(result.value, task);

    //Make sure it is deleted after success
    state.shutdown.send(()).expect("manager lives");
    runner.await.expect("finish successfully");
    channel.assert_all_empty();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_not_receive_task_after_detect_failure() {
    let Fixture { state, runner } = Fixture::new().await;
    let poll_time = state.config.poll_time;
    let mut channel = state.channel;

    let task = EntryValue {
        id: uuid::Uuid::new_v4(),
        payload: format!("SomePayload2"),
    };
    let id = state.queue.append(&task).await.expect("to send task");
    tracing::info!("Mark task(id={id}) for failure");
    state.mock.sender.rewrite.lock().await.insert(id, TaskResultKind::Failure);

    let result = channel.messages.recv().await.expect("to have message");
    assert_eq!(result.value, task);
    //Failure will be always removed from queue, so make sure it is so

    tokio::time::sleep(poll_time * 2).await;

    state.mock.sender.rewrite.lock().await.insert(id, TaskResultKind::Success);
    //Make sure it is deleted after success
    state.shutdown.send(()).expect("manager lives");
    runner.await.expect("finish successfully");
    channel.assert_all_empty();
}

//Consumes ~100mb RAM to store all tasks
//Speed will depend on your number of cores
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn should_try_stress_test_single_manager() {
    const PRODUCER_NUMBER: usize = 16;
    const MESSAGE_NUMBER: usize = 102400;

    let Fixture { state, runner } = Fixture::new().await;
    tracing::info!("Send {} messages", PRODUCER_NUMBER * MESSAGE_NUMBER);
    let mut channel = state.channel;
    let shutdown = state.shutdown;
    let config = state.config;
    let queue = state.queue.clone();

    let mut producers = Vec::new();
    for producer in 0..PRODUCER_NUMBER {
        let suffix = state.suffix.clone();
        let job = tokio::spawn(async move {
            let queue = create_queue(&suffix).await;

            for idx in 1..=MESSAGE_NUMBER {
                let task = EntryValue {
                    id: uuid::Uuid::new_v4(),
                    payload: format!("SomePayload(producer({producer}-{idx})"),
                };
                loop {
                    if let Err(error) = queue.append(&task).await {
                        if error.is_timeout() || error.is_io_error() || error.is_connection_refusal() || error.is_connection_dropped() {
                            tokio::time::sleep(time::Duration::from_millis(100)).await
                        }
                    } else {
                        break;
                    }
                }
            }
        });
        producers.push(job);
    }

    let case = tokio::spawn(async move {
        for producer in producers.into_iter() {
            producer.await.expect("to finish successfully");
        }

        tracing::info!("Producers done");
        let mut received_tasks = HashSet::new();
        tokio::time::sleep(config.poll_time * 2).await;
        while let Ok(task) = channel.messages.try_recv() {
            assert!(received_tasks.insert(task.value.payload));
        }
        shutdown.send(()).expect("manager lives");
        assert_eq!(received_tasks.len(), PRODUCER_NUMBER * MESSAGE_NUMBER);
        channel.assert_all_empty();
    });
    runner.await.expect("finish successfully");
    case.await.expect("case success");
    assert_eq!(queue.len().await.expect("get len"), 0);
}
