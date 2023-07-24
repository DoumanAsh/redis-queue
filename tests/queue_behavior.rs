use redis_queue::utils::queue_trim_consumed;
use redis_queue::{Queue, QueueConfig};
use redis_queue::types::{EntryValue, FetchParams, FetchType, PendingParams, Range, RangeIdx, TimestampId};

use core::time;

#[tokio::test]
async fn verify_redis_behavior() {
    const STREAM: &str = "verify_redis_behavior";
    const GROUP: &str = "test1";
    const READER: &str = "reader1";

    let config = QueueConfig { stream: STREAM.into() };

    let client = redis::Client::open("redis://127.0.0.1/").expect("to create redis client");
    let conn = client.get_tokio_connection_manager().await.expect("to get connection");
    let queue = Queue::new(config, conn);

    let result = queue.time().await.expect("get time");
    println!("redis current time={}", result.as_millis());

    queue.purge().await.expect("to delete redis key");
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 0);

    {
        let mut conn = queue.connection();
        let _: () = redis::Cmd::new()
            .arg("ZADD")
            .arg(STREAM)
            .arg("1")
            .arg("data")
            .query_async(&mut conn)
            .await
            .expect("Success");
    }

    //If key is used by incompatible type, we always return error.
    let error = queue.create_group(GROUP).await.unwrap_err();
    assert_eq!(error.kind(), redis::ErrorKind::ClientError);
    queue.purge().await.expect("to delete redis key");

    queue.create_group(&GROUP).await.expect("create group");
    //Make sure second time we do not fail
    queue.create_group(&GROUP).await.expect("create group");

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].pending, 0);

    let value1 = EntryValue {
        id: uuid::Uuid::new_v4(),
        payload: "test",
    };
    let id1 = queue.append(&value1).await.expect("append entry");

    let groups = queue.groups_info().await.expect("to get group info");
    assert!(groups[0].last_delivered_id < id1);
    assert_eq!(groups[0].pending, 0);

    let value2 = EntryValue {
        id: uuid::Uuid::new_v4(),
        payload: "test",
    };
    let id2 = queue.append(&value2).await.expect("append entry");

    let groups = queue.groups_info().await.expect("to get group info");
    assert!(groups[0].last_delivered_id < id1);
    assert_eq!(groups[0].pending, 0);

    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 2);

    let mut params = FetchParams {
        group: GROUP,
        consumer: READER,
        count: 0,
        typ: FetchType::New,
        timeout: None,
    };

    let stream = queue.fetch::<String>(&params).await.expect("Fetch new messages");
    assert_eq!(stream.stream, STREAM);

    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    //Make sure trim task doesn't remove not consumed messages
    assert_eq!(len, 2);

    let items = &stream.entries;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].id, id1);
    assert_eq!(items[0].value.id, value1.id);
    assert_eq!(items[0].value.payload, value1.payload);
    assert_eq!(items[1].id, id2);
    assert_eq!(items[1].value.id, value2.id);
    assert_eq!(items[1].value.payload, value2.payload);

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups[0].last_delivered_id, id2);
    assert_eq!(groups[0].pending, 2);

    let stream = queue.fetch::<String>(&params).await.expect("Fetch new messages");
    assert_eq!(stream.stream, STREAM);
    let items = &stream.entries;
    assert_eq!(items.len(), 0);

    params.typ = FetchType::Pending;
    let stream = queue.fetch::<String>(&params).await.expect("Fetch pending messages");
    assert_eq!(stream.stream, STREAM);

    let items = &stream.entries;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].id, id1);
    assert_eq!(items[0].value.id, value1.id);
    assert_eq!(items[0].value.payload, value1.payload);
    assert_eq!(items[1].id, id2);
    assert_eq!(items[1].value.id, value2.id);
    assert_eq!(items[1].value.payload, value2.payload);

    params.typ = FetchType::After(id1);
    let stream = queue.fetch::<String>(&params).await.expect("Fetch pending messages");
    assert_eq!(stream.stream, STREAM);

    let items = &stream.entries;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, id2);
    assert_eq!(items[0].value.id, value2.id);
    assert_eq!(items[0].value.payload, value2.payload);

    queue.consume(&GROUP, &[id2]).await.expect("To consume second message");
    let stream = queue.fetch::<String>(&params).await.expect("Fetch pending messages");
    assert_eq!(stream.stream, STREAM);
    let items = &stream.entries;
    assert_eq!(items.len(), 0);

    //We consumed second id, not first, so we do not trim everything yet
    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 2);

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups[0].last_delivered_id, id2);
    assert_eq!(groups[0].pending, 1);

    queue.consume(&GROUP, &[id1]).await.expect("To consume second message");
    params.typ = FetchType::Pending;

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups[0].last_delivered_id, id2);
    assert_eq!(groups[0].pending, 0);

    let stream = queue.fetch::<String>(&params).await.expect("Fetch pending messages");
    assert_eq!(stream.stream, STREAM);

    let items = &stream.entries;
    assert_eq!(items.len(), 0);

    let size = queue.delete(&[id1, id2]).await.expect("to delete entries");
    assert_eq!(size, 2);
    let len = queue.len().await.expect("get length");
    assert_eq!(len, 0);

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].pending, 0);

    //Prepare pending test
    let id3_delay = time::Duration::from_secs(10);
    let id1 = queue.append(&value1).await.expect("append entry");
    let id2 = queue.append(&value2).await.expect("append entry");
    let id3 = queue.append_delayed(&value2, id3_delay).await.expect("append entry");
    let time = queue.time().await.expect("get current time");

    let mut pending_params = PendingParams {
        group: GROUP,
        consumer: Some(READER),
        idle: None,
        range: Range {
            start: RangeIdx::Any,
            end: RangeIdx::Any,
        },
        count: 10,
    };
    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups.len(), 1);
    assert!(groups[0].last_delivered_id < id1);
    assert_eq!(groups[0].pending, 0);

    let pending = queue.pending(&pending_params).await.expect("get pending");
    //not fetched yet
    assert_eq!(pending.len(), 0);
    let pending_stats = queue
        .pending_stats(&pending_params.group)
        .await
        .expect("get pending stats");
    assert_eq!(pending_stats.len, 0);

    params.typ = FetchType::New;

    let stream = queue.fetch::<String>(&params).await.expect("Fetch new messages");
    assert_eq!(stream.stream, STREAM);
    assert_eq!(stream.entries.len(), 3);
    //Make sure we do not trim yet
    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 3);

    let groups = queue.groups_info().await.expect("to get group info");
    assert_eq!(groups.len(), 1);
    //We fetched all messages, so id3 should be last delivered
    assert_eq!(groups[0].last_delivered_id, id3);
    assert_eq!(groups[0].pending, 3);

    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);
    assert_eq!(pending[2].id, id3);
    assert_eq!(pending[0].consumer, READER);
    assert_eq!(pending[1].consumer, READER);
    assert_eq!(pending[2].consumer, READER);
    assert_eq!(pending[0].count, 1);
    assert_eq!(pending[1].count, 1);
    assert_eq!(pending[2].count, 1);

    let pending_stats = queue
        .pending_stats(&pending_params.group)
        .await
        .expect("get pending stats");
    assert_eq!(pending_stats.len, 3);
    assert_eq!(pending_stats.lowest_id, id1);
    assert_eq!(pending_stats.highest_id, id3);
    assert_eq!(pending_stats.consumers.len(), 1);
    assert_eq!(pending_stats.consumers[0].no_ack_num, 3);

    pending_params.range.end = RangeIdx::Timestamp(TimestampId::new(time + id3_delay));
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 3);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);
    assert_eq!(pending[2].id, id3);

    pending_params.range.end =
        RangeIdx::Timestamp(TimestampId::new(time + id3_delay - time::Duration::from_secs(1)));
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);

    pending_params.range.start = RangeIdx::Id(id1);
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, id1);
    assert_eq!(pending[1].id, id2);

    pending_params.range.start = RangeIdx::ExcludeId(id1);
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id2);

    pending_params.range.end = RangeIdx::Id(id2);
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id2);

    pending_params.range.end = RangeIdx::ExcludeId(id2);
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 0);

    pending_params.range.start = RangeIdx::Id(id1);
    let pending = queue.pending(&pending_params).await.expect("get pending");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id1);

    //Make sure we do not trim yet
    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 3);

    queue.consume(&GROUP, &[id3]).await.expect("consume id3");

    //id3 is > id1 so we do not trim
    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 3);

    //Trim id1
    queue.consume(&GROUP, &[id1]).await.expect("To consume second message");

    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 2);

    queue.consume(&GROUP, &[id2]).await.expect("To consume second message");

    //Now all messages can be trimmed
    queue_trim_consumed(queue.clone(), 10).await;
    let len = queue.len().await.expect("get len");
    assert_eq!(len, 0);
}
