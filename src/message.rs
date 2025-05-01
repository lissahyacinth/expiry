use std::sync::mpsc::Receiver;
use std::time::Duration;
use time::OffsetDateTime;
use crate::{ExpiryError, ExpiryResult, HashableKey, WritableValue};

pub(crate) enum ScheduleMessageReply {
    Failed,
    Succeeded
}

pub(crate) struct ScheduleAt<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) at: OffsetDateTime
}

impl<K,V> ScheduleAt<K, V> where
    K: HashableKey,
    V: WritableValue {
    pub fn new(key: K, value: V, at: OffsetDateTime) -> Self {
        Self {
            key, value, at
        }
    }
}

pub(crate) struct Cancel<K>
where
    K: HashableKey,
{
    pub(crate) key: K,
}

impl<K> Cancel<K> where
    K: HashableKey,
     {
    pub fn new(key: K) -> Self {
        Self {
            key
        }
    }
}

pub(crate) enum ScheduleMessage<K,V>
where
    K: HashableKey,
    V: WritableValue,
{
    ScheduleAt(ScheduleAt<K, V>),
    Shutdown(),
    Cancel(Cancel<K>),
    Purge(),
    Wakeup()
}

pub(crate) fn check_reply(receiver: &Receiver<ScheduleMessageReply>) -> ExpiryResult<()> {
    if let Ok(reply)  = receiver.recv_timeout(Duration::from_millis(100)) {
        match reply {
            ScheduleMessageReply::Failed => {
                Err(ExpiryError::Communication("Scheduling thread failed to process message".to_string()))
            }
            ScheduleMessageReply::Succeeded => {
                Ok(())
            }
        }
    } else {
        Err(ExpiryError::Communication("Did not receive reply from scheduling thread".to_string()))
    }
}