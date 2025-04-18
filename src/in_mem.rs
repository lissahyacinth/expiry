use std::cmp::Reverse;
use std::collections::BinaryHeap;
use crate::{HashableKey, SchedulableObject, Scheduler, WritableValue};
use crate::errors::ExpiryResult;

struct InMemoryWaker<K, V>
    where K: HashableKey,
    V: WritableValue {
    events: Reverse<BinaryHeap<SchedulableObject<K, V>>>
}

impl<K: HashableKey, V: WritableValue> Scheduler<K, V> for InMemoryWaker<K, V> {
    fn schedule(&mut self, key: K, value: V, after: time::Duration) -> ExpiryResult<()> {
        self.events.0.push(
            SchedulableObject::new(
                time::OffsetDateTime::now_utc() + after,
                key,
                value
            )
        );
        Ok(())
    }

    fn cancel(&mut self, key: K) -> ExpiryResult<()> {
        todo!()
    }
}