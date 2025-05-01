use crate::errors::ExpiryResult;
use crate::timing::{next_expiry, timeout_occurred};
use crate::{ExpiryError, HashableKey, PerformOnSchedule, SchedulableObject, ScheduleOps, WritableValue};
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use time::OffsetDateTime;
use crate::message::{check_reply, Cancel, ScheduleAt, ScheduleMessage, ScheduleMessageReply};

type OrderedSchedulableEvents<K, V> = BinaryHeap<Reverse<SchedulableObject<K, V>>>;

pub struct InnerScheduler<K, V, F>
where
    K: HashableKey,
    V: WritableValue,
    F: Fn(&V, &Box<dyn ScheduleOps<K, V>>)
{
    // TODO: RefCell is more appropriate, but means lifetime rules.
    events: Mutex<OrderedSchedulableEvents<K, V>>,
    cancelled_events: Mutex<Vec<K>>,
    schedule_message_channel: Receiver<ScheduleMessage<K, V>>,
    schedule_message_reply_channel: Sender<ScheduleMessageReply>,
    callback: Box<F>
}

impl<K, V, F> ScheduleOps<K, V> for InnerScheduler<K, V, F>
where
    K: HashableKey,
    V: WritableValue,
    F: Fn(&V, &Box<dyn ScheduleOps<K, V>>)
{
    fn schedule_at(&self, key: K, value: V, at: OffsetDateTime) -> ExpiryResult<()> {
        self.events
            .lock()
            .push(Reverse(SchedulableObject::new(at, key, value)));
        Ok(())
    }

    fn cancel(&self, key: K) -> ExpiryResult<()> {
        let mut lock = self.cancelled_events.lock();
        lock.push(key);
        Ok(())
    }

    fn purge(&self) -> ExpiryResult<()> {
        let mut lock = self.cancelled_events.lock();
        _ = lock.drain(..);
        let mut lock = self.events.lock();
        _ = lock.drain();
        Ok(())
    }
}

impl<K, V, F> InnerScheduler<K, V, F>
where
    K: HashableKey,
    V: WritableValue,
    F: Fn(&V, &Box<dyn ScheduleOps<K, V>>)
{
    fn new(
        events: OrderedSchedulableEvents<K, V>,
        cancelled_events: Vec<K>,
        schedule_message_channel: Receiver<ScheduleMessage<K, V>>,
        schedule_message_reply_channel: Sender<ScheduleMessageReply>,
        callback: Box<F>
    ) -> Self {
        Self {
            events: Mutex::new(events),
            cancelled_events: Mutex::new(cancelled_events),
            schedule_message_channel,
            schedule_message_reply_channel,
            callback
        }
    }
}

pub struct InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    schedule_message_channel: Sender<ScheduleMessage<K, V>>,
    schedule_message_reply_channel: Receiver<ScheduleMessageReply>,
    _processor_thread: JoinHandle<()>,
}

impl<K, V> ScheduleOps<K,V> for InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn schedule_at(&self, key: K, value: V, at: OffsetDateTime) -> ExpiryResult<()> {
        self.schedule_message_channel.send(
            ScheduleMessage::ScheduleAt(ScheduleAt::new(key, value, at))
        ).map_err(|e| ExpiryError::Communication(
            format!("Could not send schedule_at message due to {}", e)))?;
        check_reply(&self.schedule_message_reply_channel)
    }

    fn cancel(&self, key: K) -> ExpiryResult<()> {
        self.schedule_message_channel.send(
            ScheduleMessage::Cancel(Cancel::new(key))
        ).map_err(|e| ExpiryError::Communication(
            format!("Could not send cancel message due to {}", e)))?;
        check_reply(&self.schedule_message_reply_channel)
    }

    fn purge(&self) -> ExpiryResult<()> {
        self.schedule_message_channel.send(
            ScheduleMessage::Purge()
        ).map_err(|e| ExpiryError::Communication(
            format!("Could not send purge message due to {}", e)))?;
        check_reply(&self.schedule_message_reply_channel)
    }
}

impl<K, V, F> InnerScheduler<K, V, F>
where
    K: HashableKey,
    V: WritableValue,
    F: Fn(&V, &Box<dyn ScheduleOps<K, V>>)
{
    fn process_expiry(&mut self) {
        loop {
            let sleep_duration = {
                let lock = self.events.lock();
                next_expiry(lock.peek().map(|e| e.0.expires_at_time))
            };
                if timeout_occurred(self, &self.schedule_message_channel, &self.schedule_message_reply_channel, sleep_duration).unwrap() {
                let mut events_lock = self.events.lock();
                let cancelled_events_lock = self.cancelled_events.lock();
                let can_remove_event = if let Some(Reverse(event)) = events_lock.peek() {
                    if cancelled_events_lock.contains(&event.id) {
                        tracing::debug!(key = ?event.id, "Event was marked as cancelled, ignoring");
                        true
                    } else {
                        tracing::debug!("Looking at event at time {}", event.expires_at_time);
                        if event.expires_at_time <= OffsetDateTime::now_utc() {
                            tracing::debug!(key = ?event.id, "Processing event");
                            if let Err(e) = self.callback(&event.data, &self) {
                                tracing::warn!(key = ?event.id, error = ?e, "Event processing failed, will retry");
                                false
                            } else {
                                true
                            }
                        } else {
                            false
                        }
                    }
                } else {
                    false
                };
                if can_remove_event {
                    let _ = events_lock.pop();
                }
            }
        }
        tracing::warn!("Expiry Processing thread is shutting down");
    }
}

impl<K, V> Drop for InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wakeup the background thread so that it checks the shutdown flag
        let _ = self.schedule_message_channel.send(ScheduleMessage::Wakeup());
    }
}

impl<K, V> InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    pub fn new(callback: Box<dyn Fn(&V, &Box<dyn ScheduleOps<K, V>>) + Send + Sync>) -> Self {
        let (inner_scheduler_function_sender, inner_scheduler_function_receiver) = mpsc::channel();
        let (inner_scheduler_reply_sender, inner_scheduler_reply_receiver) = mpsc::channel();

        let processor_thread = thread::spawn(move || {
            let mut scheduler = InnerScheduler::new(
                BinaryHeap::new(),
                Vec::new(),
                inner_scheduler_function_receiver,
                inner_scheduler_reply_sender,
                callback
            );
            scheduler.process_expiry();
        });

        Self {
            schedule_message_channel: inner_scheduler_function_sender,
            schedule_message_reply_channel: inner_scheduler_reply_receiver,
            _processor_thread: processor_thread,
        }
    }
}
