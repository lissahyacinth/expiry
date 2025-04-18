use crate::errors::ExpiryResult;
use crate::timing::{next_expiry, timeout_occurred};
use crate::{HashableKey, PerformOnSchedule, SchedulableObject, Scheduler, WritableValue};
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use time::OffsetDateTime;

type OrderedSchedulableEvents<K, V> = BinaryHeap<Reverse<SchedulableObject<K, V>>>;

pub struct InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    wakeup_sender: Sender<()>,
    shutdown: Arc<AtomicBool>,
    _processor_thread: JoinHandle<()>,
    events: Arc<Mutex<OrderedSchedulableEvents<K, V>>>,
    cancelled_events: Arc<Mutex<Vec<K>>>,
}

impl<K: HashableKey, V: WritableValue> InMemoryWaker<K, V> {
    fn process_expiry(
        events: Arc<Mutex<OrderedSchedulableEvents<K, V>>>,
        cancelled_events: Arc<Mutex<Vec<K>>>,
        shutdown: Arc<AtomicBool>,
        wakeup_receiver: Receiver<()>,
        callback: PerformOnSchedule<V>,
    ) {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let sleep_duration = {
                let lock = events.lock();
                next_expiry(lock.peek().map(|e| e.0.expires_at_time))
            };
            if timeout_occurred(&wakeup_receiver, sleep_duration) {
                let mut events_lock = events.lock();
                let cancelled_events_lock = cancelled_events.lock();
                let can_remove_event = if let Some(Reverse(event)) = events_lock.peek() {
                    if cancelled_events_lock.contains(&event.id) {
                        tracing::debug!(key = ?event.id, "Event was marked as cancelled, ignoring");
                        true
                    } else {
                        tracing::debug!("Looking at event at time {}", event.expires_at_time);
                        if event.expires_at_time <= OffsetDateTime::now_utc() {
                            tracing::debug!(key = ?event.id, "Processing event");
                            if let Err(e) = callback(&event.data) {
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

impl<K: HashableKey, V: WritableValue> Drop for InMemoryWaker<K, V> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wakeup the background thread so that it checks the shutdown flag
        let _ = self.wakeup_sender.send(());
    }
}

impl<K: HashableKey, V: WritableValue> Scheduler<K, V> for InMemoryWaker<K, V> {
    fn new(callback: PerformOnSchedule<V>) -> Self {
        let (wakeup_sender, wakeup_receiver) = mpsc::channel();
        let shared_data = Arc::new(Mutex::new(BinaryHeap::new()));
        let shared_cancellations = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_data = Arc::clone(&shared_data);
        let thread_cancellations = Arc::clone(&shared_cancellations);
        let thread_shutdown = Arc::clone(&shutdown);
        let processor_thread = thread::spawn(move || {
            Self::process_expiry(
                thread_data,
                thread_cancellations,
                thread_shutdown,
                wakeup_receiver,
                callback,
            );
        });
        Self {
            wakeup_sender,
            shutdown,
            _processor_thread: processor_thread,
            events: shared_data,
            cancelled_events: shared_cancellations,
        }
    }

    fn schedule(&mut self, key: K, value: V, after: time::Duration) -> ExpiryResult<()> {
        let scheduled_wakeup_time = OffsetDateTime::now_utc() + after;
        tracing::debug!("Add new event to wake up at {}", scheduled_wakeup_time);
        let maybe_next_wakeup_time = self.events.lock().peek().map(|e| e.0.expires_at_time);
        self.events.lock().push(Reverse(SchedulableObject::new(
            scheduled_wakeup_time,
            key,
            value,
        )));
        match maybe_next_wakeup_time {
            None => {
                // We could be on default sleep time - set a wakeup
                self.wakeup_sender
                    .send(())
                    .expect("Receiver is not available");
            }
            Some(next_planned_wake_up) => {
                if next_planned_wake_up > scheduled_wakeup_time {
                    tracing::debug!(
                        "Sending message to wake early - new scheduled item is earlier than current wakeup time"
                    );
                    self.wakeup_sender
                        .send(())
                        .expect("Receiver is not available");
                }
            }
        }
        Ok(())
    }

    fn cancel(&mut self, key: K) -> ExpiryResult<()> {
        let mut lock = self.cancelled_events.lock();
        lock.push(key);
        Ok(())
    }

    fn purge(&mut self) -> ExpiryResult<()> {
        let mut lock = self.cancelled_events.lock();
        _ = lock.drain(..);
        let mut lock = self.events.lock();
        _ = lock.drain();
        Ok(())
    }
}
