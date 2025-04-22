use crate::errors::ExpiryResult;
use crate::timing::{next_expiry, timeout_occurred};
use crate::{HashableKey, PerformOnSchedule, SchedulableObject, ScheduleOps, WritableValue};
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

pub struct InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    events: Arc<Mutex<OrderedSchedulableEvents<K, V>>>,
    cancelled_events: Arc<Mutex<Vec<K>>>,
    wakeup_sender: Sender<()>,
}

impl<K, V> InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn new(
        events: Arc<Mutex<OrderedSchedulableEvents<K, V>>>,
        cancelled_events: Arc<Mutex<Vec<K>>>,
        wakeup_sender: Sender<()>,
    ) -> Self {
        Self {
            events,
            cancelled_events,
            wakeup_sender,
        }
    }
}

impl<K, V> ScheduleOps<K, V> for InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn schedule_at(&self, key: K, value: V, at: OffsetDateTime) -> ExpiryResult<()> {
        tracing::debug!("Add new event to wake up at {}", at);
        let maybe_next_wakeup_time = self.events.lock().peek().map(|e| e.0.expires_at_time);
        self.events
            .lock()
            .push(Reverse(SchedulableObject::new(at, key, value)));
        match maybe_next_wakeup_time {
            None => {
                // We could be on default sleep time - set a wakeup
                self.wakeup_sender
                    .send(())
                    .expect("Receiver is not available");
            }
            Some(next_planned_wake_up) => {
                if next_planned_wake_up > at {
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

pub struct InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    wakeup_sender: Sender<()>,
    shutdown: Arc<AtomicBool>,
    _processor_thread: JoinHandle<()>,
    scheduler: Arc<InnerScheduler<K, V>>,
}

impl<K, V> ScheduleOps<K,V> for InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn schedule_at(&self, key: K, value: V, at: OffsetDateTime) -> ExpiryResult<()> {
        self.scheduler.schedule_at(key, value, at)
    }

    fn cancel(&self, key: K) -> ExpiryResult<()> {
        self.scheduler.cancel(key)
    }

    fn purge(&self) -> ExpiryResult<()> {
        self.scheduler.purge()
    }
}

impl<K, V> InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn process_expiry(
        events: Arc<Mutex<OrderedSchedulableEvents<K, V>>>,
        cancelled_events: Arc<Mutex<Vec<K>>>,
        shutdown: Arc<AtomicBool>,
        wakeup_receiver: Receiver<()>,
        callback: PerformOnSchedule<K, V>,
        scheduler: Arc<InnerScheduler<K, V>>,
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
                            if let Err(e) = callback(&event.data, scheduler.clone()) {
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
        let _ = self.wakeup_sender.send(());
    }
}

impl<K, V> InMemoryWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    pub fn new(callback: PerformOnSchedule<K, V>) -> Self {
        let (wakeup_sender, wakeup_receiver) = mpsc::channel();
        let shared_data = Arc::new(Mutex::new(BinaryHeap::new()));
        let shared_cancellations = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let inner_scheduler = Arc::new(InnerScheduler::new(
            shared_data.clone(),
            shared_cancellations.clone(),
            wakeup_sender.clone(),
        ));

        let thread_events = Arc::clone(&shared_data.clone());
        let thread_cancellations = Arc::clone(&shared_cancellations);
        let thread_shutdown = Arc::clone(&shutdown);
        let thread_scheduler = Arc::clone(&inner_scheduler);

        let processor_thread = thread::spawn(move || {
            Self::process_expiry(
                thread_events,
                thread_cancellations,
                thread_shutdown,
                wakeup_receiver,
                callback,
                thread_scheduler,
            );
        });

        Self {
            wakeup_sender,
            shutdown,
            _processor_thread: processor_thread,
            scheduler: inner_scheduler,
        }
    }
}
