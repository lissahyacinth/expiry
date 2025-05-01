use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use time::OffsetDateTime;
use crate::{ExpiryResult, HashableKey, ScheduleOps, WritableValue};
use crate::message::{ScheduleMessage, ScheduleMessageReply};

const DEFAULT_WAIT: usize = 500;

pub(crate) fn next_expiry(maybe_event_wakeup: Option<time::OffsetDateTime>) -> time::Duration {
    if let Some(wake_up_time) = maybe_event_wakeup {
        tracing::debug!(
            "At least one event available - wake-up time set to {}",
            wake_up_time
        );
        let now = OffsetDateTime::now_utc();
        if wake_up_time <= now {
            time::Duration::milliseconds(0)
        } else {
            wake_up_time - now
        }
    } else {
        tracing::debug!("Found no events - sleeping for 500ms");
        time::Duration::milliseconds(DEFAULT_WAIT as i64)
    }
}

pub(crate) fn timeout_occurred<K, V>(
    scheduler: &mut dyn ScheduleOps<K, V>,
    schedule_caller: &Receiver<ScheduleMessage<K, V>>,
    schedule_reply: &Sender<ScheduleMessageReply>,
    duration: time::Duration
) -> ExpiryResult<bool> where
    K: HashableKey,
    V: WritableValue,
{
    match schedule_caller.recv_timeout(
        std::time::Duration::try_from(duration)
            .expect("Could not convert from time::Duration to std::time::Duration"),
    ) {
        Ok(schedule_message) => {
            match schedule_message {
                ScheduleMessage::ScheduleAt(schedule_event) => {
                    match scheduler.schedule_at(schedule_event.key, schedule_event.value, schedule_event.at) {
                        Ok(_) => {
                            let _ = schedule_reply.send(ScheduleMessageReply::Succeeded);
                        }
                        Err(_) => {
                            let _ = schedule_reply.send(ScheduleMessageReply::Failed);
                        }
                    };
                    Ok(false)
                }
                ScheduleMessage::Cancel(cancel_event) => {
                    match scheduler.cancel(cancel_event.key) {
                        Ok(_) => {
                        let _ = schedule_reply.send(ScheduleMessageReply::Succeeded);
                    }
                    Err(_) => {
                        let _ = schedule_reply.send(ScheduleMessageReply::Failed);
                    }
                };
                    Ok(false)
                }
                ScheduleMessage::Purge() => {
                    match scheduler.purge() {
                        Ok(_) => {
                        let _ = schedule_reply.send(ScheduleMessageReply::Succeeded);
                    }
                    Err(_) => {
                        let _ = schedule_reply.send(ScheduleMessageReply::Failed);
                    }
                };
                    Ok(false)
                }
                ScheduleMessage::Wakeup() => {
                    tracing::debug!("Received wakeup event");
                    Ok(false)
                }
            }

        }
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(true),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // Channel closed
            tracing::error!("Wakeup channel sender disconnected - shutting down");
            Ok(false)
        }
    }
}
