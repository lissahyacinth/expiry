use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use time::OffsetDateTime;

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

pub(crate) fn timeout_occurred(receiver: &Receiver<()>, duration: time::Duration) -> bool {
    match receiver.recv_timeout(
        std::time::Duration::try_from(duration)
            .expect("Could not convert from time::Duration to std::time::Duration"),
    ) {
        Ok(_) => {
            tracing::debug!("Received wakeup event");
            false
        }
        Err(mpsc::RecvTimeoutError::Timeout) => true,
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // Channel closed
            tracing::error!("Wakeup channel sender disconnected - shutting down");
            false
        }
    }
}
