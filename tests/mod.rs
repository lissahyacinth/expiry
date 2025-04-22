#[cfg(test)]
mod test {
    #[cfg(feature = "sqlite")]
    use expiry::SqliteWaker;
    use expiry::{ExpiryResult, InMemoryWaker, PerformOnSchedule, ScheduleOps};
    use parking_lot::{Mutex, Once};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread::sleep;
    use std::time::Duration;
    use tracing::metadata::LevelFilter;

    static START: Once = Once::new();

    fn test_event_cancellation<S, F>(create_scheduler: F)
    where
        S: ScheduleOps<usize, usize>,
        F: FnOnce(PerformOnSchedule<usize, usize>) -> S,
    {
        START.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::from_default_env()
                        .add_directive(LevelFilter::DEBUG.into()),
                )
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_target(true)
                .with_span_events(
                    tracing_subscriber::fmt::format::FmtSpan::ENTER
                        | tracing_subscriber::fmt::format::FmtSpan::EXIT,
                )
                .init();
        });
        let processed_events = Arc::new(Mutex::new(Vec::new()));
        let processed_clone = Arc::clone(&processed_events);

        let on_wake = move |value: &usize,
                            _scheduler: Arc<dyn ScheduleOps<usize, usize>>|
              -> ExpiryResult<()> {
            processed_clone.lock().push(*value);
            Ok(())
        };

        let store = create_scheduler(Box::new(on_wake));

        store
            .schedule(1, 10, time::Duration::milliseconds(50))
            .expect("Could not schedule");
        store
            .schedule(2, 20, time::Duration::milliseconds(150))
            .expect("Could not schedule");
        store
            .schedule(3, 30, time::Duration::milliseconds(90))
            .expect("Could not schedule");

        store.cancel(2).expect("Could not cancel");

        sleep(Duration::from_millis(500));

        let processed = processed_events.lock();
        assert!(processed.contains(&10));
        assert!(!processed.contains(&20));
        assert!(processed.contains(&30));
    }

    fn test_scheduler_impl<S, F>(create_scheduler: F)
    where
        S: ScheduleOps<usize, usize>,
        F: FnOnce(PerformOnSchedule<usize, usize>) -> S,
    {
        START.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::from_default_env()
                        .add_directive(LevelFilter::DEBUG.into()),
                )
                .with_thread_ids(true) // Show thread IDs
                .with_thread_names(true) // Show thread names if available
                .with_target(true) // Include the target module
                .with_span_events(
                    tracing_subscriber::fmt::format::FmtSpan::ENTER
                        | tracing_subscriber::fmt::format::FmtSpan::EXIT,
                )
                .init();
        });
        let atomic_counter = Arc::new(AtomicU64::new(0));
        let atomic_arc_clone = Arc::clone(&atomic_counter);

        let on_wake = move |value: &usize, _scheduler: Arc<dyn ScheduleOps<usize, usize>>| -> ExpiryResult<()> {
            atomic_arc_clone.fetch_add(*value as u64, Ordering::Acquire);
            tracing::info!("Processed event with value {}", value);
            Ok(())
        };

        let store = create_scheduler(Box::new(on_wake));

        store
            .schedule(1, 1, time::Duration::seconds(1000))
            .expect("Could not schedule");
        store
            .schedule(2, 2, time::Duration::milliseconds(100))
            .expect("Could not schedule");
        store
            .schedule(3, 3, time::Duration::milliseconds(100))
            .expect("Could not schedule");
        store
            .schedule(4, 4, time::Duration::milliseconds(0))
            .expect("Could not schedule");
        sleep(Duration::from_millis(200));
        // Total of scheduled events is 10, but we're not waiting the full second, so we expect 9.
        assert_eq!(atomic_counter.load(Ordering::Acquire), 9);
    }

    #[test]
    fn test_in_memory_scheduling() {
        test_scheduler_impl(InMemoryWaker::new);
    }

    #[test]
    fn test_in_memory_cancellation() {
        test_event_cancellation(InMemoryWaker::new);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn test_sqlite_scheduling() {
        test_scheduler_impl(SqliteWaker::new);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn test_sqlite_cancellation() {
        test_event_cancellation(SqliteWaker::new);
    }
}
