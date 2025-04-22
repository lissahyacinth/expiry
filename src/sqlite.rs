use crate::errors::ExpiryResult;
use crate::timing::next_expiry;
use crate::{HashableKey, PerformOnSchedule, ScheduleOps, WritableValue};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use time::OffsetDateTime;

pub struct InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    conn: Arc<Mutex<Connection>>,
    wakeup_sender: Sender<()>,
    _phantom_keys: PhantomData<K>,
    _phantom_values: PhantomData<V>,
}

impl<K, V> InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn new(conn: Arc<Mutex<Connection>>, wakeup_sender: Sender<()>) -> Self {
        Self {
            conn,
            wakeup_sender,
            _phantom_keys: Default::default(),
            _phantom_values: Default::default(),
        }
    }
}

impl<K, V> ScheduleOps<K, V> for InnerScheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn schedule_at(&self, key: K, value: V, at: OffsetDateTime) -> ExpiryResult<()> {
        let expires_at = at.unix_timestamp();

        let key_blob = bincode::encode_to_vec(&key, bincode::config::standard())?;
        let value_blob = bincode::encode_to_vec(&value, bincode::config::standard())?;

        // Insert into database
        let conn_lock = self.conn.lock();

        conn_lock.execute(
            "INSERT OR REPLACE INTO scheduled_events (id, data, expires_at) VALUES (?1, ?2, ?3)",
            params![key_blob, value_blob, expires_at],
        ).map_err(|e| crate::errors::ExpiryError::Storage(format!("Failed to insert event into database: {}", e)))?;

        // Check if we need to wake up the processor thread
        let mut stmt = conn_lock
            .prepare("SELECT MIN(expires_at) FROM scheduled_events")
            .map_err(|e| {
                crate::errors::ExpiryError::Storage(format!("Failed to prepare statement: {}", e))
            })?;

        let min_expiry: Option<i64> =
            stmt.query_row([], |row| row.get(0))
                .optional()
                .map_err(|e| {
                    crate::errors::ExpiryError::Storage(format!(
                        "Failed to get minimum expiry: {}",
                        e
                    ))
                })?;

        match min_expiry {
            None => {
                self.wakeup_sender.send(()).map_err(|_| {
                    crate::errors::ExpiryError::Communication(
                        "Failed to send wakeup signal".to_string(),
                    )
                })?;
            }
            Some(min_expiry) => {
                if min_expiry == expires_at {
                    self.wakeup_sender.send(()).map_err(|_| {
                        crate::errors::ExpiryError::Communication(
                            "Failed to send wakeup signal".to_string(),
                        )
                    })?;
                }
            }
        }

        Ok(())
    }

    fn cancel(&self, key: K) -> ExpiryResult<()> {
        let key_blob = bincode::encode_to_vec(&key, bincode::config::standard())?;

        let conn_lock = self.conn.lock();
        conn_lock
            .execute("DELETE FROM scheduled_events WHERE id = ?1", [key_blob])
            .map_err(|e| {
                crate::errors::ExpiryError::Storage(format!(
                    "Failed to delete event from database: {}",
                    e
                ))
            })?;

        Ok(())
    }

    fn purge(&self) -> ExpiryResult<()> {
        let conn_lock = self.conn.lock();
        conn_lock
            .execute("DELETE FROM scheduled_events WHERE 1=1", [])
            .map_err(|e| {
                crate::errors::ExpiryError::Storage(format!(
                    "Failed to delete all events from database: {}",
                    e
                ))
            })?;

        Ok(())
    }
}

pub struct SqliteWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    wakeup_sender: Sender<()>,
    shutdown: Arc<AtomicBool>,
    _processor_thread: JoinHandle<()>,
    scheduler: Arc<InnerScheduler<K, V>>,
}

impl<K, V> ScheduleOps<K,V> for SqliteWaker<K, V>
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

impl<K, V> SqliteWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    /// Create a new SqliteWaker with a specific database path
    pub fn with_path<P: AsRef<Path>>(
        callback: PerformOnSchedule<K, V>,
        db_path: P,
    ) -> ExpiryResult<Self> {
        let conn = Connection::open(db_path).map_err(|e| {
            crate::errors::ExpiryError::Storage(format!("Failed to open SQLite connection: {}", e))
        })?;

        // Initialize the database schema
        Self::init_db(&conn).map_err(|e| {
            crate::errors::ExpiryError::Storage(format!(
                "Failed to initialize database schema: {}",
                e
            ))
        })?;

        let (wakeup_sender, wakeup_receiver) = mpsc::channel();
        let shared_conn = Arc::new(Mutex::new(conn));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_conn = Arc::clone(&shared_conn);
        let thread_shutdown = Arc::clone(&shutdown);

        let inner_scheduler: Arc<InnerScheduler<K, V>> = Arc::new(InnerScheduler::new(
            shared_conn.clone(),
            wakeup_sender.clone(),
        ));

        let thread_scheduler = Arc::clone(&inner_scheduler);
        let processor_thread = thread::spawn(move || {
            Self::process_expiry(
                thread_conn,
                thread_shutdown,
                wakeup_receiver,
                callback,
                thread_scheduler,
            );
        });

        Ok(Self {
            wakeup_sender,
            shutdown,
            _processor_thread: processor_thread,
            scheduler: inner_scheduler,
        })
    }

    fn init_db(conn: &Connection) -> rusqlite::Result<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS scheduled_events (
                id BLOB PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_expires_at ON scheduled_events (expires_at)",
            [],
        )?;

        Ok(())
    }

    // Process expired events
    fn process_expiry(
        conn: Arc<Mutex<Connection>>,
        shutdown: Arc<AtomicBool>,
        wakeup_receiver: Receiver<()>,
        callback: PerformOnSchedule<K, V>,
        scheduler: Arc<InnerScheduler<K, V>>,
    ) {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                tracing::info!("Received shutdown signal, shutting down.");
                break;
            }
            let sleep_duration = {
                let conn_lock = conn.lock();
                let next_expiry_in_db: Option<i64> = conn_lock
                    .prepare(
                        "SELECT expires_at FROM scheduled_events ORDER BY expires_at ASC LIMIT 1",
                    )
                    .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)))
                    .ok();

                next_expiry(next_expiry_in_db.map(|e| {
                    OffsetDateTime::from_unix_timestamp(e).expect("Could not unwrap time")
                }))
            };

            if crate::timing::timeout_occurred(&wakeup_receiver, sleep_duration) {
                let conn_lock = conn.lock();
                let now = OffsetDateTime::now_utc().unix_timestamp();

                // Get expired events
                let mut stmt = match conn_lock.prepare(
                    "SELECT id, data FROM scheduled_events
                     WHERE expires_at <= ?1
                     ORDER BY expires_at ASC LIMIT 1",
                ) {
                    Ok(stmt) => stmt,
                    Err(e) => {
                        tracing::error!("Failed to prepare statement: {}", e);
                        continue;
                    }
                };

                let result = stmt.query_row([now], |row| {
                    let id_blob: Vec<u8> = row.get(0)?;
                    let data_blob: Vec<u8> = row.get(1)?;

                    Ok((id_blob, data_blob))
                });

                if let Ok((id_blob, data_blob)) = result {
                    let id: K =
                        match bincode::decode_from_slice(&id_blob, bincode::config::standard()) {
                            Ok((id, _)) => id,
                            Err(e) => {
                                tracing::error!("Failed to deserialize key: {}", e);
                                continue;
                            }
                        };

                    let data: V =
                        match bincode::decode_from_slice(&data_blob, bincode::config::standard()) {
                            Ok((data, _)) => data,
                            Err(e) => {
                                tracing::error!("Failed to deserialize value: {}", e);
                                continue;
                            }
                        };

                    let completed = if let Err(e) = callback(&data, scheduler.clone()) {
                        tracing::warn!(key = ?id, error = ?e, "Event processing failed, will retry");
                        false
                    } else {
                        true
                    };

                    if completed {
                        // Delete the processed event
                        if let Err(e) = conn_lock
                            .execute("DELETE FROM scheduled_events WHERE id = ?1", [id_blob])
                        {
                            tracing::error!("Failed to delete processed event: {}", e);
                        }
                    }
                }
            }
        }
        tracing::warn!("Expiry Processing thread is shutting down");
    }
}

impl<K, V> Drop for SqliteWaker<K, V>
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

impl<K, V> SqliteWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    pub fn new(callback: PerformOnSchedule<K, V>) -> Self {
        // Use in-memory SQLite by default
        Self::with_path(callback, ":memory:")
            .expect("Failed to create SqliteWaker with in-memory database")
    }
}
