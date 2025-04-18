use crate::errors::ExpiryResult;
use crate::timing::next_expiry;
use crate::{HashableKey, PerformOnSchedule, Scheduler, WritableValue};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::thread::JoinHandle;
use time::{Duration, OffsetDateTime};

pub struct SqliteWaker<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    wakeup_sender: Sender<()>,
    shutdown: Arc<AtomicBool>,
    _processor_thread: JoinHandle<()>,
    conn: Arc<Mutex<Connection>>,
    _phantom_keys: PhantomData<K>,
    _phantom_values: PhantomData<V>,
}

impl<K: HashableKey, V: WritableValue> SqliteWaker<K, V> {
    /// Create a new SqliteWaker with a specific database path
    pub fn with_path<P: AsRef<Path>>(
        callback: PerformOnSchedule<V>,
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

        let processor_thread = thread::spawn(move || {
            Self::process_expiry(thread_conn, thread_shutdown, wakeup_receiver, callback);
        });

        Ok(Self {
            wakeup_sender,
            shutdown,
            _processor_thread: processor_thread,
            conn: shared_conn,
            _phantom_keys: Default::default(),
            _phantom_values: Default::default(),
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
        callback: PerformOnSchedule<V>,
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
                    let id: K = match bitcode::decode(&id_blob) {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::error!("Failed to deserialize key: {}", e);
                            continue;
                        }
                    };

                    let data: V = match bitcode::decode(&data_blob) {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::error!("Failed to deserialize value: {}", e);
                            continue;
                        }
                    };

                    let completed = if let Err(e) = callback(&data) {
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

impl<K: HashableKey, V: WritableValue> Drop for SqliteWaker<K, V> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wakeup the background thread so that it checks the shutdown flag
        let _ = self.wakeup_sender.send(());
    }
}

impl<K: HashableKey, V: WritableValue> Scheduler<K, V> for SqliteWaker<K, V> {
    fn new(callback: PerformOnSchedule<V>) -> Self {
        // Use in-memory SQLite by default
        Self::with_path(callback, ":memory:")
            .expect("Failed to create SqliteWaker with in-memory database")
    }

    fn schedule(&mut self, key: K, value: V, after: time::Duration) -> ExpiryResult<()> {
        let scheduled_wakeup_time = time::OffsetDateTime::now_utc() + after;
        let expires_at = scheduled_wakeup_time.unix_timestamp();

        let key_blob = bitcode::encode(&key);
        let value_blob = bitcode::encode(&value);

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

    fn cancel(&mut self, key: K) -> ExpiryResult<()> {
        let key_blob = bitcode::encode(&key);

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

    fn purge(&mut self) -> ExpiryResult<()> {
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
