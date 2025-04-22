mod errors;
mod in_mem;
pub use in_mem::InMemoryWaker;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteWaker;
mod consts;
#[cfg(feature = "sqlite")]
mod sqlite;
mod timing;

pub use crate::errors::ExpiryError;
pub use crate::errors::ExpiryResult;
use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::Arc;
use time::OffsetDateTime;

#[cfg(not(feature = "sqlite"))]
pub trait HashableKey: Hash + Eq + std::fmt::Debug + Send + Sync + 'static {}

#[cfg(feature = "sqlite")]
pub trait HashableKey:
    Hash + Eq + bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync + 'static
{
}

#[cfg(feature = "sqlite")]
impl<T> HashableKey for T where
    T: Hash + Eq + bincode::Encode + bincode::Decode<()> + std::fmt::Debug + Send + Sync + 'static
{
}

#[cfg(not(feature = "sqlite"))]
impl<T> HashableKey for T where T: Hash + Eq + std::fmt::Debug + Send + Sync + 'static {}

#[cfg(feature = "sqlite")]
pub trait WritableValue:
    Sized + Send + bincode::Encode + bincode::Decode<()> + Sync + 'static
{
}

#[cfg(feature = "sqlite")]
impl<T> WritableValue for T where
    T: Sized + Send + bincode::Encode + bincode::Decode<()> + Sync + 'static
{
}

#[cfg(not(feature = "sqlite"))]
impl<T> WritableValue for T where T: Sized + Send + Sync + 'static {}

#[cfg(not(feature = "sqlite"))]
pub trait WritableValue: Sized + Send + Sync + 'static {}

pub type PerformOnSchedule<K, V> =
    Box<dyn Fn(&V, Arc<dyn ScheduleOps<K, V>>) -> ExpiryResult<()> + Send + 'static>;

pub trait ScheduleOps<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    /// Schedule a value to be provided to the callable after a given Duration
    fn schedule(&self, key: K, value: V, after: time::Duration) -> ExpiryResult<()> {
        let now = OffsetDateTime::now_utc();
        self.schedule_at(key, value, now + after)
    }

    /// Schedule a value to be provided to the callable at a given time
    fn schedule_at(&self, key: K, value: V, at: time::OffsetDateTime) -> ExpiryResult<()>;

    /// Cancel a scheduled job
    fn cancel(&self, key: K) -> ExpiryResult<()>;

    /// Remove all scheduled jobs
    fn purge(&self) -> ExpiryResult<()>;
}

struct SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    expires_at_time: time::OffsetDateTime,
    id: K,
    data: V,
}

impl<K, V> SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn new(expires_at_time: time::OffsetDateTime, id: K, data: V) -> Self {
        Self {
            expires_at_time,
            id,
            data,
        }
    }
}

impl<K, V> PartialEq for SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.expires_at_time == other.expires_at_time && self.id == other.id
    }
}

impl<K, V> Eq for SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
}

impl<K, V> PartialOrd for SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> Ord for SchedulableObject<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at_time.cmp(&other.expires_at_time)
    }
}
