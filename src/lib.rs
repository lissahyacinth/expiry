mod errors;
mod in_mem;
pub use in_mem::InMemoryWaker;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteWaker;
mod consts;
#[cfg(feature = "sqlite")]
mod sqlite;
mod timing;

pub use crate::errors::ExpiryResult;
use std::cmp::Ordering;
use std::hash::Hash;

#[cfg(not(feature = "sqlite"))]
pub trait HashableKey: Hash + Eq + std::fmt::Debug + Send + Sync + 'static {}

#[cfg(feature = "sqlite")]
pub trait HashableKey:
    Hash + Eq + bitcode::Encode + for<'a> bitcode::Decode<'a> + std::fmt::Debug + Send + Sync + 'static
{
}

#[cfg(feature = "sqlite")]
impl<T> HashableKey for T where
    T: Hash
        + Eq
        + bitcode::Encode
        + for<'a> bitcode::Decode<'a>
        + std::fmt::Debug
        + Send
        + Sync
        + 'static
{
}

#[cfg(not(feature = "sqlite"))]
impl<T> HashableKey for T where T: Hash + Eq + std::fmt::Debug + Send + Sync + 'static {}

#[cfg(feature = "sqlite")]
pub trait WritableValue:
    Sized + Send + bitcode::Encode + for<'a> bitcode::Decode<'a> + Sync + 'static
{
}

#[cfg(feature = "sqlite")]
impl<T> WritableValue for T where
    T: Sized + Send + bitcode::Encode + for<'a> bitcode::Decode<'a> + Sync + 'static
{
}

#[cfg(not(feature = "sqlite"))]
impl<T> WritableValue for T where T: Sized + Send + Sync + 'static {}

#[cfg(not(feature = "sqlite"))]
pub trait WritableValue: Sized + Send + Sync + 'static {}

pub type PerformOnSchedule<V> = Box<dyn Fn(&V) -> ExpiryResult<()> + Send + 'static>;

pub trait Scheduler<K, V>
where
    K: HashableKey,
    V: WritableValue,
{
    fn new(on_wake: PerformOnSchedule<V>) -> Self;

    /// Schedule a value to be provided to the callable after a given Duration
    fn schedule(&mut self, key: K, value: V, after: time::Duration) -> ExpiryResult<()>;

    /// Cancel a scheduled job
    fn cancel(&mut self, key: K) -> ExpiryResult<()>;

    /// Remove all scheduled jobs
    fn purge(&mut self) -> ExpiryResult<()>;
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

impl<K: HashableKey, V: WritableValue> SchedulableObject<K, V> {
    fn new(expires_at_time: time::OffsetDateTime, id: K, data: V) -> Self {
        Self {
            expires_at_time,
            id,
            data,
        }
    }
}

impl<K: HashableKey, V: WritableValue> PartialEq for SchedulableObject<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at_time == other.expires_at_time && self.id == other.id
    }
}

impl<K: HashableKey, V: WritableValue> Eq for SchedulableObject<K, V> {}

impl<K: HashableKey, V: WritableValue> PartialOrd for SchedulableObject<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: HashableKey, V: WritableValue> Ord for SchedulableObject<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at_time.cmp(&other.expires_at_time)
    }
}
