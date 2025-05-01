use std::sync::mpsc::SendError;
use thiserror::Error;

pub type ExpiryResult<T> = Result<T, ExpiryError>;

#[derive(Error, Debug)]
pub enum ExpiryError {
    #[error("Generic: {0}")]
    Generic(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Error occurred during storage operations (disk, database, etc.)
    #[error("Storage error: {0}")]
    Storage(String),

    /// Error occurred during communication between threads
    #[error("Communication error: {0}")]
    Communication(String),

    #[cfg(feature = "sqlite")]
    #[error("Could not encode value")]
    EncodeError(#[from] bincode::error::EncodeError),
}

