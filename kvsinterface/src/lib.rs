//! KVS Interface
//!
//! This library defines the common interface and data structures used across the KVS system.
//! It provides the service trait that servers implement and the error types used throughout
//! the distributed key-value store.

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::SocketAddr;
use thiserror::Error;

/// Unique identifier for a transaction, combining the client address and transaction number.
pub type TransactionIdentifier = (SocketAddr, u64);

/// Errors that can occur during KVS operations.
#[derive(Error, Debug, Serialize, Deserialize)]
pub enum KvsError {
    /// Attempted to begin a transaction that already exists
    TransactionExists(TransactionIdentifier),
    /// Attempted to operate on a transaction that doesn't exist
    TransactionDoesntExist(TransactionIdentifier),
    /// A lock conflict occurred when trying to write to a key
    LockConflict {
        tx_id: TransactionIdentifier,
        key: String,
    },
    /// Attempted to read a key that doesn't exist
    KeyDoesntExist(String),
}

impl Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TransactionExists(tx_id) => {
                write!(f, "Transaction with ID {tx_id:?} already exists")
            }
            Self::TransactionDoesntExist(tx_id) => {
                write!(f, "Transaction with ID {tx_id:?} doesn't exist")
            }
            Self::LockConflict { tx_id, key } => write!(
                f,
                "Transaction with ID {tx_id:?} operating on key {key} hit a conflict"
            ),
            Self::KeyDoesntExist(key) => write!(f, "Key {key} does not exist"),
        }
    }
}

pub type KvsResult<T> = Result<T, KvsError>;

/// The main KVS service trait that defines the interface for key-value store operations.
///
/// This trait defines the RPC interface used by coordinators to communicate with KVS servers.
/// All operations are associated with a transaction number to support distributed transactions.
#[tarpc::service]
pub trait Kvs {
    /// Begins a new transaction with the given transaction number.
    async fn begin(tx_no: u64) -> KvsResult<()>;

    /// Reads the value associated with the given key within the specified transaction.
    async fn get(tx_no: u64, key: String) -> KvsResult<Option<u64>>;

    /// Writes a value to the given key within the specified transaction.
    async fn put(tx_no: u64, key: String, val: u64) -> KvsResult<()>;

    /// Commits the specified transaction, making all writes visible.
    async fn commit(tx_no: u64) -> KvsResult<()>;

    /// Aborts the specified transaction, discarding all uncommitted writes.
    async fn abort(tx_no: u64) -> KvsResult<()>;
}
