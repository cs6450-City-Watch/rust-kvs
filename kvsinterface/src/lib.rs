use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::SocketAddr;
use thiserror::Error;

pub type TransactionIdentifier = (SocketAddr, u64);

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum KvsError {
    TransactionExists(TransactionIdentifier),
    TransactionDoesntExist(TransactionIdentifier),
    LockConflict {
        tx_id: TransactionIdentifier,
        key: String,
    },
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

// current idea is that the KVS interface to the coordinator/client should not change
// rather the server itself should manage its own timestamps
#[tarpc::service]
pub trait Kvs {
    async fn begin(tx_no: u64) -> KvsResult<()>;
    async fn get(tx_no: u64, key: String) -> KvsResult<u64>;
    async fn put(tx_no: u64, key: String, val: u64) -> KvsResult<()>;
    async fn commit(tx_no: u64) -> KvsResult<()>;
    async fn abort(tx_no: u64) -> KvsResult<()>;
}
