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
                write!(f, "Transaction with ID {:?} already exists", tx_id)
            }
            Self::TransactionDoesntExist(tx_id) => {
                write!(f, "Transaction with ID {:?} doesn't exist", tx_id)
            }
            Self::LockConflict { tx_id, key } => write!(
                f,
                "Transaction with ID {:?} operating on key {} hit a conflict",
                tx_id, key
            ),
            Self::KeyDoesntExist(key) => write!(f, "Key {} does not exist", key),
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
