use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::SocketAddr;
use thiserror::Error;

use std::time::SystemTime;

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

#[tarpc::service]
pub trait Kvs {
    async fn begin(tx_no: u64, ts: SystemTime) -> KvsResult<()>;
    // IDEA: only the transaction init operation (`begin`) needs to have an associated timestamp
    // this is to compare all `gets` against
    // once done, our commit operation will assign a timestamp
    // to all of the relevant `put`s.
    async fn get(tx_no: u64, key: String) -> KvsResult<u64>;
    async fn put(tx_no: u64, key: String, val: u64) -> KvsResult<()>;
    async fn commit(tx_no: u64) -> KvsResult<()>;
    async fn abort(tx_no: u64) -> KvsResult<()>;
}

// TimeStampedEntry and KvsReplica could be argued to belong only in kvsserver.
// Right now, I don't care all that much.

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeStampedEntry {
    pub ts: SystemTime,
    pub key: String,
    pub val: u64,
}

// not at all fault-tolerant; still applicable since we're more just trying to get consistency across replicas
// also like the Kvs service, it could be argued that this should be using tonic to not be stuck with rust
#[tarpc::service]
pub trait KvsReplica {
    async fn append_entries(entries: Vec<TimeStampedEntry>) -> KvsResult<()>;
}
