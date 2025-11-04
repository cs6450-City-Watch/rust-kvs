use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

use kvsinterface::TransactionIdentifier;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeStampedEntry {
    pub ts: SystemTime,
    pub key: String,
    pub val: u64,
}

lazy_static! {
    // store is the actual stored values. This is this node's portion of the distributed kvs.
    // TODO: probably unnecessary with versions
    pub static ref STORE: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    // timestamped versions of the store
    pub static ref VERSIONS: Arc<DashMap<SystemTime, DashMap<String, u64>>> = Arc::new(DashMap::new());

    // time stamps of most recent read ops for an element
    pub static ref READ_STAMPS: Arc<DashMap<String, DashSet<(TransactionIdentifier, SystemTime)>>> = Arc::new(DashMap::new());
    // transaction_reads really only used for easier deallocation in read_stamps
    pub static ref TRANSACTION_READS: Arc<DashMap<TransactionIdentifier, DashSet<String>>> = Arc::new(DashMap::new());

    // organizes write-ahead buffers for each transaction
    pub static ref WRITE_AHEADS: Arc<DashMap<TransactionIdentifier, HashMap<String, u64>>> = Arc::new(DashMap::new());

    // maps tx_ids to relevant timestamps. the timestamp for a transaction ensures the transaction only reads from
    // any version at most as old as itself.
    pub static ref TX_TIMESTAMPS: Arc<DashMap<TransactionIdentifier, SystemTime>> = Arc::new(DashMap::new());
    pub static ref LATEST_COMMIT_LOCK: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
}

/// Gives the timestamp of the latest version before given timestamp `ts`.
pub fn latest_before(ts: SystemTime) -> SystemTime {
    VERSIONS
        .iter()
        .filter(|entry| entry.key() <= &ts) // don't check anything after ts
        .fold(SystemTime::UNIX_EPOCH, |a, b| a.max(*b.key())) // converts between map entry and SystemTime with `b.key`
}

/// Reports whether a given timestamp `ts` is the timestamp for the earliest (or oldest) transaction.
pub fn is_oldest(ts: SystemTime) -> bool {
    !TX_TIMESTAMPS.iter().any(|entry| entry.value() < &ts)
}

pub fn mark_read(tx_id: TransactionIdentifier, tx_ts: SystemTime, key: String) {
    READ_STAMPS
        .entry(key.to_owned())
        .or_insert(DashSet::with_capacity(3))
        .insert((tx_id, tx_ts));

    TRANSACTION_READS
        .get_mut(&tx_id)
        .expect("no transaction read buffer allocated for valid transaction ID")
        .insert(key);
}

pub fn deallocate_transaction(tx_id: TransactionIdentifier, tx_ts: SystemTime) {
    // check if we also need to deallocate versions
    // invariant: timestamps are monotonically increasing

    if is_oldest(tx_ts) {
        let prev_version = latest_before(tx_ts);
        let removable_versions: Vec<SystemTime> = VERSIONS
            .iter()
            .map(|entry| *entry.key())
            .filter(|ts| *ts < prev_version)
            .collect();
        for version_ts in removable_versions {
            VERSIONS.remove(&version_ts);
        }
    }

    // deallocate write-ahead buffer
    WRITE_AHEADS.remove(&tx_id);

    // remove read markers, key-side
    for read_key in TRANSACTION_READS
        .get(&tx_id)
        .expect("deallocating reads on nonexistent transaction")
        .iter()
    {
        READ_STAMPS
            .get_mut(&*read_key) // kind of ugly. First need to get the string, then need to borrow it.
            .unwrap_or_else(|| panic!("read_stamps set for given key {} does not exist", *read_key))
            .remove(&(tx_id, tx_ts));
    }

    // remove read markers, tx side
    TRANSACTION_READS.remove(&tx_id);
    // remove entry for transaction timestamp
    TX_TIMESTAMPS.remove(&tx_id);
}
