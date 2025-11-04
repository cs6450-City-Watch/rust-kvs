//! Storage management module for the KVS server
//!
//! This module manages all persistent and transient data structures used by the KVS server,
//! including versioned data storage, transaction state tracking, read/write timestamps,
//! and garbage collection of old versions.

use dashmap::{DashMap, DashSet};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::sync::Mutex;

use kvsinterface::TransactionIdentifier;

/// Represents a timestamped entry in the KVS for replication purposes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeStampedEntry {
    /// The timestamp when this entry was committed
    pub ts: SystemTime,
    /// The key being stored
    pub key: String,
    /// The value being stored
    pub val: u64,
}

lazy_static! {
    /// The main store for this node's portion of the distributed KVS.
    /// TODO: Probably unnecessary with versioned storage.
    pub static ref STORE: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());

    /// Timestamped versions of the store for snapshot isolation.
    /// Maps commit timestamps to the state of the store at that time.
    pub static ref VERSIONS: Arc<DashMap<SystemTime, DashMap<String, u64>>> = Arc::new(DashMap::new());

    /// Tracks the most recent read timestamps for each key.
    /// Used to detect write-write conflicts in snapshot isolation.
    pub static ref READ_STAMPS: Arc<DashMap<String, DashSet<(TransactionIdentifier, SystemTime)>>> = Arc::new(DashMap::new());

    /// Tracks which keys each transaction has read for easier cleanup.
    pub static ref TRANSACTION_READS: Arc<DashMap<TransactionIdentifier, DashSet<String>>> = Arc::new(DashMap::new());

    /// Write-ahead buffers for each active transaction.
    /// Stores uncommitted writes before they are applied to versioned storage.
    pub static ref WRITE_AHEADS: Arc<DashMap<TransactionIdentifier, HashMap<String, u64>>> = Arc::new(DashMap::new());

    /// Maps transaction IDs to their assigned timestamps.
    /// The timestamp ensures the transaction only reads from versions as old as itself.
    pub static ref TX_TIMESTAMPS: Arc<DashMap<TransactionIdentifier, SystemTime>> = Arc::new(DashMap::new());

    /// Global lock to serialize commit operations for consistency.
    pub static ref LATEST_COMMIT_LOCK: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
}

/// Returns the timestamp of the latest committed version before the given timestamp.
/// This is used to determine which version a transaction should read from.
pub fn latest_before(ts: SystemTime) -> SystemTime {
    VERSIONS
        .iter()
        .filter(|entry| entry.key() <= &ts) // don't check anything after ts
        .fold(SystemTime::UNIX_EPOCH, |a, b| a.max(*b.key())) // converts between map entry and SystemTime with `b.key`
}

/// Checks if the given timestamp belongs to the oldest active transaction.
/// This is used to determine when version garbage collection can occur.
pub fn is_oldest(ts: SystemTime) -> bool {
    !TX_TIMESTAMPS.iter().any(|entry| entry.value() < &ts)
}

/// Marks that a transaction has read a particular key at a given timestamp.
/// This is used for conflict detection and dependency tracking.
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

/// Cleans up all resources associated with a completed transaction.
/// This includes write-ahead buffers, read tracking, and potentially old versions.
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
