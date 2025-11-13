//! KVS service implementation
//!
//! This module implements the core KVS service logic with snapshot isolation,
//! distributed timestamp management, and transaction coordination. It provides
//! ACID properties for distributed transactions across multiple clients.

use dashmap::Entry;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tarpc::context::Context;
use tokio::time::sleep;

use kvsinterface::{Kvs, KvsError, KvsResult};

use crate::grpc::now;
use crate::storage::{
    TimeStampedEntry, deallocate_transaction, latest_before, latest_commit_lock, mark_read,
    read_stamps, tx_timestamps, versions, write_aheads,
};

/// Waits until the given timestamp has passed according to SomeTime semantics.
/// From the Spanner paper: waits until `now().earliest > commit_timestamp`.
pub async fn elapse(ts: SystemTime) {
    while now().await.earliest <= ts {
        sleep(Duration::from_millis(100)).await;
    }
}

/// Trait for KVS replication service.
/// This would be used for maintaining consistency across replicas.
/// Note: Not fault-tolerant; primarily for consistency demonstration.
#[tarpc::service]
pub trait KvsReplica {
    /// Appends entries to the replica log for replication purposes.
    async fn append_entries(entries: Vec<TimeStampedEntry>) -> KvsResult<()>;
}

/// Replicator service implementation.
#[derive(Clone)]
pub struct KvsReplicator;

impl KvsReplica for KvsReplicator {
    async fn append_entries(self, _: Context, _entries: Vec<TimeStampedEntry>) -> KvsResult<()> {
        unimplemented!()
    }
}

/// Main KVS server implementation that handles distributed transactions.
/// Each server instance is identified by its socket address.
#[derive(Clone)]
pub struct KvsServer(pub SocketAddr);

impl Kvs for KvsServer {
    /// Allocates all of the local information for this transaction.
    /// The transaction is assigned a timestamp which is the "earliest" (`now().earliest`)
    /// before this transaction.
    ///
    /// ## Relevant allocations
    /// - timestamps entry (timestamp for the transaction)
    /// - write-ahead buffer (intermediate storage prior to commit-time for writes)
    /// - transaction read set (keys read by this transaction- makes it easier to manage dependencies)
    async fn begin(self, _: Context, tx_no: u64) -> KvsResult<()> {
        // acquiring this entry is an implicit lock acquiring, at least for the moment.
        // remember: the ENTRY is being locked.
        let tx_id = (self.0, tx_no);
        match tx_timestamps.entry(tx_id) {
            Entry::Occupied(_) => Err(kvsinterface::KvsError::TransactionExists(tx_id)),
            Entry::Vacant(timestamps_entry) => {
                // allocate resources to track transaction
                timestamps_entry.insert(now().await.earliest); // TODO: do we want earliest or latest here?
                write_aheads.insert(tx_id, HashMap::new());
                crate::storage::transaction_reads.insert(tx_id, dashmap::DashSet::new());

                Ok(())
            }
        }
    }

    /// Algorithm is more or less as follows:
    /// 1. Get the timestamp for the transaction (if it doesn't exist, error)
    /// 2. Get the timestamp for the version to reference on read (if this is invalid, e.g. `UNIX_EPOCH`, error)
    /// 3. from the version, read the value for the key (if key doesn't map to a value, error)
    /// 4. mark the key as having been read and return the read value
    async fn get(self, _: Context, tx_no: u64, key: String) -> KvsResult<Option<u64>> {
        let tx_id = (self.0, tx_no);
        match tx_timestamps.get(&tx_id) {
            None => Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => {
                let relevant_version_ts = latest_before(*ts);
                if SystemTime::UNIX_EPOCH == relevant_version_ts {
                    return Ok(None);
                }

                match versions.get(&relevant_version_ts).unwrap().get(&key) {
                    None => Ok(None),
                    Some(entry) => {
                        mark_read(tx_id, *ts, key);
                        Ok(Some(*entry))
                    }
                }
            }
        }
    }

    /// Algorithm is more or less as follows:
    /// 1. Get the transaction's timestamp (if not found, error)
    /// 2. check the read timestamps.
    ///   - if there exists a read timestamp that is further in the future than this transaction (a write), error
    /// 3. mark value as read (effectively handles WAW dependencies)
    /// 4. insert the written kv pair in the write-ahead buffer
    async fn put(self, _: Context, tx_no: u64, key: String, val: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        // check that we can write at all
        let tx_ts = match tx_timestamps.get(&tx_id) {
            None => return Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(write_ts) => *write_ts,
        };
        match read_stamps.get(&key) {
            None => (),
            Some(stamps) => {
                // if there's a read timestamp further in the future than this transaction, error
                if tx_ts
                    < stamps
                        .iter()
                        .fold(SystemTime::UNIX_EPOCH, |a, b| a.max(b.1))
                {
                    return Err(KvsError::LockConflict { tx_id, key });
                }
            }
        }
        match write_aheads.entry(tx_id) {
            Entry::Vacant(_) => {
                panic!("transaction exists with a valid timestamp but no valid write-ahead buffer")
            }
            Entry::Occupied(mut write_ahead_entry) => {
                mark_read(tx_id, tx_ts, key.to_owned());
                write_ahead_entry.get_mut().insert(key.clone(), val);
                //println!("tx_id: {:?} put {} into {}", tx_id, val, key);
                Ok(())
            }
        }
    }

    /// Algorithm is more or less as follows:
    /// 1. Get the transaction's timestamp (if not found, error)
    /// 2. assign `time = ST.now()`
    /// 3. get the latest version before `time.earliest` and clone it (if it doesn't exist, make a new version)
    /// 4. copy every write from the write-ahead buffer into this cloned version
    /// 5. wait until `time.latest`
    /// 6. insert the version into readable versions (make visible, basically)
    /// 7. deallocate resources for the transaction
    async fn commit(self, _: Context, tx_no: u64) -> KvsResult<()> {
        // invariant of ST timestamps monotonically increasing implies latest version -> version before now().earliest,
        // then copy all writes over, wait until now().latest, and commit
        let tx_id = (self.0, tx_no);
        let ts = match tx_timestamps.get(&tx_id) {
            None => return Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => *ts,
        };
        let lock = Arc::clone(&latest_commit_lock);
        {
            let _guard = lock.lock().await;
            let time = now().await;
            let latest_version_ts = latest_before(time.earliest);
            let this_version = if latest_version_ts == SystemTime::UNIX_EPOCH {
                dashmap::DashMap::new()
            } else {
                versions
                    .get(&latest_version_ts)
                    .expect("latest version points to a nonexistent version")
                    .clone()
            };
            // this looks ugly, but really avoids deadlocking with itself.
            // The alternative is to use a match on `write_aheads.get(&tx_no)`
            // where the `Some` variant also has to perform `write_aheads.remove(&tx_no)`
            //println!("tx_id: {:?} (before) version: {:?}", tx_id, this_version);
            for entry in write_aheads.get(&tx_id).unwrap().iter() {
                this_version.insert(entry.0.clone(), *entry.1);
            }
            //println!("tx_id: {:?} (after) version: {:?}", tx_id, this_version);

            // TODO: the order here seems a little fucky. revisit.
            elapse(time.latest).await;
            versions.insert(time.latest, this_version);
            /*
            println!(
                "tx_id: {:?} releasing lock, timestamp: {:?}",
                tx_id, time.latest
            );
            */
        }
        // TODO: notify replica. do we want to line this up with our sleep?

        deallocate_transaction(tx_id, ts);
        Ok(())
    }

    /// Deallocates resources for an unsuccessful transaction.
    async fn abort(self, _: Context, tx_no: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        let ts = match tx_timestamps.get(&tx_id) {
            None => return Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => *ts,
        };

        deallocate_transaction(tx_id, ts);
        Ok(())
    }
}
