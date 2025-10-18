use clap::Parser;
use dashmap::{DashMap, Entry};
use futures::{future, prelude::*};
use lazy_static::lazy_static;
use std::{sync::Arc, time::SystemTime};
use tarpc::{
    context::Context,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

use serde::{Deserialize, Serialize};

use std::net::SocketAddr;
use std::{
    collections::HashMap,
    // time::Duration, // use when relying on truetime
};

pub mod sometime {
    tonic::include_proto!("sometime");
}

use sometime::some_time_client::SomeTimeClient;

use kvsinterface::{Kvs, KvsResult, TransactionIdentifier};

#[derive(Parser)]
struct Flags {
    #[clap(long, short, default_value_t = 8080)]
    port: u16,
    #[clap(long, short, default_value_t = 0)]
    node_id: u16,
    #[clap(long, short, action)]
    localhost: bool,
}

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

#[derive(Clone)]
struct KvsReplicator;

impl KvsReplica for KvsReplicator {
    async fn append_entries(self, _: Context, entries: Vec<TimeStampedEntry>) -> KvsResult<()> {
        todo!()
    }
}

#[derive(Copy, Clone)]
enum LockType {
    Write,
    Read,
}

lazy_static! {
    // store is the actual stored values. This is this node's portion of the distributed kvs.
    static ref store: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    // locks held by each transaction
    static ref lock_sets: Arc<DashMap<TransactionIdentifier, DashMap<String, LockType>>> = Arc::new(DashMap::new());
    // organizes write-ahead buffers for each transaction
    static ref write_aheads: Arc<DashMap<TransactionIdentifier, HashMap<String, u64>>> = Arc::new(DashMap::new());
}

/// Checks for existing locks for a key. Filters out `tx_id`.
fn lock_for_key(tx_id: TransactionIdentifier, key: &String) -> Option<LockType> {
    for lock_set in lock_sets.iter().filter(|lock_set| *lock_set.key() != tx_id) {
        // finding the first lock is sufficient as if a write-lock is active, no other read-locks can be active
        // likewise, if a read-lock is active, the first entry reflecting so will be accurate to all
        if let Some(locktype) = lock_set.get(key) {
            return Some(*locktype);
        }
    }
    None
}

/// Claims a key of type `locktype` for transaction `tx_id` on key `key`.
fn claim_lock(tx_id: TransactionIdentifier, key: String, locktype: LockType) {
    let lock_set = lock_sets
        .get_mut(&tx_id)
        .expect("lock_set lookup on a non-existent transaction");
    lock_set.insert(key, locktype);
}

#[derive(Clone)]
struct KvsServer(SocketAddr);

impl Kvs for KvsServer {
    async fn begin(self, _: Context, tx_no: u64) -> KvsResult<()> {
        unimplemented!(
            "need to accomodate for timestamps (some other metadata associating transaction IDs with timing)"
        );
        // acquiring this entry is an implicit lock acquiring, at least for the moment.
        // remember: the ENTRY is being locked.
        let tx_id = (self.0, tx_no);
        match write_aheads.entry(tx_id) {
            Entry::Occupied(_) => Err(kvsinterface::KvsError::TransactionExists(tx_id)),
            Entry::Vacant(write_ahead_entry) => {
                write_ahead_entry.insert(HashMap::with_capacity(3));
                lock_sets.insert(tx_id, DashMap::with_capacity(3));
                Ok(())
            }
        }
    }
    async fn get(self, _: Context, tx_no: u64, key: String) -> KvsResult<u64> {
        let tx_id = (self.0, tx_no);
        match write_aheads.entry(tx_id) {
            Entry::Vacant(_) => Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id)),
            Entry::Occupied(write_ahead_entry) => {
                // 1. check if we've already moved this into our write_ahead. if so, return the val
                if let Some(v) = write_ahead_entry.get().get(&key) {
                    return Ok(*v);
                }
                // 2. if not, check the highest held lock. If it's a write-lock, abort.
                if let Some(LockType::Write) = lock_for_key(tx_id, &key) {
                    return Err(kvsinterface::KvsError::LockConflict { tx_id, key });
                }
                // 3. if there's no write lock, read the entry and claim the read lock.
                match store.get(&key) {
                    None => Err(kvsinterface::KvsError::KeyDoesntExist(key)),
                    Some(val) => {
                        claim_lock(tx_id, key.clone(), LockType::Read);
                        Ok(*val)
                    }
                }
            }
        }
    }
    async fn put(self, _: Context, tx_no: u64, key: String, val: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        match write_aheads.entry(tx_id) {
            Entry::Vacant(_) => Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id)),
            Entry::Occupied(mut write_ahead_entry) => {
                // 1. check if we've already moved this into our write_ahead
                if let Some(v) = write_ahead_entry.get_mut().get_mut(&key) {
                    *v = val;
                    return Ok(());
                }
                // 2. if not, check if there's a lock.
                // if there isn't, claim a write lock, and write the darn thing to the write_ahead.
                if let Some(_) = lock_for_key(tx_id, &key) {
                    return Err(kvsinterface::KvsError::LockConflict { tx_id, key });
                }
                claim_lock(tx_id, key.clone(), LockType::Write);
                write_ahead_entry.get_mut().insert(key, val);
                Ok(())
            }
        }
    }
    async fn commit(self, _: Context, tx_no: u64) -> KvsResult<()> {
        unimplemented!(
            "need to assign timestamps to every write in this transaction and then share with other replicas, as well as everything with commit-wait"
        );
        let tx_id = (self.0, tx_no);
        if let None = write_aheads.get(&tx_id) {
            return Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id));
        }
        // this looks ugly, but really avoids deadlocking with itself.
        // The alternative is to use a match on `write_aheads.get(&tx_no)`
        // where the `Some` variant also has to perform `write_aheads.remove(&tx_no)`
        for entry in write_aheads.get(&tx_id).unwrap().iter() {
            store.insert(entry.0.clone(), *entry.1);
        }

        write_aheads.remove(&tx_id);
        lock_sets.remove(&tx_id);
        Ok(())
    }
    async fn abort(self, _: Context, tx_no: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        if let Entry::Vacant(_) = write_aheads.entry(tx_id) {
            return Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id));
        }
        // deallocate everything from this transaction

        write_aheads.remove(&tx_id);
        lock_sets.remove(&tx_id);
        Ok(())
    }
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    let mut listener = if flags.localhost {
        let server_addr = ("localhost", flags.port);
        println!("listening on: {:?}", server_addr);
        tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?
    } else {
        let server_addr = (format!("node{}", flags.node_id), flags.port);
        println!("listening on: {:?}", server_addr);
        tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?
    };

    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // ignore failures
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // limit channels to 1 per IP
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = KvsServer(channel.transport().peer_addr().unwrap());
            channel.execute(server.serve()).for_each(spawn)
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
