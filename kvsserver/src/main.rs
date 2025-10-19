use clap::Parser;
use dashmap::{DashMap, Entry};
use futures::{future, prelude::*};
use lazy_static::lazy_static;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tarpc::{
    context::Context,
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

use tokio::time::sleep;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use std::net::SocketAddr;

pub mod sometime {
    tonic::include_proto!("sometime");
}

use sometime::some_time_client::SomeTimeClient;

use kvsinterface::{Kvs, KvsError, KvsResult, TransactionIdentifier};

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

lazy_static! {
    // store is the actual stored values. This is this node's portion of the distributed kvs.
    // TODO: probably unnecessary with versions
    static ref store: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    static ref versions: Arc<DashMap<SystemTime, DashMap<String, u64>>> = Arc::new(DashMap::new());
    // time stamp of most recent read operation for an element
    // TODO: need to accommodate for more than one read timestamp
    static ref read_stamps: Arc<DashMap<String, SystemTime>> = Arc::new(DashMap::new());
    // organizes write-ahead buffers for each transaction
    static ref write_aheads: Arc<DashMap<TransactionIdentifier, HashMap<String, u64>>> = Arc::new(DashMap::new());

    // maps tx_ids to relevant timestamps. the timestamp for a transaction ensures the transaction only reads from
    // any version at most as old as itself.
    static ref tx_timestamps: Arc<DashMap<TransactionIdentifier, SystemTime>> = Arc::new(DashMap::new());
}

#[derive(Copy, Clone)]
struct SomeTimeTS {
    pub earliest: SystemTime,
    pub latest: SystemTime,
}

/// Gives the timestamp of the latest version before given timestamp `ts`.
fn latest_before(ts: SystemTime) -> SystemTime {
    versions
        .iter()
        .filter(|entry| entry.key() <= &ts) // don't check anything after ts
        .fold(SystemTime::UNIX_EPOCH, |a, b| a.max(*b.key())) // converts between map entry and SystemTime with `b.key`
}

/// Reports whether a given timestamp `ts` is the timestamp for the earliest (or oldest) transaction.
fn is_oldest(ts: SystemTime) -> bool {
    tx_timestamps
        .iter()
        .filter(|entry| entry.value() <= &ts)
        .next()
        .is_none()
}

/// TODO: replace this with an actual ST RPC. Should of course reflect our semantics on what earliest and latest are.
fn now() -> SomeTimeTS {
    SomeTimeTS {
        earliest: SystemTime::now(),
        latest: SystemTime::now(),
    }
}

/// waits until `ts` has passed.
async fn elapse(ts: SystemTime) {
    loop {
        if ts.elapsed().is_err() {
            // `elapsed` defined to be an error if the time is in the future
            sleep(Duration::from_millis(100)).await;
        }
    }
}

#[derive(Clone)]
struct KvsServer(SocketAddr);
impl Kvs for KvsServer {
    async fn begin(self, _: Context, tx_no: u64) -> KvsResult<()> {
        // acquiring this entry is an implicit lock acquiring, at least for the moment.
        // remember: the ENTRY is being locked.
        let tx_id = (self.0, tx_no);
        match tx_timestamps.entry(tx_id) {
            Entry::Occupied(_) => Err(kvsinterface::KvsError::TransactionExists(tx_id)),
            Entry::Vacant(timestamps_entry) => {
                timestamps_entry.insert(now().latest); // TODO: do we actually want earliest here?
                write_aheads.insert(tx_id, HashMap::new());

                Ok(())
            }
        }
    }
    async fn get(self, _: Context, tx_no: u64, key: String) -> KvsResult<u64> {
        let tx_id = (self.0, tx_no);
        match tx_timestamps.get(&tx_id) {
            None => Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => {
                let relevant_version_ts = latest_before(*ts);
                if SystemTime::UNIX_EPOCH == relevant_version_ts {
                    return Err(KvsError::KeyDoesntExist(key));
                }

                match versions.get(&relevant_version_ts).unwrap().get(&key) {
                    None => Err(KvsError::KeyDoesntExist(key)),
                    Some(entry) => {
                        read_stamps.insert(key, *ts);
                        Ok(*entry)
                    }
                }
            }
        }
    }
    // XXX: logic no longer relies on write-aheads for reads
    // simple fix, but I didn't want to do it tonight
    // I also don't think it breaks any semantics to do it this way
    async fn put(self, _: Context, tx_no: u64, key: String, val: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        // check that we can write at all
        match tx_timestamps.get(&tx_id) {
            None => return Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(write_ts) => match read_stamps.get(&key) {
                None => (),
                Some(read_ts) => {
                    if *write_ts <= *read_ts {
                        return Err(KvsError::LockConflict { tx_id, key });
                    }
                }
            },
        };
        match write_aheads.entry(tx_id) {
            Entry::Vacant(_) => Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id)),
            Entry::Occupied(mut write_ahead_entry) => {
                write_ahead_entry.get_mut().insert(key, val);
                Ok(())
            }
        }
    }
    // TODO: check if oldest transaction
    // if so, clean up older versions
    async fn commit(self, _: Context, tx_no: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        let ts = match tx_timestamps.get(&tx_id) {
            None => return Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => *ts,
        };
        let latest_version_ts = latest_before(ts);
        let this_version = if latest_version_ts == SystemTime::UNIX_EPOCH {
            DashMap::new()
        } else {
            versions
                .get(&latest_version_ts)
                .expect("latest version points to a nonexistent version")
                .clone()
        };
        // this looks ugly, but really avoids deadlocking with itself.
        // The alternative is to use a match on `write_aheads.get(&tx_no)`
        // where the `Some` variant also has to perform `write_aheads.remove(&tx_no)`
        for entry in write_aheads.get(&tx_id).unwrap().iter() {
            this_version.insert(entry.0.clone(), *entry.1);
        }

        let commit_ts = now().latest;
        elapse(commit_ts).await;
        versions.insert(commit_ts, this_version);
        // TODO: notify replica. do we want to line this up with our sleep?

        write_aheads.remove(&tx_id);
        // lock_sets.remove(&tx_id);
        Ok(())
    }
    // TODO: check if oldest transaction
    // if so, clean up older versions
    async fn abort(self, _: Context, tx_no: u64) -> KvsResult<()> {
        let tx_id = (self.0, tx_no);
        if let Entry::Vacant(_) = write_aheads.entry(tx_id) {
            return Err(kvsinterface::KvsError::TransactionDoesntExist(tx_id));
        }
        // deallocate everything from this transaction

        write_aheads.remove(&tx_id);
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
