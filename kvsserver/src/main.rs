use clap::Parser;
use dashmap::{DashMap, DashSet, Entry};
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

use tokio::sync::Mutex;
use tokio::time::sleep;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use std::net::SocketAddr;

pub mod sometime {
    tonic::include_proto!("sometime");
}

use prost_types::Timestamp;
use sometime::Interval;
use sometime::some_time_client::SomeTimeClient;
use sometime::some_time_server::{SomeTime, SomeTimeServer};
use tonic::{Request, Response, Status, transport::Server};

use kvsinterface::{Kvs, KvsError, KvsResult, TransactionIdentifier};

#[derive(Parser)]
struct Flags {
    #[clap(long, short, default_value_t = 8080)]
    port: u16,
    #[clap(long, short, default_value_t = 0)]
    node_id: u16,
    #[clap(long, short, action)]
    localhost: bool,
    #[clap(long, short)]
    grpc_port: Option<u16>,
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

#[derive(Default)]
pub struct LocalTimeService;

impl KvsReplica for KvsReplicator {
    async fn append_entries(self, _: Context, entries: Vec<TimeStampedEntry>) -> KvsResult<()> {
        unimplemented!()
    }
}

#[tonic::async_trait]
impl SomeTime for LocalTimeService {
    async fn now(&self, _request: Request<Timestamp>) -> Result<Response<Interval>, Status> {
        let current_time = now();

        let earliest = Some(Timestamp::from(current_time.earliest));
        let latest = Some(Timestamp::from(current_time.latest));

        let interval = Interval { earliest, latest };
        Ok(Response::new(interval))
    }
}

lazy_static! {
    // store is the actual stored values. This is this node's portion of the distributed kvs.
    // TODO: probably unnecessary with versions
    static ref store: Arc<DashMap<String, u64>> = Arc::new(DashMap::new());
    // timestamped versions of the store
    static ref versions: Arc<DashMap<SystemTime, DashMap<String, u64>>> = Arc::new(DashMap::new());

    // time stamps of most recent read ops for an element
    static ref read_stamps: Arc<DashMap<String, DashSet<(TransactionIdentifier, SystemTime)>>> = Arc::new(DashMap::new());
    // transaction_reads really only used for easier deallocation in read_stamps
    static ref transaction_reads: Arc<DashMap<TransactionIdentifier, DashSet<String>>> = Arc::new(DashMap::new());

    // organizes write-ahead buffers for each transaction
    static ref write_aheads: Arc<DashMap<TransactionIdentifier, HashMap<String, u64>>> = Arc::new(DashMap::new());

    // maps tx_ids to relevant timestamps. the timestamp for a transaction ensures the transaction only reads from
    // any version at most as old as itself.
    static ref tx_timestamps: Arc<DashMap<TransactionIdentifier, SystemTime>> = Arc::new(DashMap::new());
    static ref latest_commit_lock: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
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
        .filter(|entry| entry.value() < &ts)
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
/// From the spanner paper: waits until `now().earliest > commit_timestamp`
async fn elapse(ts: SystemTime) {
    while !(now().earliest > ts) {
        sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone)]
struct KvsServer(SocketAddr);
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
                timestamps_entry.insert(now().earliest); // TODO: do we want earliest or latest here?
                write_aheads.insert(tx_id, HashMap::new());
                transaction_reads.insert(tx_id, DashSet::new());

                Ok(())
            }
        }
    }
    /// Algorithm is more or less as follows:
    /// 1. Get the timestamp for the transaction (if it doesn't exist, error)
    /// 2. Get the timestamp for the version to reference on read (if this is invalid, e.g. `UNIX_EPOCH`, error)
    /// 3. from the version, read the value for the key (if key doesn't map to a value, error)
    /// 4. mark the key as having been read and return the read value
    async fn get(self, _: Context, tx_no: u64, key: String) -> KvsResult<u64> {
        let tx_id = (self.0, tx_no);
        match tx_timestamps.get(&tx_id) {
            None => Err(KvsError::TransactionDoesntExist(tx_id)),
            Some(ts) => {
                let relevant_version_ts = latest_before(*ts);
                /*
                println!(
                    "tx_id: {:?} GET REQUEST own ts: {:?} rvts: {:?}",
                    tx_id, *ts, relevant_version_ts
                );
                println!("tx_id: {:?} versions: {:?}", tx_id, versions.len());
                */
                if SystemTime::UNIX_EPOCH == relevant_version_ts {
                    return Err(KvsError::KeyDoesntExist(key));
                }

                match versions.get(&relevant_version_ts).unwrap().get(&key) {
                    None => Err(KvsError::KeyDoesntExist(key)),
                    Some(entry) => {
                        mark_read(tx_id, *ts, key);
                        Ok(*entry)
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
            let time = now();
            let latest_version_ts = latest_before(time.earliest);
            /*
            println!(
                "tx_id: {:?} got lock, time.earliest = {:?}, latest_version_ts = {:?}",
                tx_id, time.earliest, latest_version_ts
            );
            */
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

fn deallocate_transaction(tx_id: TransactionIdentifier, tx_ts: SystemTime) {
    // check if we also need to deallocate versions
    // invariant: timestamps are monotonically increasing

    if is_oldest(tx_ts) {
        let prev_version = latest_before(tx_ts);
        let removable_versions: Vec<SystemTime> = versions
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|ts| *ts < prev_version)
            .collect();
        for version_ts in removable_versions {
            versions.remove(&version_ts);
        }
    }

    // deallocate write-ahead buffer
    write_aheads.remove(&tx_id);

    // remove read markers, key-side
    for read_key in transaction_reads
        .get(&tx_id)
        .expect("deallocating reads on nonexistent transaction")
        .iter()
    {
        read_stamps
            .get_mut(&*read_key) // kind of ugly. First need to get the string, then need to borrow it.
            .expect(&format!(
                "read_stamps set for given key {} does not exist",
                *read_key
            ))
            .remove(&(tx_id, tx_ts));
    }

    // remove read markers, tx side
    transaction_reads.remove(&tx_id);
    // remove entry for transaction timestamp
    tx_timestamps.remove(&tx_id);
}

fn mark_read(tx_id: TransactionIdentifier, tx_ts: SystemTime, key: String) {
    // XXX: currently unsure of why dashmaps can be immutable on insert
    match read_stamps.entry(key.to_owned()) {
        Entry::Vacant(entry) => {
            let ds = DashSet::with_capacity(3);
            ds.insert((tx_id, tx_ts));
            entry.insert(ds);
        }
        Entry::Occupied(entry) => {
            entry.get().insert((tx_id, tx_ts));
        }
    }
    transaction_reads
        .get_mut(&tx_id)
        .expect("no transaction read buffer allocated for valid transaction ID")
        .insert(key);
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    // Start gRPC server
    let grpc_port = flags.grpc_port.unwrap_or(flags.port + 1000);
    let grpc_addr = if flags.localhost {
        format!("127.0.0.1:{}", grpc_port)
    } else {
        format!("0.0.0.0:{}", grpc_port)
    };

    println!("Starting gRPC server on: {}", grpc_addr);
    let grpc_addr = grpc_addr.parse().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(SomeTimeServer::new(LocalTimeService::default()))
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

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
