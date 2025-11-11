//! KVS Coordinator
//!
//! This program implements a coordinator service that orchestrates distributed transactions
//! across multiple KVS servers. It reads transaction specifications from files and
//! coordinates their execution using the two-phase commit protocol.

use clap::Parser;
use kvsinterface::{KvsClient, KvsResult};
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::time::sleep;

mod txn_parser;
use txn_parser::parse_transactions;

/// Command-line arguments for the KVS coordinator
#[derive(Parser)]
struct Flags {
    #[clap(long, short, default_value_t = String::from("node"), help = "Base of hostname on LAN for a server, e.g. the \"node\" in \"node0\"")]
    server_base: String,
    #[clap(
        long,
        short,
        default_value_t = 1,
        help = "Number of servers/participants this coordinator is working with."
    )]
    num_servers: u64,
    #[clap(
        long,
        short,
        default_value_t = 8080,
        help = "Port that this coordinator communicates with participants on."
    )]
    port_no: u16,
    #[clap(
        long,
        short,
        action,
        help = "Ignores other configuration like server_base, num_servers, etc and only works over the loopback"
    )]
    localhost: bool,
    #[clap(
        long,
        short,
        default_value = None,
        help = "Ignores other configuration and just directly connects to one IP address."
    )]
    ip_addr: Option<String>,
    #[arg(help = "Path to file encoding operations.")]
    file_path: String,
}

/// Represents the different operations that can be performed in a KVS transaction
#[derive(Clone, Debug, PartialEq, Eq)]
enum KvsOperation {
    /// Start a new transaction
    Begin,
    /// Read the value for a given key
    Get(String),
    /// Write a value to a given key
    Put(String, u64),
    /// Commit the current transaction
    Commit,
    /// Abort the current transaction
    Abort,
}

impl Hash for KvsOperation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Begin | Self::Commit | Self::Abort => {
                panic!("hash on invalid operation: {self:?}")
            }
            Self::Get(key) | Self::Put(key, _) => key.hash(state),
        }
    }
}

impl KvsOperation {
    /// Executes this operation on the given client with the specified transaction number.
    /// Returns an optional value for Get operations, None for other operations.
    async fn run(&self, client: &KvsClient, tx_no: u64) -> KvsResult<Option<u64>> {
        loop {
            if let Self::Get(key) = self {
                let rpc_res = client.get(context::current(), tx_no, key.to_string()).await;
                match rpc_res {
                    Ok(Ok(key)) => return Ok(Some(key)),
                    Ok(Err(e)) => return Err(e),
                    _ => continue,
                }
            }

            let rpc_res = match self {
                Self::Begin => client.begin(context::current(), tx_no).await,
                Self::Put(key, val) => {
                    client
                        .put(context::current(), tx_no, key.to_string(), *val)
                        .await
                }
                Self::Commit => client.commit(context::current(), tx_no).await,
                Self::Abort => client.abort(context::current(), tx_no).await,
                Self::Get(_) => unreachable!(),
            };
            match rpc_res {
                Ok(Ok(())) => return Ok(None),
                Ok(Err(e)) => return Err(e),
                _ => continue,
            }
        }
    }
}

/// Gets the index of the relevant client for a given operation using consistent hashing.
/// Operations on the same key will always be routed to the same server.
fn get_relevant_client(num_servers: &u64, op: &KvsOperation) -> usize {
    let mut s = DefaultHasher::new();
    op.hash(&mut s);
    let hash = s.finish();
    (hash % num_servers) as usize
}

/// Gets the set of all relevant client indices for a transaction's operations.
/// This determines which servers need to participate in the transaction.
fn get_relevant_clients(num_servers: &u64, ops: &[KvsOperation]) -> HashSet<usize> {
    // KvsClient does not impl Eq, so we need to work around it with indices into clients
    let mut relevant_client_idxs = HashSet::with_capacity(3);

    for op in ops.iter() {
        let mut s = DefaultHasher::new();
        op.hash(&mut s);
        let hash = s.finish();
        let idx = (hash % num_servers) as usize;
        relevant_client_idxs.insert(idx);
    }

    relevant_client_idxs
}

/// Executes a single transaction using the two-phase commit protocol.
///
/// This function coordinates the execution of a transaction across multiple servers:
/// 1. Begins the transaction on all relevant servers
/// 2. Executes operations in sequence
/// 3. Commits or aborts based on operation success
///
/// Returns a vector of values read during the transaction.
async fn run_transaction(
    clients: &[KvsClient],
    num_servers: u64,
    tx_no: u64,
    ops: Vec<KvsOperation>,
) -> KvsResult<Vec<u64>> {
    let mut results = Vec::with_capacity(2);
    let relevant_clients = get_relevant_clients(&num_servers, &ops);
    for client_idx in relevant_clients.clone().iter() {
        KvsOperation::Begin
            .run(&clients[*client_idx], tx_no)
            .await?;
    }
    let mut should_abort = false;
    for op in ops.iter() {
        let client_idx = get_relevant_client(&num_servers, op);
        let res = op.run(&clients[client_idx], tx_no).await;
        println!("res: {res:?}");
        if res.is_err() {
            should_abort = true;
            break;
        } else if let Ok(Some(val)) = res {
            results.push(val);
        }
    }
    let decision = if should_abort {
        println!("aborting");
        KvsOperation::Abort
    } else {
        println!("committing");
        KvsOperation::Commit
    };
    for client_idx in relevant_clients.iter() {
        decision
            .run(&clients[*client_idx], tx_no)
            .await
            .expect("txID validity should be checked by here");
    }

    Ok(results)
}

/// Main entry point for the KVS coordinator.
///
/// Parses command-line arguments, establishes connections to KVS servers,
/// reads transaction specifications from a file, and executes them in sequence.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    let clients = if let Some(addr) = flags.ip_addr {
        println!("connecting to {addr}");
        let mut transport = tarpc::serde_transport::tcp::connect(
            format!("{}:{}", addr, flags.port_no),
            Json::default,
        );
        transport.config_mut().max_frame_length(usize::MAX);
        vec![KvsClient::new(client::Config::default(), transport.await?).spawn()]
    } else if flags.localhost {
        println!("Connecting to localhost");
        let mut transport = tarpc::serde_transport::tcp::connect(
            format!("localhost:{}", flags.port_no),
            Json::default,
        );
        transport.config_mut().max_frame_length(usize::MAX);

        vec![KvsClient::new(client::Config::default(), transport.await?).spawn()]
    } else {
        let mut clients = Vec::with_capacity(3);
        for i in 0..flags.num_servers {
            println!(
                "connecting to: {}{}:{}",
                flags.server_base, i, flags.port_no
            );
            let mut transport = tarpc::serde_transport::tcp::connect(
                format!("{}{}:{}", flags.server_base, i, flags.port_no),
                Json::default,
            );
            transport.config_mut().max_frame_length(usize::MAX);
            let client = KvsClient::new(client::Config::default(), transport.await?).spawn();
            clients.push(client);
        }

        clients
    };

    let transactions = parse_transactions(flags.file_path);
    for (tx_no, transaction) in transactions.iter().enumerate() {
        run_transaction(
            &clients,
            flags.num_servers,
            tx_no.try_into().unwrap(),
            transaction.to_owned(),
        )
        .await?;
    }

    // idk I think the example client does this for a reason though
    // something about letting a span processor finish
    sleep(Duration::from_micros(1)).await;

    Ok(())
}
