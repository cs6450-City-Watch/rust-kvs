use clap::Parser;
use kvsinterface::{KvsClient, KvsResult};
use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::time::sleep;

use std::time::SystemTime;

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
    #[clap(long)]
    key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum KvsOperation {
    Begin,
    Get(String),
    Put(String, u64),
    Commit,
    Abort,
}

impl Hash for KvsOperation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Begin | Self::Commit | Self::Abort => {
                panic!("hash on invalid operation: {:?}", self)
            }
            Self::Get(key) | Self::Put(key, _) => key.hash(state),
        }
    }
}

impl KvsOperation {
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

/// gets the index of the relevant client
fn get_relevant_client(num_servers: &u64, op: &KvsOperation) -> usize {
    let mut s = DefaultHasher::new();
    op.hash(&mut s);
    let hash = s.finish();
    let idx = (hash % num_servers) as usize;
    idx
}

fn get_relevant_clients(num_servers: &u64, ops: &Vec<KvsOperation>) -> HashSet<usize> {
    // KvsClient does not impl Eq, so we need to work around it with indices into clients
    let mut relevant_client_idxs = HashSet::with_capacity(3);

    for op in ops.clone().iter() {
        let mut s = DefaultHasher::new();
        op.hash(&mut s);
        let hash = s.finish();
        let idx = (hash % num_servers) as usize;
        relevant_client_idxs.insert(idx);
    }

    relevant_client_idxs
}

async fn run_transaction(
    clients: &Vec<KvsClient>,
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
        println!("res: {:?}", res);
        if let Err(_) = res {
            should_abort = true;
            break;
        }
        else if let Ok(Some(val)) = res {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    let clients = if let Some(addr) = flags.ip_addr {
        println!("connecting to {}", addr);
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

    for j in 1..100 {
        println!("j: {}", j);
        run_transaction(&clients, flags.num_servers, 0, vec![KvsOperation::Put(flags.key.clone(), 0)]).await?;
        for i in 1..100 {
            let reads = run_transaction(
                &clients,
                flags.num_servers,
                i,
                //0,
                vec![
                    // KvsOperation::Put("Hello".into(), i),
                    KvsOperation::Get(flags.key.clone()),
                    KvsOperation::Put(flags.key.clone(), i),
                ],
            )
            .await?;
            assert_eq!(reads[0], i-1);
        }
    }

    // idk I think the example client does this for a reason though
    // something about letting a span processor finish
    sleep(Duration::from_micros(1)).await;

    Ok(())
}
