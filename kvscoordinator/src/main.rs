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

fn get_relevant_client(
    clients: &Vec<KvsClient>,
    num_servers: &u64,
    op: &KvsOperation,
) -> KvsClient {
    let mut s = DefaultHasher::new();
    op.hash(&mut s);
    let hash = s.finish();
    let idx = (hash % num_servers) as usize;
    let client = clients[idx].clone();
    client
}

fn get_relevant_clients(
    clients: &Vec<KvsClient>,
    num_servers: &u64,
    ops: &Vec<KvsOperation>,
) -> Vec<KvsClient> {
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
        .iter()
        .map(|idx| clients[idx.to_owned()].clone())
        .collect()
}

async fn run_transaction(
    clients: &Vec<KvsClient>,
    num_servers: u64,
    tx_no: u64,
    ops: Vec<KvsOperation>,
) -> KvsResult<()> {
    let relevant_clients = get_relevant_clients(clients, &num_servers, &ops);
    for client in relevant_clients.clone().iter() {
        KvsOperation::Begin.run(client, tx_no).await?;
    }
    let mut should_abort = false;
    for op in ops.iter() {
        let client = get_relevant_client(clients, &num_servers, op);
        let res = op.run(&client, tx_no).await;
        println!("res: {:?}", res);
        if let Err(_) = res {
            should_abort = true;
            break;
        }
    }
    let decision = if should_abort {
        println!("aborting");
        KvsOperation::Abort
    } else {
        println!("committing");
        KvsOperation::Commit
    };
    for client in relevant_clients.iter() {
        decision
            .run(&client, tx_no)
            .await
            .expect("txID validity should be checked by here");
    }

    Ok(())
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

    for i in 0..100 {
        run_transaction(
            &clients,
            flags.num_servers,
            //i,
            0,
            vec![
                // KvsOperation::Put("Hello".into(), i),
                KvsOperation::Put("The quick brown fox jumped over the lazy gray dog".into(), i),
                KvsOperation::Put("woke grok ruined my life".into(), i + 1),
                KvsOperation::Put("".into(), i + 2),
                KvsOperation::Put("aHR0cHM6Ly90ZW5vci5jb20vdmlldy9zaXhzZXZlbi1zaXgtc2V2ZW4tc2l4LXNldmUtNjctZ2lmLTE0MTQzMzM3NjY5MDMyOTU4MzQ5".into(), i+3),
                KvsOperation::Put("694206741".into(), i+4),
            ],
        )
        .await?;
    }

    // idk I think the example client does this for a reason though
    // something about letting a span processor finish
    sleep(Duration::from_micros(1)).await;

    Ok(())
}
