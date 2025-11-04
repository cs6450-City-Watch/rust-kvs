use std::time::SystemTime;

use clap::Parser;
use futures::{future, prelude::*};
use kvsinterface::Kvs;
use prost_types::Timestamp;
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

mod grpc;
mod kvs;
mod storage;

use grpc::SomeTimeTS;
use grpc::sometime::some_time_client::SomeTimeClient;
use kvs::KvsServer;

#[derive(Parser)]
struct Flags {
    #[clap(long, short, default_value_t = 8080)]
    port: u16,
    #[clap(long, short, default_value_t = 0)]
    node_id: u16,
    #[clap(long, short, action)]
    localhost: bool,
    #[clap(long, default_value_t = 50051)]
    sometime_port: u16,
    #[clap(long, default_value_t = 0)] // TODO change default value
    sometime_node_id: u16,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

// TODO: delete, fix now()
async fn quick_test(mut client: SomeTimeClient<tonic::transport::Channel>) -> anyhow::Result<()> {
    let request = tonic::Request::new(Timestamp::default());
    let response = client.now(request).await?;
    let response = response.into_inner();

    let g_earliest: Timestamp = response.earliest.unwrap();
    let g_latest: Timestamp = response.latest.unwrap();

    let s_earliest: SystemTime = g_earliest
        .try_into()
        .expect("Failed to convert Timestamp to SystemTime");
    let s_latest: SystemTime = g_latest
        .try_into()
        .expect("Failed to convert Timestamp to SystemTime");

    let timestamp = SomeTimeTS {
        earliest: s_earliest,
        latest: s_latest,
    };

    println!("\nTimestamp requested");
    println!("Earliest: {:?}", timestamp.earliest);
    println!("Latest:   {:?}\n", timestamp.latest);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    // Construct SomeTime service address
    let sometime_addr = if flags.localhost {
        "http://localhost:50051".to_string()
    } else {
        format!(
            "http://node{}:{}",
            flags.sometime_node_id, flags.sometime_port
        )
    };

    println!("connecting to SomeTime service at: {sometime_addr}");
    let client = SomeTimeClient::connect(sometime_addr).await?;

    quick_test(client).await?;
    let mut listener = if flags.localhost {
        let server_addr = ("localhost", flags.port);
        println!("listening on: {server_addr:?}");
        tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?
    } else {
        let server_addr = (format!("node{}", flags.node_id), flags.port);
        println!("listening on: {server_addr:?}");
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
