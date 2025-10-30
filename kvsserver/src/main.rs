use clap::Parser;
use futures::{future, prelude::*};
use kvsinterface::Kvs;
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};
use tonic::transport::Server;

mod grpc;
mod kvs;
mod storage;

use grpc::{LocalTimeService, sometime::some_time_server::SomeTimeServer};
use kvs::KvsServer;

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

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    // Start gRPC server
    let grpc_port = flags.grpc_port.unwrap_or(flags.port + 1000);
    let grpc_addr = if flags.localhost {
        format!("127.0.0.1:{grpc_port}")
    } else {
        format!("0.0.0.0:{grpc_port}")
    };

    println!("Starting gRPC server on: {grpc_addr}");
    let grpc_addr = grpc_addr.parse().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(SomeTimeServer::new(LocalTimeService))
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

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
