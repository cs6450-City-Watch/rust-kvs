//! KVS Server
//!
//! This program implements a distributed key-value store server that supports transactions
//! with snapshot isolation. It integrates with the SomeTime service for distributed
//! timestamp coordination and provides ACID guarantees for concurrent transactions.

use std::{fmt::Display, str::FromStr};

use clap::Parser;
use futures::{future, prelude::*};
use kvsinterface::Kvs;
use tarpc::{
    server::{self, Channel, incoming::Incoming},
    tokio_serde::formats::Json,
};

mod grpc;
mod kvs;
mod storage;

use kvs::KvsServer;

#[derive(Clone)]
struct Address<const DEFAULT_PORT: u16>(String, u16);

impl<const DEFAULT_PORT: u16> Display for Address<DEFAULT_PORT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl<const DEFAULT_PORT: u16> FromStr for Address<DEFAULT_PORT> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped) = s.strip_prefix(':') {
            // Port only (e.g., ":8080")
            let port = stripped.parse().map_err(|_| "Invalid port")?;
            Ok(Address(String::new(), port))
        } else if let Some((host, port)) = s.rsplit_once(':') {
            // Host and port (e.g., "localhost:8080")
            let port = port.parse().map_err(|_| "Invalid port")?;
            Ok(Address(host.to_string(), port))
        } else {
            // Host only (e.g., "localhost")
            Ok(Address(s.to_string(), DEFAULT_PORT))
        }
    }
}

/// Command-line arguments for the KVS server
#[derive(Parser)]
struct Flags {
    /// The address for this KVS server to listen on
    ///
    /// Include the hostname/IP and/or port as needed
    #[clap(long, short, default_value_t = Address("localhost".into(), 8080))]
    listen_on: Address<8080>,

    /// The address the SomeTime server is listening on
    ///
    /// Include the hostname/IP and/or port as needed
    #[clap(long, short, default_value_t = Address("localhost".into(), 50051))]
    sometime_host: Address<50051>,
}

/// Helper function to spawn a future on the tokio runtime.
async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

/// Main entry point for the KVS server.
///
/// Establishes connection with SomeTime service, sets up the TCP listener,
/// and handles incoming client connections with transaction support.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let flags = Flags::parse();

    let sometime_addr = format!("http://{}", flags.sometime_host);

    println!("connecting to SomeTime service at: {sometime_addr}");

    let server_addr = (flags.listen_on.0, flags.listen_on.1);
    println!("listening on: {}:{}", server_addr.0, server_addr.1);
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;

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
