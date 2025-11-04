# Rusty KVS with SomeTime

A distributed, transactional key-value store implementation in Rust that leverages the SomeTime timestamp service for consistent distributed transactions.
The SomeTime API works regardless of asynch/synch deployment of SomeTime.
In general, it works like how TrueTime works with Spanner.

This KVS is sharded and replicated across multiple machines.
Using the CloudLab facilities across the US, we've also been able to confirm that this works with minimal overhead regarding typical operations.

## Architecture

The system consists of three main components:

1. **KVS Interface (`kvsinterface`)**:
  Common library defining the RPC interface and data structures used across the system.

2. **KVS Server (`kvsserver`)**:
  The distributed storage nodes that handle data storage and transaction processing.
  Integrates with SomeTime for distributed timestamp coordination for consistency.

3. **KVS Coordinator (`kvscoordinator`)**
  Transaction coordinator that orchestrates distributed transactions.

## Usage

### Starting KVS Servers

Start multiple server instances across your cluster:

```bash
# Node 0
[kvsserver]$ cargo run -- --node-id 0 --port 8080

# Node 1  
[kvsserver]$ cargo run -- --node-id 1 --port 8080

# Node 2
[kvsserver]$ cargo run -- --node-id 2 --port 8080
```

For local testing:
```bash
[kvsserver]$ cargo run --bin kvsserver -- --localhost --port 8080
```

### Running Transactions

Create a transaction file (`example.txns`):
```
begin
put(user:123, 42)
put(balance:123, 1000)
get(user:123)
end

begin
get(balance:123)
put(balance:123, 1500)
end
```

Execute transactions:
```bash
# Distributed cluster
[kvscoordinator]$ cargo run -- --server-base node --num-servers 3 example.txns

# Local testing
[kvscoordinator]$ cargo run -- --localhost example.txns

# Single server
[kvscoordinator]$ cargo run -- --ip-addr 192.168.1.100 example.txns
```

### Transaction Language

The transaction DSL supports the following instructions:
- `begin`: Start a new transaction
- `put(key, value)`: Write a key-value pair (u64 values only)
- `get(key)`: Read a value by key
- `end`: Complete the transaction (auto-commit/abort based on conflicts)

Keywords are reserved and cannot be used as identifiers.
Whitespace is ignored.

## Command-Line Options

### KVS Coordinator
```
Usage: kvscoordinator [OPTIONS] <FILE_PATH>

Arguments:
  <FILE_PATH>  Path to file encoding operations.

Options:
  -s, --server-base <SERVER_BASE>  Base of hostname on LAN for a server, e.g. the "node" in "node0" [default: node]
  -n, --num-servers <NUM_SERVERS>  Number of servers/participants this coordinator is working with. [default: 1]
  -p, --port-no <PORT_NO>          Port that this coordinator communicates with participants on. [default: 8080]
  -l, --localhost                  Ignores other configuration like server_base, num_servers, etc and only works over the loopback
  -i, --ip-addr <IP_ADDR>          Ignores other configuration and just directly connects to one IP address.
  -h, --help                       Print help
```

### KVS Server
```
Usage: kvsserver [OPTIONS]

Options:
  -p, --port <PORT>                          [default: 8080]
  -n, --node-id <NODE_ID>                    [default: 0]
  -l, --localhost
      --sometime-port <SOMETIME_PORT>        [default: 50051]
      --sometime-node-id <SOMETIME_NODE_ID>  [default: 0]
  -h, --help                                 Print help
```

## Prerequisites

### System Requirements
- Rust toolchain (install via [rustup.rs](https://rustup.rs))
- Protocol Buffers compiler (`protoc`)

### Network Configuration
- Servers should be accessible via `node{X}` hostnames for cluster deployment
- Firewall rules allowing TCP communication on configured ports
- For CloudLab deployments: standard node naming convention supported

### Dependencies
All Rust dependencies are managed via Cargo:
- `tarpc`: High-performance RPC framework
- `dashmap`: Concurrent hash maps for storage
- `tokio`: Async runtime
- `tonic`: gRPC implementation
- `clap`: Command-line argument parsing
