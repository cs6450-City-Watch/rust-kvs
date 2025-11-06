# Rusty KVS with SomeTime

SomeTime API works regardless of asynch/synch deployment of SomeTime.
In general, it works like how TrueTime works with Spanner.

This KVS is, of course, a sharded and replicated KVS that spans multiple machines.
Using the CloudLab facilities across the US, we've also been able
to confirm that this works with minimal overhead regarding typical operations.

## SomeTime usage in our project

Again, like Spanner and TrueTime, SomeTime is used by this client
by ensuring consistency in distributed transactions, globally ordering events,
commit-wait logic, and applications with snapshots.

### Commit-Wait

Transactions are committed with a timestamp using `tstamp = ST.Now().latest`.
The commitment is actually performed once the timestamp for the commitment
is in the past, i.e. `tstamp <= ST.Now().earliest`.

### Consistency in Transactions (TODO)

Again, using the commit timestamp, a reasonable client (e.g. our rusty KVS)
will ensure that other replicas participating in the transaction
also use the same commit timestamp. This ensures global consistency.

Reads are only performed on data that has a timestamp earlier than `ST.Now().earliest`.

Transactions also give external consistency across replicas-
commit operations depend on this consistency as part of commit-wait logic.

### Ordering of Events

Obviously, ordering of events kind of naturally falls out of using
a "global clock" of sorts.

### Snapshots

Snapshots are facilitated more or less with an MVCC approach.
Transactions with different timestamps will get the data relevant to that timestamp.

## Useful Commands

This repository contains two useful programs: kvscoordinator and kvsserver.
In general, you can run `cargo run -- -h` to find the CLI options for each.

### KVS Coordinator options

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

#### FILE_PATH argument

The KVS coordinator has an ingrained transaction parser to assist in writing transactions on the fly.

##### Example file:
```
begin
put(hello, 42)
get(hello)
end
```
`end` used instead of something like `commit` or `abort`
as that decision depends on context beyond this file.

Whitespace is ignored, but each command should follow one of the following patterns:
- `begin` (just "begin")
- `put(<ID>, <VAL>)` (put followed by lparen followed by ID followed by comma followed by val followed by rparen)
- `get(<ID>)` (get followed by lparen followed by ID followed by rparen)
- `end`

Keywords `begin`, `put,` `get`, and `end` are parsed differently from identifiers
and should not be used as identifiers.

### KVS Server options

These are generally the complement to the KVS Coordinator options and do what you would expect.
Currently we only support deployments on CloudLab LAN experiments- that is to say, the `node` prefix is assumed.

```
Usage: kvsserver [OPTIONS]

Options:
  -p, --port <PORT>        [default: 8080]
  -n, --node-id <NODE_ID>  [default: 0]
  -l, --localhost
  -h, --help               Print help
```

## Prerequisites

It is not strictly necessary to understand the rust programming language to use this project.
Currently, it is meant as a supplement to projects implementing the "TrueTime" interface for timestamp ordering.

Otherwise, to compile and run, you will need to have a system whose hostname follows the `node{x}` pattern,
and the following build dependencies:

- [rust](<rustup.rs>)
- any protobuf compiler (e.g. for Fedora 42: `rust-protobuf-devel`)

