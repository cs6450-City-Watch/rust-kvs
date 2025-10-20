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

