#/bin/env bash

ssh node1 "/mnt/nfs/riverman/kvs/kvscoordinator/target/debug/kvscoordinator --server-addr 10.10.1.1:8080 > $PWD/node1.out" &
ssh node2 "/mnt/nfs/riverman/kvs/kvscoordinator/target/debug/kvscoordinator --server-addr 10.10.1.1:8080 > $PWD/node2.out" &

