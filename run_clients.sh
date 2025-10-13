#/bin/env bash

ssh node2 '(time /mnt/nfs/kvs/kvscoordinator/target/debug/kvscoordinator -n 2) > /mnt/nfs/kvs/node2.out 2>&1' &
ssh node3 '(time /mnt/nfs/kvs/kvscoordinator/target/debug/kvscoordinator -n 2) > /mnt/nfs/kvs/node3.out 2>&1' &

