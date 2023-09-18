#! /bin/bash

kill_all_matching () {
    for pid in $(sudo pgrep -f $1)
    do
        sudo kill -9 $pid
    done
}

kill_all_matching summerset_server
kill_all_matching summerset_client
kill_all_matching summerset_manager
kill_all_matching local_cluster.py
kill_all_matching local_client.py
