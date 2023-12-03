#! /bin/bash

# Usage: ./scripts/kill_chain_procs.sh


kill_all_matching () {
    for pid in $(pgrep -f $1)
    do
        sudo kill -9 $pid > /dev/null 2>&1
    done
}


kill_all_matching java
