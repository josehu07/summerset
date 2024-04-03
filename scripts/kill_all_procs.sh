#! /bin/bash


kill_all_matching () {
    for pid in $(pgrep -f $1)
    do
        sudo kill -9 $pid > /dev/null 2>&1
    done
}


kill_all_matching summerset_server
kill_all_matching summerset_client
kill_all_matching summerset_manager

kill_all_matching local_cluster.py
kill_all_matching local_clients.py


if [ $# -ge 1 ] && [ "$1" = "incl_distr" ];
then
    kill_all_matching distr_cluster.py
    kill_all_matching distr_clients.py
fi
