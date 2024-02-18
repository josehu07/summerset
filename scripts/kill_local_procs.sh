#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


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
kill_all_matching local_client.py
