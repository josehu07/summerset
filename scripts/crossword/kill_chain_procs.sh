#! /bin/bash


kill_all_matching () {
    for pid in $(pgrep -f $1)
    do
        sudo kill -9 $pid > /dev/null 2>&1
    done
}


kill_all_matching java


if [ $# -ge 1 ] && [ "$1" = "incl_distr" ];
then
    kill_all_matching distr_chainapp.py
    kill_all_matching distr_chaincli.py
fi