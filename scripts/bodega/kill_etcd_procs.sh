#! /bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/../utils/kill_match.sh"

pattern='(^|[ /])bin/etcd( |$)'
if [ "$1" = "incl_distr" ]; then
    pattern+='|-m scripts\.bodega\.distr_etcd(cluster|clients)( |$)'
fi

kill_match "$pattern"
