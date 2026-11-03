#! /bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/../utils/kill_match.sh"

pattern='zkServer\.sh start-foreground|org\.apache\.zookeeper\.server\.quorum\.QuorumPeerMain'
if [ "$1" = "incl_distr" ]; then
    pattern+='|-m scripts\.bodega\.distr_zk(cluster|clients)( |$)'
fi

kill_match "$pattern"
