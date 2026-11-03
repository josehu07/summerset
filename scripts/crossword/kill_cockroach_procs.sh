#! /bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/../utils/kill_match.sh"

pattern='(^|[ /])cockroach (start|init|sql|workload)( |$)'
if [ "$1" = "incl_distr" ]; then
    pattern+='|-m scripts\.crossword\.distr_cock(roach|wload)( |$)'
fi

kill_match "$pattern"
