#! /bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/../utils/kill_match.sh"

pattern='-cp chain(-client)?\.jar:\. '
if [ "$1" = "incl_distr" ]; then
    pattern+='|-m scripts\.crossword\.distr_chain(app|cli)( |$)'
fi

kill_match "$pattern"
