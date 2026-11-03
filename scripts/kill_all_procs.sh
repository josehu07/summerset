#! /bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/utils/kill_match.sh"

pattern='/summerset_(server|manager|client)( |$)'
pattern+='|-m scripts\.local_(cluster|clients)( |$)'
pattern+='|target/[^ ]*deps/(summerset(_server|_manager|_client)?|(cluster|server|manager|client)_tests)-[0-9a-f]+( |$)'
if [ "$1" = "incl_distr" ]; then
    pattern+='|-m scripts\.distr_(cluster|clients)( |$)'
fi

kill_match "$pattern"
