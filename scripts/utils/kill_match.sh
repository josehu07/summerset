#! /bin/bash

# prints PIDs of this shell and all its ancestors (e.g., an ssh session or a
# script that invoked this one), which must never be killed
ancestors() {
    local pid=$$
    while [ -n "$pid" ] && [ "$pid" -gt 1 ]; do
        echo "$pid"
        pid=$(ps -o ppid= -p "$pid" | tr -d ' ')
    done
}

# prints PID and command line of live processes matching ERE $1, except for
# this shell's own helper processes and PIDs listed in $2
matching() {
    ps -A -ww -o pid=,ppid=,stat=,args= |
        RE="$1" ME="$$" SKIP=" $2 " awk '{
            pid = $1; ppid = $2; stat = $3
            cmd = $0; sub(/^ *[0-9]+ +[0-9]+ +[^ ]+ +/, "", cmd)
            if (stat !~ /^Z/ && ppid != ENVIRON["ME"] &&
                index(ENVIRON["SKIP"], " " pid " ") == 0 && cmd ~ ENVIRON["RE"])
                print pid " " cmd
        }'
}

# signals processes listed in $2 (as printed by `matching`), through sudo if
# needed (never prompting for a password)
signal_procs() {
    local pid _
    while read -r pid _; do
        kill -s "$1" "$pid" 2>/dev/null || sudo -n kill -s "$1" "$pid" 2>/dev/null
    done <<<"$2"
}

# prints processes listed in $2, each line prefixed by $1
print_procs() {
    local line
    while read -r line; do
        echo "$1$line"
    done <<<"$2"
}

# kills all processes matching ERE $1: SIGTERM first for a graceful exit, then
# SIGKILL whatever is left after ~3 secs; fails if any still survives after ~5
kill_match() {
    local skip procs start=$SECONDS forced=
    skip=$(ancestors | tr '\n' ' ')
    procs=$(matching "$1" "$skip")
    [ -z "$procs" ] && return 0
    [ -t 1 ] && print_procs "Killing: " "$procs" | cut -c1-120
    signal_procs TERM "$procs"
    while [ $((SECONDS - start)) -lt 5 ]; do
        sleep 0.1
        procs=$(matching "$1" "$skip")
        [ -z "$procs" ] && return 0
        if [ -z "$forced" ] && [ $((SECONDS - start)) -ge 3 ]; then
            signal_procs KILL "$procs"
            forced=1
        fi
    done
    print_procs "FAILED to kill: " "$procs" >&2
    return 1
}
