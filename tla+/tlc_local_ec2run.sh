#! /bin/bash

set -euo pipefail


usage() {
    cat <<'EOF'
Find the EC2 'tla-cluster-master' instance, sync this repo to it, and launch
tla+/tlc_local_check.sh there in the background. With --kill, instead of
launching, kill any TLC activity on the master for a clean slate.

Usage:
  ./tla+/tlc_local_ec2run.sh -s <SPEC_DIR> -n <SPEC_NAME> [OPTIONS]
  ./tla+/tlc_local_ec2run.sh --kill [OPTIONS]

Spec arguments (required unless --kill):
  -s, --spec-dir <DIR>        spec folder under tla+/, e.g. multipaxos_leader_lease
  -n, --spec-name <NAME>      e.g. MultiPaxos
  -c, --cfg-suffix <SUFFIX>   optional, e.g. small -> MultiPaxos_MC_small.cfg

Mode:
      --kill                  kill any TLC activity on the master, then exit

Connection:
  -u, --ssh-user <USER>       SSH login user (default: ubuntu)
  -k, --ssh-key <PATH>        private key path
                              (default: ~/.ssh/TLC-Model-Check-Key-Pair.pem)
  -r, --aws-region <REGION>   AWS region (default: AWS CLI's resolved default)
  -t, --tag-name <NAME>       EC2 Name tag of the master
                              (default: tla-cluster-master)

  -h, --help                  show this help

Examples:
  ./tla+/tlc_local_ec2run.sh -s multipaxos_leader_lease -n MultiPaxos
  ./tla+/tlc_local_ec2run.sh -s multipaxos_leader_lease -n MultiPaxos -c small
  ./tla+/tlc_local_ec2run.sh --kill
EOF
}


SPEC_DIR=""
SPEC_NAME=""
CFG_SUFFIX=""
KILL_MODE=0
SSH_USER="ubuntu"
SSH_KEY="$HOME/.ssh/TLC-Model-Check-Key-Pair.pem"
AWS_REGION=""
EC2_TAG_NAME="tla-cluster-master"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--spec-dir)    SPEC_DIR="$2"; shift 2 ;;
        -n|--spec-name)   SPEC_NAME="$2"; shift 2 ;;
        -c|--cfg-suffix)  CFG_SUFFIX="$2"; shift 2 ;;
        --kill)           KILL_MODE=1; shift ;;
        -u|--ssh-user)    SSH_USER="$2"; shift 2 ;;
        -k|--ssh-key)     SSH_KEY="$2"; shift 2 ;;
        -r|--aws-region)  AWS_REGION="$2"; shift 2 ;;
        -t|--tag-name)    EC2_TAG_NAME="$2"; shift 2 ;;
        -h|--help)        usage; exit 0 ;;
        --)               shift; break ;;
        *)                echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$KILL_MODE" -eq 0 ]]; then
    if [[ -z "$SPEC_DIR" || -z "$SPEC_NAME" ]]; then
        echo "ERROR: --spec-dir and --spec-name are required (unless --kill)" >&2
        usage >&2
        exit 2
    fi
fi

AWS_REGION_ARG=""
[[ -n "$AWS_REGION" ]] && AWS_REGION_ARG="--region ${AWS_REGION}"


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_NAME="$(basename "$REPO_ROOT")"


# 0. Validate spec files exist locally (skip in --kill mode).
if [[ "$KILL_MODE" -eq 0 ]]; then
    cfg_tag=""
    [[ -n "$CFG_SUFFIX" ]] && cfg_tag="_${CFG_SUFFIX}"
    spec_local_dir="${SCRIPT_DIR}/${SPEC_DIR}"
    required_files=(
        "${spec_local_dir}/${SPEC_NAME}.tla"
        "${spec_local_dir}/${SPEC_NAME}_MC.tla"
        "${spec_local_dir}/${SPEC_NAME}_MC${cfg_tag}.cfg"
    )
    missing=()
    for f in "${required_files[@]}"; do
        [[ -f "$f" ]] || missing+=("$f")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR: required spec files not found locally:" >&2
        for f in "${missing[@]}"; do echo "  - ${f#"$REPO_ROOT"/}" >&2; done
        exit 1
    fi
fi


# 1. Find master public IP via AWS CLI.
echo
echo "[1/4] Looking up EC2 instance tagged Name=${EC2_TAG_NAME}..."
# shellcheck disable=SC2086
MASTER_IP="$(aws ec2 describe-instances ${AWS_REGION_ARG} \
    --filters "Name=tag:Name,Values=${EC2_TAG_NAME}" \
              "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].PublicIpAddress' \
    --output text | awk 'NF {print; exit}')"

if [[ -z "$MASTER_IP" || "$MASTER_IP" == "None" ]]; then
    echo "ERROR: no running EC2 instance with tag Name=${EC2_TAG_NAME} found." >&2
    echo "       (check AWS_PROFILE / --aws-region are set correctly)" >&2
    exit 1
fi
echo "Master public IP: ${MASTER_IP}"


SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)
[[ -n "$SSH_KEY" ]] && SSH_OPTS+=(-i "$SSH_KEY")


# --- Kill mode: terminate any TLC activity on master, then exit. ---
if [[ "$KILL_MODE" -eq 1 ]]; then
    REMOTE_KILL='set -u
patterns="tlc_local_check\.sh|tla2tools\.jar|tlc2\.tool\.distributed"
pids="$(pgrep -f -- "$patterns" || true)"
if [[ -z "$pids" ]]; then
    echo "No matching TLC processes found."
    exit 0
fi
echo "Matching processes:"
ps -o pid=,user=,etime=,args= -p $pids
echo
echo "Sending SIGTERM..."
kill $pids 2>/dev/null || true
for i in 1 2 3 4 5; do
    sleep 1
    pids="$(pgrep -f -- "$patterns" || true)"
    [[ -z "$pids" ]] && break
done
if [[ -n "$pids" ]]; then
    echo "Survivors after 5s; sending SIGKILL: $pids"
    kill -9 $pids 2>/dev/null || true
    sleep 1
fi
remaining="$(pgrep -f -- "$patterns" || true)"
if [[ -n "$remaining" ]]; then
    echo "ERROR: still alive: $remaining" >&2
    exit 1
fi
echo "All TLC processes killed."'

    echo
    echo "[2/2] Killing TLC processes on master..."
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${MASTER_IP}" "bash -s" <<< "$REMOTE_KILL"
    exit 0
fi


# 2. rsync repo to ~/<repo-name> on master.
SSH_CMD_STR="ssh ${SSH_OPTS[*]}"

echo
echo "[2/4] Syncing ${REPO_NAME}/ to ${SSH_USER}@${MASTER_IP}:~/${REPO_NAME}/ ..."
rsync -az --delete --stats \
      --exclude='.git/' \
      --exclude='target/' \
      --exclude='states/' \
      --exclude='*.log' \
      --exclude='/scratch/' \
      -e "$SSH_CMD_STR" \
      "${REPO_ROOT}/" "${SSH_USER}@${MASTER_IP}:${REPO_NAME}/"


# 3. Confirm ~/tla2tools.jar is in place on master.
echo
echo "[3/4] Checking ~/tla2tools.jar on master..."
if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${MASTER_IP}" 'test -f "$HOME/tla2tools.jar"'; then
    echo "ERROR: ~/tla2tools.jar missing on master." >&2
    echo "       run 'cd ${REPO_NAME}/tla+ && ./install_tla2tools.sh' there first." >&2
    exit 1
fi
echo "Found: tla2tools.jar OK"


# 4. Launch tlc_local_check.sh on the master, detached, log to a run-stamped file.
RUN_ID="$(date -u +%Y%m%d-%H%M%S)"
LOG_NAME="tlc-${SPEC_DIR}-${RUN_ID}.log"

CFG_FLAG=""
[[ -n "$CFG_SUFFIX" ]] && CFG_FLAG="-c ${CFG_SUFFIX}"

REMOTE_CMD="cd \"\$HOME/${REPO_NAME}\" && \
( setsid nohup ./tla+/tlc_local_check.sh -s ${SPEC_DIR} -n ${SPEC_NAME} ${CFG_FLAG} \
    > \"\$HOME/${REPO_NAME}/${LOG_NAME}\" 2>&1 < /dev/null & \
  echo \"PID=\$!\" ) &
exit 0"

echo
echo "[4/4] Launching tlc_local_check.sh on master (run id: ${RUN_ID})..."
ssh -n "${SSH_OPTS[@]}" "${SSH_USER}@${MASTER_IP}" "$REMOTE_CMD"

KEY_PART=""
[[ -n "$SSH_KEY" ]] && KEY_PART="-i ${SSH_KEY} "

echo
echo "Live-monitor with:"
echo "ssh ${KEY_PART}${SSH_USER}@${MASTER_IP} 'tail -f ${REPO_NAME}/${LOG_NAME}'"
