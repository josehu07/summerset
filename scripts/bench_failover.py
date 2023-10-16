import sys
import os
import subprocess
import statistics


BASE_PATH = "/mnt/eval"
EXPER_NAME = "failover"


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

NUM_REPLICAS = 5
VALUE_SIZE = 1024 * 1024  # 1MB
PUT_RATIO = 100
LENGTH_SECS = 60


PROTOCOL_BACKER_PATH = {
    "MultiPaxos": lambda r: f"backer_path='{BASE_PATH}/wals/{EXPER_NAME}/multipaxos.{r}.wal'",
    "Raft": lambda r: f"backer_path='{BASE_PATH}/wals/{EXPER_NAME}/raft.{r}.wal'",
    "RSPaxos": lambda r: f"backer_path='{BASE_PATH}/wals/{EXPER_NAME}/rs_paxos.{r}.wal'",
    "CRaft": lambda r: f"backer_path='{BASE_PATH}/wals/{EXPER_NAME}/craft.{r}.wal'",
    "Crossword": lambda r: f"backer_path='{BASE_PATH}/wals/{EXPER_NAME}/crossword.{r}.wal'",
}

PROTOCOL_SNAPSHOT_PATH = {
    "MultiPaxos": lambda r: f"snapshot_path='{BASE_PATH}/snaps/{EXPER_NAME}/multipaxos.{r}.snap'",
    "Raft": lambda r: f"snapshot_path='{BASE_PATH}/snaps/{EXPER_NAME}/raft.{r}.snap'",
    "RSPaxos": lambda r: f"snapshot_path='{BASE_PATH}/snaps/{EXPER_NAME}/rs_paxos.{r}.snap'",
    "CRaft": lambda r: f"snapshot_path='{BASE_PATH}/snaps/{EXPER_NAME}/craft.{r}.snap'",
    "Crossword": lambda r: f"snapshot_path='{BASE_PATH}/snaps/{EXPER_NAME}/crossword.{r}.snap'",
}


CLIENT_OUTPUT_PATH = {
    "MultiPaxos": lambda c: f"{BASE_PATH}/output/{EXPER_NAME}/multipaxos.{c}.log",
    "Raft": lambda c: f"{BASE_PATH}/output/{EXPER_NAME}/raft.{c}.log",
    "RSPaxos": lambda c: f"{BASE_PATH}/output/{EXPER_NAME}/rs_paxos.{c}.log",
    "CRaft": lambda c: f"{BASE_PATH}/output/{EXPER_NAME}/craft.{c}.log",
    "Crossword": lambda c: f"{BASE_PATH}/output/{EXPER_NAME}/crossword.{c}.log",
}


def path_get_last_segment(path):
    if "/" not in path:
        return None
    eidx = len(path) - 1
    while eidx > 0 and path[eidx] == "/":
        eidx -= 1
    bidx = path[:eidx].rfind("/")
    bidx += 1
    return path[bidx : eidx + 1]


def check_proper_cwd():
    cwd = os.getcwd()
    if "summerset" not in path_get_last_segment(cwd) or not os.path.isdir("scripts/"):
        print(
            "ERROR: script must be run under top-level repo with `python3 scripts/<script>.py ...`"
        )
        sys.exit(1)


def do_cargo_build():
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace", "-r"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd):
    # print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def kill_all_local_procs():
    # print("Killing all local procs...")
    cmd = ["sudo", "./scripts/kill_local_procs.sh"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def launch_cluster(protocol, num_replicas, files_config):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
        "-r",
        "-c",
        files_config,
        "--pin_cores",
        str(SERVER_PIN_CORES),
    ]
    return run_process(cmd)


def wait_cluster_setup(proc, num_replicas):
    # print("Waiting for cluster setup...")
    accepting_clients = [False for _ in range(num_replicas)]

    for line in iter(proc.stderr.readline, b""):
        l = line.decode()
        print(l, end="", file=sys.stderr)
        if "manager" not in l and "accepting clients" in l:
            replica = int(l[l.find("(") + 1 : l.find(")")])
            assert not accepting_clients[replica]
            accepting_clients[replica] = True

        if accepting_clients.count(True) == num_replicas:
            break


def run_bench_clients(protocol, num_clients, value_size, put_ratio, length_s):
    cmd = [
        "python3",
        "./scripts/local_clients.py",
        "-p",
        protocol,
        "-r",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "bench",
        "-n",
        str(num_clients),
        "-v",
        str(value_size),
        "-w",
        str(put_ratio),
        "-l",
        str(length_s),
    ]
    return run_process(cmd)


def bench_round(
    protocol,
    num_replicas,
    value_size,
    put_ratio,
    length_s,
    fault_tolerance=None,
    shards_per_replica=None,
):
    print(
        f"{EXPER_NAME}  {protocol:<10s}  n={num_replicas:1d}  v={value_size:<9d}  "
        + f"w%={put_ratio:<3d}  {length_s:3d}s"
    )

    kill_all_local_procs()

    file_configs = [PROTOCOL_BACKER_PATH[protocol]()]
    configs.append(f"perf_storage_a={PERF_STORAGE_ALPHA}")
    configs.append(f"perf_storage_b={PERF_STORAGE_BETA}")
    configs.append(f"perf_network_a={PERF_NETWORK_ALPHA}")
    configs.append(f"perf_network_b={PERF_NETWORK_BETA}")

    proc_cluster = launch_cluster(protocol, num_replicas, "+".join(configs))
    wait_cluster_setup(proc_cluster, num_replicas)

    proc_client = run_bench_client(protocol, value_size, put_ratio, length_s)
    out, err = proc_client.communicate()

    proc_cluster.terminate()
    proc_cluster.wait()

    if proc_client.returncode != 0:
        print(err.decode())
    else:
        parse_output(out.decode())


if __name__ == "__main__":
    check_proper_cwd()
    do_cargo_build()

    for protocol in PROTOCOL_BACKER_PATH:
        bench_round(
            protocol,
            NUM_REPLICAS,
            VALUE_SIZE,
            PUT_RATIO,
            LENGTH_SECS,
        )
