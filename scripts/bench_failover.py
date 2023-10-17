import sys
import os
import argparse
import subprocess
import statistics


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"

EXPER_NAME = "failover"


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

NUM_REPLICAS = 5
NUM_CLIENTS = 2

VALUE_SIZE = 1024 * 1024
PUT_RATIO = 100
LENGTH_SECS = 60

PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]


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


def launch_cluster(protocol, num_replicas, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
        "-r",
        "--file_prefix",
        f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
    ]
    if config is not None and len(config) > 0:
        cmd += ["-c", config]
    return run_process(cmd)


def wait_cluster_setup(proc, num_replicas):
    # print("Waiting for cluster setup...")
    accepting_clients = [False for _ in range(num_replicas)]

    for line in iter(proc.stderr.readline, b""):
        l = line.decode()
        # print(l, end="", file=sys.stderr)
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
        "-f",
        str(0),
        "-v",
        str(value_size),
        "-w",
        str(put_ratio),
        "-l",
        str(length_s),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
    ]
    return run_process(cmd)


def bench_round(
    protocol,
    num_replicas,
    num_clients,
    value_size,
    put_ratio,
    length_s,
):
    print(
        f"{EXPER_NAME}  {protocol:<10s}  {num_replicas:1d}  v={value_size:<9d}  "
        + f"w%={put_ratio:<3d}  {length_s:3d}s  {num_clients:2d}"
    )
    kill_all_local_procs()

    proc_cluster = launch_cluster(protocol, num_replicas)
    wait_cluster_setup(proc_cluster, num_replicas)

    proc_clients = run_bench_clients(
        protocol, num_clients, value_size, put_ratio, length_s
    )
    out, err = proc_clients.communicate()

    proc_cluster.terminate()
    kill_all_local_procs()

    if proc_clients.returncode != 0:
        print("Experiment FAILED!")
        print(out.decode())
        print(err.decode())
        sys.exit(1)
    else:
        print("  Done")


if __name__ == "__main__":
    check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not args.plot:
        do_cargo_build()

        for protocol in PROTOCOLS:
            bench_round(
                protocol,
                NUM_REPLICAS,
                NUM_CLIENTS,
                VALUE_SIZE,
                PUT_RATIO,
                LENGTH_SECS,
            )

    else:
        pass
