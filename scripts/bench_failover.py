import sys
import os
import argparse
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "failover"

PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"

BATCH_INTERVAL = 1

NUM_REPLICAS = 5
NUM_CLIENTS = 8

VALUE_SIZE = 1024 * 1024
PUT_RATIO = 100

NETEM_MEAN = 1
NETEM_JITTER = 0
NETEM_RATE = 1


LENGTH_SECS = 120
CLIENT_TIMEOUT_SECS = 2

FAIL1_SECS = 40
FAIL2_SECS = 80

PLOT_SECS_BEGIN = 15
PLOT_SECS_END = 115


def launch_cluster(protocol, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--file_prefix",
        f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--use_veth",
    ]
    if config is not None and len(config) > 0:
        cmd += ["-c", config]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def wait_cluster_setup(proc, fserr=None):
    # print("Waiting for cluster setup...")
    accepting_clients = [False for _ in range(NUM_REPLICAS)]

    for line in iter(proc.stderr.readline, b""):
        if fserr is not None:
            fserr.write(line)
        l = line.decode()
        # print(l, end="", file=sys.stderr)

        if "accepting clients" in l:
            replica = l[l.find("(") + 1 : l.find(")")]
            if replica == "m":
                continue
            replica = int(replica)
            assert not accepting_clients[replica]
            accepting_clients[replica] = True

        if accepting_clients.count(True) == NUM_REPLICAS:
            break


def run_bench_clients(protocol):
    cmd = [
        "python3",
        "./scripts/local_clients.py",
        "-p",
        protocol,
        "-r",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--use_veth",
        "--base_idx",
        str(0),
        "--timeout_ms",
        str(CLIENT_TIMEOUT_SECS * 1000),
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-f",
        str(0),  # closed-loop
        "-v",
        str(VALUE_SIZE),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def run_mess_client(protocol, pauses=None, resumes=None):
    cmd = [
        "python3",
        "./scripts/local_clients.py",
        "-p",
        protocol,
        "-r",
        "--use_veth",
        "--base_idx",
        str(NUM_CLIENTS),
        "mess",
    ]
    if pauses is not None and len(pauses) > 0:
        cmd += ["--pause", pauses]
    if resumes is not None and len(resumes) > 0:
        cmd += ["--resume", resumes]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol):
    print(
        f"  {EXPER_NAME}  {protocol:<10s}  {NUM_REPLICAS:1d}  v={VALUE_SIZE:<9d}"
        f"  w%={PUT_RATIO:<3d}  {LENGTH_SECS:3d}s  {NUM_CLIENTS:2d}"
    )
    utils.kill_all_local_procs()
    time.sleep(1)

    # launch service cluster
    proc_cluster = launch_cluster(
        protocol, config=f"batch_interval_ms={BATCH_INTERVAL}"
    )
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol)

    # at the first failure point, pause current leader
    time.sleep(FAIL1_SECS)
    print("    Pausing leader...")
    proc_mess = run_mess_client(protocol, pauses="l")
    proc_mess.wait()

    # at the second failure point, pause current leader
    time.sleep(FAIL2_SECS - FAIL1_SECS)
    print("    Pausing leader...")
    proc_mess = run_mess_client(protocol, pauses="l")
    proc_mess.wait()

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.kill_all_local_procs()
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "ab") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs():
    results = dict()
    for protocol in PROTOCOLS:
        results[protocol] = utils.gather_outputs(
            protocol,
            NUM_CLIENTS,
            f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
            PLOT_SECS_BEGIN,
            PLOT_SECS_END,
            0.1,
        )
    return results


def print_results(results):
    for protocol, result in results.items():
        print(protocol)
        for i, t in enumerate(result["time"]):
            print(f" [{t:>5.1f}] {result['tput_sum'][i]:>6.2f} ", end="")
            if (i + 1) % 6 == 0:
                print()
        if len(result["time"]) % 6 != 0:
            print()


def plot_results(results):
    for protocol, result in results.items():
        xs = result["time"]
        sd, sp, sj = 8, 0, 0
        if protocol == "Raft" or protocol == "CRaft":
            # due to an implementation choice, Raft clients see a spike of
            # "ghost" replies after leader has failed; removing it here
            sp = 50
        elif protocol == "Crossword":
            # due to limited sampling granularity, Crossword gossiping makes
            # throughput results look a bit more "jittering" than it actually
            # is; smoothing a bit more here
            sd, sj = 12, 50
        ys = utils.list_smoothing(result["tput_sum"], sd, sp, sj)
        plt.plot(xs, ys, label=protocol)

    plt.legend()

    plt.savefig(
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}/{EXPER_NAME}.png", dpi=300
    )
    plt.close()


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
    if not os.path.isdir(runlog_path):
        os.system(f"mkdir -p {runlog_path}")

    if not args.plot:
        utils.do_cargo_build(release=True)

        print("Setting tc netem qdiscs...")
        for replica in range(NUM_REPLICAS):
            utils.set_tc_qdisc_netem(
                SERVER_NETNS(replica),
                SERVER_DEV(replica),
                NETEM_MEAN,
                NETEM_JITTER,
                NETEM_RATE,
            )

        print("Running experiments...")
        for protocol in PROTOCOLS:
            bench_round(protocol)

        print("Clearing tc netem qdiscs...")
        utils.kill_all_local_procs()
        for replica in range(NUM_REPLICAS):
            utils.clear_tc_qdisc_netem(
                SERVER_NETNS(replica),
                SERVER_DEV(replica),
            )

    else:
        results = collect_outputs()
        print_results(results)
        plot_results(results)
