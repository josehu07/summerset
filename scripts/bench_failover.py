import sys
import os
import argparse
import statistics

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "failover"


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

BATCH_INTERVAL = 1

NUM_REPLICAS = 5
NUM_CLIENTS = 8

VALUE_SIZE = 1024 * 1024
PUT_RATIO = 100
LENGTH_SECS = 30

NETEM_MEAN = 1
NETEM_JITTER = 1
NETEM_RATE = 1


PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]


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

        if "manager" not in l and "accepting clients" in l:
            replica = int(l[l.find("(") + 1 : l.find(")")])
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


def bench_round(protocol):
    print(
        f"  {EXPER_NAME}  {protocol:<10s}  {NUM_REPLICAS:1d}  v={VALUE_SIZE:<9d}"
        f"  w%={PUT_RATIO:<3d}  {LENGTH_SECS:3d}s  {NUM_CLIENTS:2d}"
    )
    utils.kill_all_local_procs()

    proc_cluster = launch_cluster(
        protocol, config=f"batch_interval_ms={BATCH_INTERVAL}"
    )
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    proc_clients = run_bench_clients(protocol)
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    proc_cluster.terminate()
    utils.kill_all_local_procs()
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "ab") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done")


def plot_results(results):
    for protocol, result in results.items():
        print(protocol)
        ts = result["time"]
        tputs = utils.list_smoothing(result["tput_sum"], 2)
        for i, t in enumerate(ts):
            print(t, tputs[i])


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

        utils.set_tcp_buf_sizes()
        utils.set_tc_qdisc_netem(NETEM_MEAN, NETEM_JITTER, NETEM_RATE)

        print("Running experiments...")
        for protocol in PROTOCOLS:
            bench_round(protocol)

        utils.clear_tc_qdisc_netem()

    else:
        results = dict()
        for protocol in PROTOCOLS:
            results[protocol] = utils.gather_outputs(
                protocol,
                NUM_CLIENTS,
                f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
                LENGTH_SECS * 0.3,
                LENGTH_SECS * 0.9,
            )

        plot_results(results)