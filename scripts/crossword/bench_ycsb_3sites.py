import sys
import os
import argparse
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import common_utils as utils

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


BASE_PATH = "/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "ycsb_3sites"

MAIN_HOST_NICKNAME = "host0"

YCSB_DIR = f"{BASE_PATH}/ycsb"
YCSB_TRACE = "/tmp/ycsb_workloada.txt"

SUMMERSET_PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]
CHAIN_PROTOCOLS = ["chain_delayed", "chain_mixed"]


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

NUM_REPLICAS = 3
NUM_CLIENTS = 16


BATCH_INTERVAL = 1

VALUE_SIZE = 256 * 1024
PUT_RATIO = 50  # YCSB-A has 50% updates + 50% reads


LENGTH_SECS = 60

RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 58


def gen_ycsb_a_trace():
    cmd = [
        f"{YCSB_DIR}/bin/ycsb.sh",
        "run",
        "basic",
        "-P",
        f"{YCSB_DIR}/workloads/workloada",
    ]
    proc = utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )
    out, _ = proc.communicate()
    raw = out.decode()

    # clean the trace
    with open(YCSB_TRACE, "w+") as fout:
        for line in raw.strip().split("\n"):
            line = line.strip()
            if line.startswith("READ ") or line.startswith("UPDATE "):
                segs = line.split()
                op = segs[0]
                key = segs[2]
                fout.write(f"{op} {key}\n")


def launch_cluster_summerset(protocol, config=None):
    cmd = [
        "python3",
        "./scripts/remote_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--me",
        MAIN_HOST_NICKNAME,
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


def wait_cluster_setup_summerset(proc, fserr=None):
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

        if accepting_clients.count(True) == 1:
            # early breaking here: do not reply on SSH-piped output
            break

    # take extra 10 seconds for the other peers to become ready
    time.sleep(10)


def run_bench_clients_summerset(protocol):
    cmd = [
        "python3",
        "./scripts/remote_clients.py",
        "-p",
        protocol,
        "-r",
        "--me",
        MAIN_HOST_NICKNAME,
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--base_idx",
        str(0),
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-f",
        str(0),  # closed-loop
        "-v",
        str(VALUE_SIZE),
        # "-w",
        # str(PUT_RATIO),
        "-y",
        YCSB_TRACE,
        "-l",
        str(LENGTH_SECS),
        "--norm_stdev_ratio",
        str(0.1),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round_summerset(protocol):
    print(f"  {EXPER_NAME}  {protocol:<10s}")
    utils.kill_all_local_procs()
    time.sleep(5)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += f"+sim_read_lease=true"
    if protocol == "RSPaxos" or protocol == "CRaft":
        config += f"+fault_tolerance=1"
    if protocol == "Crossword":
        config += f"+init_assignment='1'"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster_summerset(protocol, config=config)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup_summerset(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients_summerset(protocol)

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


def launch_cluster_chain(protocol):
    cmd = [
        "python3",
        "./scripts/crossword/remote_chainapp.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "--me",
        MAIN_HOST_NICKNAME,
        "--file_prefix",
        f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def wait_cluster_setup_chain(proc, fserr=None):
    # print("Waiting for cluster setup...")
    # NOTE: using `proc.stdout` here as the ChainPaxos app prints logs to stdout
    for line in iter(proc.stdout.readline, b""):
        if fserr is not None:
            fserr.write(line)
        l = line.decode()
        # print(l, end="", file=sys.stderr)

        if "I am leader now!" in l:
            break

    # take extra 10 seconds for the other peers to become ready
    time.sleep(10)


def run_bench_clients_chain(protocol):
    cmd = [
        "python3",
        "./scripts/crossword/remote_chaincli.py",
        "-p",
        protocol,
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "-n",
        str(NUM_CLIENTS),
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


def bench_round_chain(protocol):
    print(f"  {EXPER_NAME}  {protocol:<10s}")

    # launch service cluster
    proc_cluster = launch_cluster_chain(protocol)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup_chain(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients_chain(protocol)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    os.system("./scripts/crossword/kill_chain_procs.sh")
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "ab") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(odir):
    results = dict()

    for protocol in SUMMERSET_PROTOCOLS:
        result = utils.gather_outputs(
            protocol,
            NUM_CLIENTS,
            odir,
            RESULT_SECS_BEGIN,
            RESULT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 20, 0, 0, 1
        if protocol == "Crossword":
            # setting sm here to compensate for printing models to console
            sm = 1.1
        tput_mean_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
        tput_stdev_list = result["tput_stdev"]
        lat_mean_list = utils.list_smoothing(result["lat_avg"], sd, sp, sj, 1 / sm)
        lat_stdev_list = result["lat_stdev"]

        results[protocol] = {
            "tput": {
                "mean": tput_mean_list,
                "stdev": tput_stdev_list,
            },
            "lat": {
                "mean": lat_mean_list,
                "stdev": lat_stdev_list,
            },
        }

    # do capping for other protocols to remove weird spikes/dips introduced
    # by the TCP stack
    def result_cap(pa, pb, down):
        for metric in ("tput", "lat"):
            for stat in ("mean", "stdev"):
                results[f"{pa}"][metric][stat] = utils.list_capping(
                    results[f"{pa}"][metric][stat],
                    results[f"{pb}"][metric][stat],
                    5,
                    down=down if metric == "tput" else not down,
                )

    result_cap("RSPaxos", "CRaft", False)
    result_cap("RSPaxos", "CRaft", True)
    result_cap("MultiPaxos", "Raft", False)
    result_cap("MultiPaxos", "Raft", True)

    for protocol in SUMMERSET_PROTOCOLS:
        if protocol in results:
            tput_mean_list = results[protocol]["tput"]["mean"]
            tput_stdev_list = results[protocol]["tput"]["stdev"]
            lat_mean_list = results[protocol]["lat"]["mean"]
            lat_stdev_list = results[protocol]["lat"]["stdev"]

            results[protocol] = {
                "tput": {
                    "mean": sum(tput_mean_list) / len(tput_mean_list),
                    "stdev": (
                        sum(map(lambda s: s**2, tput_stdev_list)) / len(tput_stdev_list)
                    )
                    ** 0.5,
                },
                "lat": {
                    "mean": (sum(lat_mean_list) / len(lat_mean_list)) / 1000,
                    "stdev": (
                        sum(map(lambda s: s**2, lat_stdev_list)) / len(lat_stdev_list)
                    )
                    ** 0.5
                    / (1000 * NUM_CLIENTS / SERVER_PIN_CORES),
                },
            }

    for protocol in CHAIN_PROTOCOLS:
        results[protocol] = utils.parse_ycsb_log(
            protocol,
            odir,
            1,
            1,
        )
        results[protocol]["tput"]["stdev"] /= NUM_CLIENTS / SERVER_PIN_CORES
        results[protocol]["lat"]["stdev"] /= NUM_CLIENTS / SERVER_PIN_CORES

    return results


def print_results(results):
    for protocol, result in results.items():
        print(protocol)
        print(
            f"  tput  mean {result['tput']['mean']:7.2f}  stdev {result['tput']['stdev']:7.2f}"
            + f"  lat  mean {result['lat']['mean']:7.2f}  stdev {result['lat']['stdev']:7.2f}"
        )


def plot_results(results, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4.5, 2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Raft",
        "Crossword",
        "RSPaxos",
        "CRaft",
        "chain_delayed",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Raft": ("Raft", "lightgreen", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "RSPaxos": ("RSPaxos", "salmon", "//"),
        "CRaft": ("CRaft", "wheat", "\\\\"),
        "chain_delayed": ("ChainPaxos", "plum", "--"),
    }

    ax1 = plt.subplot(121)
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol]["tput"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            result["mean"],
            width=1,
            color=color,
            edgecolor="black",
            linewidth=1.4,
            label=label,
            hatch=hatch,
            # yerr=result["stdev"],
            # ecolor="black",
            # capsize=1,
        )

    plt.tick_params(bottom=False, labelbottom=False)
    plt.ylabel("Throughput (reqs/s)")

    ax2 = plt.subplot(122)
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol]["lat"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            result["mean"],
            width=1,
            color=color,
            edgecolor="gray",
            linewidth=1.4,
            hatch=hatch,
            yerr=result["stdev"],
            ecolor="black",
            capsize=1,
        )

    plt.tick_params(bottom=False, labelbottom=False)
    plt.ylabel("Latency (ms)")

    for ax in fig.axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()

    pdf_name = f"{odir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_legend(handles, labels, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2, 2),
            "font.size": 10,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handleheight=0.9,
        handlelength=1.3,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

    pdf_name = f"{odir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-t", "--trace", action="store_true", help="if set, do YCSB trace generation"
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default=f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
        help=".out files directory",
    )
    args = parser.parse_args()

    if args.trace:
        print("Generating YCSB-A trace for Summerset...")
        if os.path.isfile(YCSB_TRACE):
            print(f"  {YCSB_TRACE} already there, skipped")
        else:
            gen_ycsb_a_trace()
            print(f"  Done: {YCSB_TRACE}")

    elif not args.plot:
        utils.check_enough_cpus()

        runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        print("Running experiments...")
        for protocol in SUMMERSET_PROTOCOLS:
            bench_round_summerset(protocol)
        for protocol in CHAIN_PROTOCOLS:
            bench_round_chain(protocol)

        state_path = f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}"
        utils.remove_files_in_dir(state_path)

    else:
        results = collect_outputs(args.odir)
        print_results(results)
        handles, labels = plot_results(results, args.odir)
        plot_legend(handles, labels, args.odir)
