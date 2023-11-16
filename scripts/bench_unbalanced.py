import sys
import os
import argparse
import time
import statistics

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "unbalanced"

PROTOCOL_FT_ASSIGNS = [
    ("MultiPaxos", 2, None),
    ("RSPaxos", 2, None),
    ("RSPaxos", 1, None),
    ("Raft", 2, None),
    ("CRaft", 2, None),
    ("CRaft", 1, None),
    ("Crossword", 2, "0:0,1,2,3,4/1:3,4,5,6,7/2:6,7,8,9,10/3:11,12,13/4:14"),
    ("Crossword", 2, "3"),
]


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"
SERVER_IFB = lambda r: f"ifb{r}"

NUM_REPLICAS = 5
NUM_CLIENTS = 16

FORCE_LEADER = 0


BATCH_INTERVAL = 1

VALUE_SIZE = 256 * 1024
PUT_RATIO = 100


NETEM_MEAN = lambda _: 1  # will be exagerated by #clients
NETEM_JITTER = lambda _: 0
NETEM_RATE = lambda r: 2 if r < 3 else 1 if r < 4 else 0.2

RS_TOTAL_SHARDS = 15
RS_DATA_SHARDS = 9


LENGTH_SECS = 20

RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 18


def round_midfix_str(fault_tolerance, init_assignment):
    return (
        f".{fault_tolerance}."
        + f"{'b' if init_assignment is None or len(init_assignment) == 1 else 'u'}"
    )


def launch_cluster(protocol, midfix_str, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--force_leader",
        str(FORCE_LEADER),
        "--file_prefix",
        f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        "--file_midfix",
        midfix_str,
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


def run_bench_clients(protocol, midfix_str):
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
        "--normal_stdev_ratio",
        str(0.1),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
        "--file_midfix",
        midfix_str,
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol, fault_tolerance, init_assignment):
    midfix_str = round_midfix_str(fault_tolerance, init_assignment)
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "RSPaxos" or protocol == "CRaft":
        config += f"+fault_tolerance={fault_tolerance}"
    elif protocol == "Crossword":
        config += f"+rs_total_shards={RS_TOTAL_SHARDS}"
        config += f"+rs_data_shards={RS_DATA_SHARDS}"
        config += f"+init_assignment='{init_assignment}'"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, midfix_str, config=config)
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol, midfix_str)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.kill_all_local_procs()
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "ab") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(odir):
    results = dict()
    for protocol, fault_tolerance, init_assignment in PROTOCOL_FT_ASSIGNS:
        midfix_str = round_midfix_str(fault_tolerance, init_assignment)
        result = utils.gather_outputs(
            f"{protocol}{midfix_str}",
            NUM_CLIENTS,
            odir,
            RESULT_SECS_BEGIN,
            RESULT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 20, 0, 0, 1
        tput_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)

        results[f"{protocol}{midfix_str}"] = {
            "mean": sum(tput_list) / len(tput_list),
            "stdev": statistics.stdev(tput_list),
        }

    return results


def print_results(results):
    for protocol_with_midfix, result in results.items():
        print(protocol_with_midfix)
        print(f"  mean {result['mean']:7.2f}  stdev {result['stdev']:7.2f}")


def plot_results(results, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.6, 2.9),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos.2.b",
        "Raft.2.b",
        "RSPaxos.2.b",
        "CRaft.2.b",
        "Crossword.2.b",
        "Crossword.2.u",
        "RSPaxos.1.b",
        "CRaft.1.b",
    ]
    PROTOCOLS_XPOS = {
        "MultiPaxos.2.b": 1,
        "Raft.2.b": 2,
        "RSPaxos.2.b": 3,
        "CRaft.2.b": 4,
        "Crossword.2.b": 5,
        "Crossword.2.u": 6,
        "RSPaxos.1.b": 7.8,
        "CRaft.1.b": 8.8,
    }
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos.2.b": ("MultiPaxos", "darkgray", None),
        "Raft.2.b": ("Raft", "lightgreen", None),
        "RSPaxos.2.b": ("RSPaxos (q=5 forced)", "salmon", "//"),
        "CRaft.2.b": ("CRaft (q=5 forced)", "wheat", "\\\\"),
        "Crossword.2.b": ("Crossword (balanced)", "lightsteelblue", "xx"),
        "Crossword.2.u": ("Crossword (unbalanced)", "cornflowerblue", ".."),
        "RSPaxos.1.b": ("RSPaxos (q=4, f=1)", "pink", "//"),
        "CRaft.1.b": ("CRaft (q=4, f=1)", "cornsilk", "\\\\"),
    }

    for protocol_with_midfix in PROTOCOLS_ORDER:
        xpos = PROTOCOLS_XPOS[protocol_with_midfix]
        result = results[protocol_with_midfix]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol_with_midfix]
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

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xticks([3.5, 8.3], ["f=2", "f=1"])
    plt.tick_params(bottom=False)

    plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{odir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.4, 2.2),
            "font.size": 10,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    handles.insert(-2, matplotlib.lines.Line2D([], [], linestyle=""))
    labels.insert(-2, "")  # insert spacing between groups
    lgd = plt.legend(
        handles,
        labels,
        handleheight=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )
    for rec in lgd.get_texts():
        if "f=1" in rec.get_text():
            rec.set_fontstyle("italic")
        # if "Crossword" in rec.get_text():
        #     rec.set_fontweight("bold")

    pdf_name = f"{odir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.check_proper_cwd()
    utils.check_enough_cpus()

    parser = argparse.ArgumentParser(allow_abbrev=False)
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

    if not args.plot:
        runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        utils.do_cargo_build(release=True)

        print("Setting tc netem qdiscs...")
        utils.set_all_tc_qdisc_netems(
            NUM_REPLICAS,
            SERVER_NETNS,
            SERVER_DEV,
            SERVER_IFB,
            NETEM_MEAN,
            NETEM_JITTER,
            NETEM_RATE,
            involve_ifb=True,
        )

        print("Running experiments...")
        for protocol, fault_tolerance, init_assignment in PROTOCOL_FT_ASSIGNS:
            bench_round(protocol, fault_tolerance, init_assignment)

        print("Clearing tc netem qdiscs...")
        utils.kill_all_local_procs()
        utils.clear_all_tc_qdisc_netems(
            NUM_REPLICAS, SERVER_NETNS, SERVER_DEV, SERVER_IFB
        )

    else:
        results = collect_outputs(args.odir)
        print_results(results)
        handles, labels = plot_results(results, args.odir)
        plot_legend(handles, labels, args.odir)
