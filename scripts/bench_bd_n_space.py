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

EXPER_NAME = "bd_n_space"

PROTOCOLS = ["MultiPaxos", "Crossword"]


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"
SERVER_IFB = lambda r: f"ifb{r}"

NUM_REPLICAS = 5
NUM_CLIENTS = 16


BATCH_INTERVAL = 1

VALUE_SIZE = 64 * 1024

PUT_RATIO = 100


NETEM_MEAN = lambda _: 1  # will be exagerated by #clients
NETEM_JITTER = lambda _: 0
NETEM_RATE = lambda _: 1


LENGTH_SECS = 60


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
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol):
    print(
        f"  {EXPER_NAME}  {protocol:<10s}  {NUM_REPLICAS:1d}  v={VALUE_SIZE}"
        f"  w%={PUT_RATIO:<3d}  {LENGTH_SECS:3d}s  {NUM_CLIENTS:2d}"
    )
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += f"+record_breakdown=true"
    if protocol == "Crossword":
        config += f"+disable_gossip_timer=true"
        config += f"+init_assignment='1'"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, config=config)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol)

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


def collect_bd_stats(ldir):
    bd_stats = dict()
    for protocol in PROTOCOLS:
        bd_stats[protocol] = dict()
        total_cnt = 0

        with open(f"{ldir}/{protocol}.s.err", "r") as flog:
            for line in flog:
                if "bd cnt" in line:
                    line = line.strip()
                    line = line[line.find("bd cnt") + 6 :]
                    segs = line.split()

                    cnt = int(segs[0])
                    if cnt > 0:
                        total_cnt += cnt
                        idx = 1
                        while idx < segs.len():
                            step = segs[idx]
                            mean = float(segs[idx + 1])
                            stdev = float(segs[idx + 2])

                            if step not in bd_stats[protocol]:
                                bd_stats[protocol][step] = [mean, stdev**2]
                            else:
                                bd_stats[protocol][step][0] += mean
                                bd_stats[protocol][step][1] += stdev**2

                            idx += 3

        for step in bd_stats[protocol]:
            bd_stats[protocol][0] /= total_cnt
            bd_stats[protocol][1] = (bd_stats[protocol][1] / total_cnt) ** 0.5

    return bd_stats


def print_results(bd_stats, space_usage):
    for protocol, stats in bd_stats.items():
        print(protocol)
        for step, stat in stats:
            print(f"  {step} {stat[0]:.2f} ±{stat[1]:.2f}", end="")
        print()


def plot_results(results, ldir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4, 3),
            "font.size": 10,
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
        "RSPaxos.1.b": 8.2,
        "CRaft.1.b": 9.2,
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

    plt.xticks([3.5, 8.7], ["f=2", "f=1"])
    plt.tick_params(bottom=False)

    plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{ldir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, ldir):
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

    pdf_name = f"{ldir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    parser.add_argument(
        "-l",
        "--ldir",
        type=str,
        default=f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}",
        help=".err files directory",
    )
    parser.add_argument(
        "-s",
        "--sdir",
        type=str,
        default=f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        help=".wal files directory",
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
        for protocol in PROTOCOLS:
            bench_round(protocol)

        print("Clearing tc netem qdiscs...")
        utils.kill_all_local_procs()
        utils.clear_all_tc_qdisc_netems(
            NUM_REPLICAS, SERVER_NETNS, SERVER_DEV, SERVER_IFB
        )

    else:
        bd_stats = collect_bd_stats(args.ldir)
        space_usage = collect_space_usage(args.sdir)
        print_results(bd_stats, space_usage)
        handles, labels = plot_results(bd_stats, space_usage, args.ldir)
        plot_legend(handles, labels, args.ldir)
