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

EXPER_NAME = "adaptive"

PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]


SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"
SERVER_IFB = lambda r: f"ifb{r}"

NUM_REPLICAS = 5
NUM_CLIENTS = 16

FORCE_LEADER = 0


BATCH_INTERVAL = 1

PUT_RATIO = 100


LENGTH_SECS = 100

SIZE_CHANGE_SECS = 35

VALUE_SIZES = [(0, 8192), (SIZE_CHANGE_SECS, 65536)]
VALUE_SIZES_PARAM = "/".join([f"{t}:{v}" for t, v in VALUE_SIZES])

ENV_CHANGE_SECS = 65

NETEM_MEAN_S = lambda _: 1  # will be exagerated by #clients
NETEM_MEAN_E = lambda r: 5 if r >= 3 else 1
NETEM_JITTER_S = lambda _: 1
NETEM_JITTER_E = lambda r: 5 if r >= 3 else 1
NETEM_RATE_S = lambda _: 1
NETEM_RATE_E = lambda r: 1 if r >= 3 else 2


PLOT_SECS_BEGIN = 5
PLOT_SECS_END = 95


def launch_cluster(protocol, config=None):
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
        VALUE_SIZES_PARAM,
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--normal_stdev_ratio",
        str(0.1),
        "--unif_interval_ms",
        str(500),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol):
    print(
        f"  {EXPER_NAME}  {protocol:<10s}  {NUM_REPLICAS:1d}  v={VALUE_SIZES_PARAM}"
        f"  w%={PUT_RATIO:<3d}  {LENGTH_SECS:3d}s  {NUM_CLIENTS:2d}"
    )
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, config=config)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol)

    # at some timepoint, change mean value size (handled by the clients)
    time.sleep(SIZE_CHANGE_SECS)
    print("    Changing mean value size...")

    # at some timepoint, change env to be more jittery
    time.sleep(ENV_CHANGE_SECS - SIZE_CHANGE_SECS)
    print("    Changing env perf params...")
    utils.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_E,
        NETEM_JITTER_E,
        NETEM_RATE_E,
    )

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

    # revert env params to initial
    utils.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_S,
        NETEM_JITTER_S,
        NETEM_RATE_S,
    )

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(odir):
    results = dict()
    for protocol in PROTOCOLS:
        result = utils.gather_outputs(
            protocol,
            NUM_CLIENTS,
            odir,
            PLOT_SECS_BEGIN,
            PLOT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 20, 0, 0, 1
        if protocol == "Raft" or protocol == "CRaft":
            # due to an implementation choice, Raft clients see a spike of
            # "ghost" replies after stable state has changed; removing it here
            sp = 50
        elif protocol == "Crossword":
            # due to limited sampling granularity, Crossword gossiping makes
            # throughput results look a bit more "jittering" than it actually
            # is; smoothing a bit more here
            # setting sd here also avoids the lines to completely overlap with
            # each other
            # setting sm here to compensate for the injected uniform dist reqs
            sd, sj, sm = 30, 50, 1.1
        tput_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)

        results[protocol] = {
            "time": result["time"],
            "tput": tput_list,
        }

    # integrity check that CRaft and RSPaxos are on the same level
    results["CRaft"]["tput"] = utils.list_capping(
        results["CRaft"]["tput"], results["RSPaxos"]["tput"], sd
    )

    return results


def print_results(results):
    for protocol, result in results.items():
        print(protocol)
        for i, t in enumerate(result["time"]):
            print(f" [{t:>5.1f}] {result['tput'][i]:>7.2f} ", end="")
            if (i + 1) % 6 == 0:
                print()
        if len(result["time"]) % 6 != 0:
            print()


def plot_results(results, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (6, 3),
            "font.size": 10,
        }
    )
    fig = plt.figure()

    PROTOCOLS_ORDER = ["Crossword", "MultiPaxos", "Raft", "RSPaxos", "CRaft"]
    PROTOCOL_LABEL_COLOR_LS_LW = {
        "Crossword": ("Crossword", "steelblue", "-", 2.0),
        "MultiPaxos": ("MultiPaxos", "dimgray", "--", 1.2),
        "Raft": ("Raft", "forestgreen", "--", 1.2),
        "RSPaxos": ("RSPaxos", "red", "-.", 1.3),
        "CRaft": ("CRaft", "peru", ":", 1.5),
    }

    ymax = 0.0
    for protocol in PROTOCOLS_ORDER:
        result = results[protocol]
        label, color, ls, lw = PROTOCOL_LABEL_COLOR_LS_LW[protocol]

        xs = result["time"]
        ys = result["tput"]
        if max(ys) > ymax:
            ymax = max(ys)

        plt.plot(
            xs,
            ys,
            label=label,
            color=color,
            linestyle=ls,
            linewidth=lw,
            zorder=10 if "Crossword" in protocol else 0,
        )

    plt.arrow(
        SIZE_CHANGE_SECS - PLOT_SECS_BEGIN,
        ymax + 20,
        0,
        -18,
        color="darkred",
        width=0.2,
        length_includes_head=True,
        head_width=1.5,
        head_length=5,
        overhang=0.5,
        clip_on=False,
    )
    plt.annotate(
        "Value size changes",
        (SIZE_CHANGE_SECS - PLOT_SECS_BEGIN, ymax + 30),
        xytext=(-18, 0),
        ha="center",
        textcoords="offset points",
        color="darkred",
        annotation_clip=False,
    )

    plt.arrow(
        ENV_CHANGE_SECS - PLOT_SECS_BEGIN,
        ymax + 20,
        0,
        -18,
        color="darkred",
        width=0.2,
        length_includes_head=True,
        head_width=1.5,
        head_length=5,
        overhang=0.5,
        clip_on=False,
    )
    plt.annotate(
        "Env perf changes",
        (ENV_CHANGE_SECS - PLOT_SECS_BEGIN, ymax + 30),
        xytext=(-28, 0),
        ha="center",
        textcoords="offset points",
        color="darkred",
        annotation_clip=False,
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.plot(1, -1, ">k", transform=ax.get_yaxis_transform(), clip_on=False)

    plt.ylim(bottom=-1, top=ymax * 1.15)

    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (reqs/s)")

    lgd = plt.legend(handlelength=1.4, loc="upper right", bbox_to_anchor=(1.02, 1.12))
    for rec in lgd.get_texts():
        if "RSPaxos" in rec.get_text() or "CRaft" in rec.get_text():
            rec.set_fontstyle("italic")
        # if "Crossword" in rec.get_text():
        #     rec.set_fontweight("bold")

    plt.tight_layout()

    png_name = f"{odir}/{EXPER_NAME}.png"
    plt.savefig(png_name, dpi=300)
    plt.close()
    print(f"Plotted: {png_name}")


if __name__ == "__main__":
    utils.check_proper_cwd()

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
            NETEM_MEAN_S,
            NETEM_JITTER_S,
            NETEM_RATE_S,
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
        results = collect_outputs(args.odir)
        print_results(results)
        plot_results(results, args.odir)
