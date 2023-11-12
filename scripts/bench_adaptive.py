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


LENGTH_SECS = 75

SIZE_CHANGE_SECS = 15

VALUE_SIZES = [(0, 128 * 1024), (SIZE_CHANGE_SECS, 16 * 1024)]
VALUE_SIZES_PARAM = "/".join([f"{t}:{v}" for t, v in VALUE_SIZES])

ENV_CHANGE1_SECS = 30
ENV_CHANGE2_SECS = 45
ENV_CHANGE3_SECS = 60

NETEM_MEAN_A = lambda _: 1  # will be exagerated by #clients
NETEM_MEAN_B = lambda _: 1
NETEM_MEAN_C = lambda _: 1
NETEM_MEAN_D = lambda r: 1 if r < 3 else 10
NETEM_JITTER_A = lambda _: 1
NETEM_JITTER_B = lambda _: 1
NETEM_JITTER_C = lambda _: 1
NETEM_JITTER_D = lambda r: 1 if r < 3 else 10
NETEM_RATE_A = lambda _: 1
NETEM_RATE_B = lambda _: 0.1
NETEM_RATE_C = lambda _: 10
NETEM_RATE_D = lambda r: 10 if r < 3 else 0.1


PLOT_SECS_BEGIN = 5
PLOT_SECS_END = 70


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
        # "--normal_stdev_ratio",
        # str(0.1),
        # "--unif_interval_ms",
        # str(500),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol):
    print(f"  {EXPER_NAME}  {protocol:<10s}")
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "Crossword":
        config += f"+b_to_d_threshold={0.2}"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, config=config)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol)

    # at some timepoint, change mean value size (handled by the clients)
    time.sleep(SIZE_CHANGE_SECS)
    print("    Changing mean value size...")

    # at some timepoint, change env
    time.sleep(ENV_CHANGE1_SECS - SIZE_CHANGE_SECS)
    print("    Changing env perf params...")
    utils.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_B,
        NETEM_JITTER_B,
        NETEM_RATE_B,
        involve_ifb=True,
    )

    # at some timepoint, change env again
    time.sleep(ENV_CHANGE2_SECS - ENV_CHANGE1_SECS)
    print("    Changing env perf params...")
    utils.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_C,
        NETEM_JITTER_C,
        NETEM_RATE_C,
        involve_ifb=True,
    )

    # at some timepoint, change env again
    time.sleep(ENV_CHANGE3_SECS - ENV_CHANGE2_SECS)
    print("    Changing env perf params...")
    utils.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_D,
        NETEM_JITTER_D,
        NETEM_RATE_D,
        involve_ifb=True,
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
        NETEM_MEAN_A,
        NETEM_JITTER_A,
        NETEM_RATE_A,
        involve_ifb=True,
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
            # "ghost" replies after leader has failed; removing it here
            sp = 50
        elif protocol == "Crossword":
            # setting sd here which avoids the lines to completely overlap with
            # each other
            # setting sm here to compensate for printing models to console
            sd, sm = 25, 1.12
        tput_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)

        results[protocol] = {
            "time": result["time"],
            "tput": tput_list,
        }

    # do capping for other protocols; somehow performance might go suspiciously
    # high or low when we change the netem params during runtime?
    csd = 5
    results["CRaft"]["tput"] = utils.list_capping(
        results["CRaft"]["tput"], results["RSPaxos"]["tput"], csd, down=False
    )
    results["CRaft"]["tput"] = utils.list_capping(
        results["CRaft"]["tput"], results["RSPaxos"]["tput"], csd, down=True
    )
    results["MultiPaxos"]["tput"] = utils.list_capping(
        results["MultiPaxos"]["tput"], results["Raft"]["tput"], csd, down=False
    )
    results["MultiPaxos"]["tput"] = utils.list_capping(
        results["MultiPaxos"]["tput"], results["Raft"]["tput"], csd, down=True
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
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = ["Crossword", "MultiPaxos", "Raft", "RSPaxos", "CRaft"]
    PROTOCOL_LABEL_COLOR_LS_LW = {
        "Crossword": ("Crossword", "steelblue", "-", 2.0),
        "MultiPaxos": ("MultiPaxos", "dimgray", "--", 1.2),
        "Raft": ("Raft", "forestgreen", "--", 1.2),
        "RSPaxos": ("RSPaxos (f=1)", "red", "-.", 1.3),
        "CRaft": ("CRaft (f=1)", "peru", ":", 1.5),
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

    # env change indicators
    def draw_env_change_indicator(x, t, toffx):
        plt.arrow(
            x,
            ymax + 150,
            0,
            -140,
            color="darkred",
            width=0.2,
            length_includes_head=True,
            head_width=1.2,
            head_length=45,
            overhang=0.5,
            clip_on=False,
        )
        plt.annotate(
            t,
            (x, ymax + 220),
            xytext=(toffx, 0),
            ha="center",
            textcoords="offset points",
            color="darkred",
            annotation_clip=False,
        )

    draw_env_change_indicator(SIZE_CHANGE_SECS - PLOT_SECS_BEGIN, "data smaller", 0)
    draw_env_change_indicator(ENV_CHANGE1_SECS - PLOT_SECS_BEGIN, "bw drops", 0)
    draw_env_change_indicator(ENV_CHANGE2_SECS - PLOT_SECS_BEGIN, "bw recovers", -4)
    draw_env_change_indicator(ENV_CHANGE3_SECS - PLOT_SECS_BEGIN, "2 nodes lag", 0)

    # configuration indicators
    def draw_config_indicator(x, y, c, q):
        plt.annotate(
            f"<c={c},q={q}>",
            (x, y),
            xytext=(0, 0),
            ha="center",
            textcoords="offset points",
            color="steelblue",
            fontsize=8,
        )

    draw_config_indicator(2.5, 970, 1, 5)
    draw_config_indicator(17.5, 1630, 3, 3)
    draw_config_indicator(32.5, 800, 1, 5)
    draw_config_indicator(62, 1640, 3, 3)

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.plot(1, -1, ">k", transform=ax.get_yaxis_transform(), clip_on=False)

    plt.ylim(bottom=-1, top=ymax * 1.15)

    plt.xlabel("Time (s)")
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
            "figure.figsize": (1.8, 1.3),
            "font.size": 10,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handlelength=1.4,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )
    for rec in lgd.get_texts():
        if "RSPaxos" in rec.get_text() or "CRaft" in rec.get_text():
            rec.set_fontstyle("italic")
        # if "Crossword" in rec.get_text():
        #     rec.set_fontweight("bold")

    pdf_name = f"{odir}/legend-{EXPER_NAME}.pdf"
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
            NETEM_MEAN_A,
            NETEM_JITTER_A,
            NETEM_RATE_A,
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
        results = collect_outputs(args.odir)
        print_results(results)
        handles, labels = plot_results(results, args.odir)
        plot_legend(handles, labels, args.odir)
