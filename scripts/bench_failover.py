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
SERVER_IFB = lambda r: f"ifb{r}"

NUM_REPLICAS = 5
NUM_CLIENTS = 16

BATCH_INTERVAL = 1

VALUE_SIZE = 128 * 1024
PUT_RATIO = 100

NETEM_MEAN = lambda _: 1  # will be exagerated by #clients
NETEM_JITTER = lambda _: 1
NETEM_RATE = lambda _: 1


LENGTH_SECS = 120
CLIENT_TIMEOUT_SECS = 2

FAIL1_SECS = 40
FAIL2_SECS = 80

PLOT_SECS_BEGIN = 25
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
    print(f"  {EXPER_NAME}  {protocol:<10s}")
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "Crossword":
        config += "+init_assignment='1'"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, config=config)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol)

    # at the first failure point, pause current leader
    time.sleep(FAIL1_SECS)
    print("    Pausing leader...")
    run_mess_client(protocol, pauses="l")

    # at the second failure point, pause current leader
    time.sleep(FAIL2_SECS - FAIL1_SECS)
    print("    Pausing leader...")
    run_mess_client(protocol, pauses="l")

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

        sd, sp, sj, sm = 10, 0, 0, 1
        if protocol == "Raft" or protocol == "CRaft":
            # due to an implementation choice, Raft clients see a spike of
            # "ghost" replies after leader has failed; removing it here
            sp = 50
        elif protocol == "Crossword":
            # due to limited sampling granularity, Crossword gossiping makes
            # throughput results look a bit more "jittering" than it actually
            # is; smoothing a bit more here
            # setting sd here also avoids the lines to completely overlap with
            # each other
            sd, sj = 15, 50
        tput_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)

        results[protocol] = {
            "time": result["time"],
            "tput": tput_list,
        }

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
            "font.size": 13,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = ["Crossword", "MultiPaxos", "Raft", "RSPaxos", "CRaft"]
    PROTOCOL_LABEL_COLOR_LS_LW = {
        "Crossword": ("Crossword", "steelblue", "-", 2.0),
        "MultiPaxos": ("MultiPaxos", "dimgray", "--", 1.2),
        "Raft": ("Raft", "forestgreen", "--", 1.2),
        "RSPaxos": ("RSPaxos (f=1)", "red", "-.", 1.3),
        "CRaft": ("CRaft (f=1, fb. ok)", "peru", ":", 1.5),
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

    # failure indicators
    def draw_failure_indicator(x, t, toffx):
        plt.arrow(
            x,
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
            t,
            (x, ymax + 30),
            xytext=(toffx, 0),
            ha="center",
            textcoords="offset points",
            color="darkred",
            annotation_clip=False,
        )

    draw_failure_indicator(FAIL1_SECS - PLOT_SECS_BEGIN, "Leader fails", 12)
    draw_failure_indicator(FAIL2_SECS - PLOT_SECS_BEGIN, "New leader fails", 12)

    # recovery time indicators (hardcoded!)
    def draw_recovery_indicator(x, y, w, t, toffx, toffy):
        plt.arrow(
            x,
            y,
            -w,
            0,
            color="gray",
            width=0.1,
            length_includes_head=True,
            head_width=3,
            head_length=0.3,
            overhang=0.5,
        )
        plt.arrow(
            x,
            y,
            w,
            0,
            color="gray",
            width=0.1,
            length_includes_head=True,
            head_width=3,
            head_length=0.3,
            overhang=0.5,
        )
        if t is not None:
            plt.annotate(
                t,
                (x + toffx, y + toffy),
                xytext=(0, 0),
                ha="center",
                textcoords="offset points",
                color="gray",
                fontsize=10,
            )

    draw_recovery_indicator(19, 135, 3.6, "small\ngossip\ngap", 1, 11)
    draw_recovery_indicator(59.2, 135, 3.6, "small\ngossip\ngap", 1, 11)

    plt.vlines(
        63.5,
        110,
        140,
        colors="gray",
        linestyles="solid",
        linewidth=0.8,
    )

    draw_recovery_indicator(23, 50, 7, None, None, None)
    draw_recovery_indicator(25.2, 62, 9.2, "state-send\nsnapshot int.", 4.6, -53)
    draw_recovery_indicator(67.5, 56, 11.5, "state-send\nsnapshot int.", 2.9, -47)

    # configuration indicators
    def draw_config_indicator(x, y, c, q, color, fb=False, unavail=False):
        t = f"[c={c},q={q}]"
        if fb:
            t += "\nfb. ok"
        if unavail:
            t += "\nunavail."
        plt.annotate(
            t,
            (x, y),
            xytext=(0, 0),
            ha="center",
            textcoords="offset points",
            color=color,
            fontsize=11,
        )

    draw_config_indicator(4.8, 228, 1, 5, "steelblue")
    draw_config_indicator(4.8, 198, 1, 4, "red")
    draw_config_indicator(4.8, 175, 1, 4, "peru")
    draw_config_indicator(4.8, 112, 3, 3, "forestgreen")

    draw_config_indicator(44.8, 148, 2, 4, "steelblue")
    draw_config_indicator(44.8, 198, 1, 4, "red")
    draw_config_indicator(44.8, 58, 3, 3, "peru", fb=True)
    draw_config_indicator(44.8, 112, 3, 3, "forestgreen")

    draw_config_indicator(89.5, 135, 3, 3, "steelblue")
    draw_config_indicator(89.5, 9, 1, 4, "red", unavail=True)
    draw_config_indicator(89.5, 78, 3, 3, "peru")
    draw_config_indicator(89.5, 112, 3, 3, "forestgreen")

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
            "pdf.fonttype": 42,
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
        utils.check_enough_cpus()

        runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        utils.do_cargo_build(release=True)

        print("Setting tc netem qdiscs...")
        utils.clear_fs_cache()
        utils.set_all_tc_qdisc_netems(
            NUM_REPLICAS,
            SERVER_NETNS,
            SERVER_DEV,
            SERVER_IFB,
            NETEM_MEAN,
            NETEM_JITTER,
            NETEM_RATE,
        )

        print("Running experiments...")
        for protocol in PROTOCOLS:
            bench_round(protocol)

        print("Clearing tc netem qdiscs...")
        utils.kill_all_local_procs()
        utils.clear_all_tc_qdisc_netems(
            NUM_REPLICAS, SERVER_NETNS, SERVER_DEV, SERVER_IFB
        )

        state_path = f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}"
        utils.remove_files_in_dir(state_path)

    else:
        results = collect_outputs(args.odir)
        print_results(results)
        handles, labels = plot_results(results, args.odir)
        plot_legend(handles, labels, args.odir)
