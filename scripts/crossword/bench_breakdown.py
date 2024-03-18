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


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "breakdown"

PROTOCOLS = ["MultiPaxos", "Crossword"]


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


LENGTH_SECS = 30


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
        "--norm_stdev_ratio",
        str(0.1),
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
    raw_stats = dict()
    for protocol in PROTOCOLS:
        raw_stats[protocol] = dict()
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
                        while idx < len(segs):
                            step = segs[idx]
                            mean = float(segs[idx + 1]) / 1000.0
                            stdev = float(segs[idx + 2]) / 1000.0

                            if step not in raw_stats[protocol]:
                                raw_stats[protocol][step] = [mean, stdev**2]
                            else:
                                raw_stats[protocol][step][0] += mean * cnt
                                raw_stats[protocol][step][1] += stdev**2 * cnt

                            idx += 3

        for step in raw_stats[protocol]:
            raw_stats[protocol][step][0] /= total_cnt
            raw_stats[protocol][step][1] = (
                raw_stats[protocol][step][1] / total_cnt
            ) ** 0.5

    bd_stats = dict()
    for protocol, stats in raw_stats.items():
        bd_stats[protocol] = dict()
        bd_stats[protocol]["comp"] = stats["comp"] if "comp" in stats else (0.0, 0.0)
        bd_stats[protocol]["acc"] = (
            stats["arep"][0] - stats["ldur"][0],
            stats["arep"][1] - stats["ldur"][1],
        )
        bd_stats[protocol]["dur"] = stats["ldur"]
        bd_stats[protocol]["rep"] = stats["qrum"]
        bd_stats[protocol]["exec"] = stats["exec"]

    return bd_stats


def collect_space_usage(sdir):
    space_usage = dict()
    for protocol in PROTOCOLS:
        wal_size = os.path.getsize(f"{sdir}/{protocol}.0.wal")
        space_usage[protocol] = wal_size / (1024.0 * 1024.0)

    return space_usage


def print_results(bd_stats, space_usage=None):
    for protocol, stats in bd_stats.items():
        print(protocol)
        for step, stat in stats.items():
            print(f"  {step} {stat[0]:5.2f} ±{stat[1]:5.2f} ms", end="")
        print()
        if space_usage is not None:
            print(f"  usage {space_usage[protocol]:7.2f} MB")


def plot_breakdown(bd_stats, ldir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3, 2),
            "font.size": 10,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = ["MultiPaxos", "Crossword"]
    PROTOCOLS_YPOS = {
        "MultiPaxos": 3.4,
        "Crossword": 1.4,
    }
    STEPS_ORDER = ["comp", "acc", "dur", "rep", "exec"]
    STEPS_LABEL_COLOR_HATCH = {
        "comp": ("RS coding computation", "lightgreen", "---"),
        "acc": ("Leader→follower Accept msg", "salmon", None),
        "dur": ("Writing to durable WAL", "orange", "///"),
        "rep": ("Follower→leader AcceptReply", "honeydew", None),
        "exec": ("Commit & execution", "lightskyblue", "xxx"),
    }
    BAR_HEIGHT = 0.8

    xmax = 0
    range_xs = {protocol: [] for protocol in PROTOCOLS_ORDER}
    for protocol in PROTOCOLS_ORDER:
        ypos = PROTOCOLS_YPOS[protocol]
        stats = bd_stats[protocol]

        xnow = 0
        for step in STEPS_ORDER:
            label, color, hatch = STEPS_LABEL_COLOR_HATCH[step]

            if step == "exec":
                stdev = sum([bd_stats[protocol][s][1] for s in STEPS_ORDER])
                stdev /= len(STEPS_ORDER)
                plt.barh(
                    ypos,
                    stats[step][0],
                    left=xnow,
                    height=BAR_HEIGHT,
                    color=color,
                    edgecolor="black",
                    linewidth=1,
                    label=label if protocol == "Crossword" else None,
                    hatch=hatch,
                    xerr=[[0], [stdev]],
                    ecolor="black",
                    capsize=3,
                )
            else:
                plt.barh(
                    ypos,
                    stats[step][0],
                    left=xnow,
                    height=BAR_HEIGHT,
                    color=color,
                    edgecolor="black",
                    linewidth=1,
                    label=label if protocol == "Crossword" else None,
                    hatch=hatch,
                )

            xnow += stats[step][0]
            if xnow > xmax:
                xmax = xnow

            if step in ("comp", "dur", "rep"):
                range_xs[protocol].append(xnow)

    plt.text(0.3, 4.2, "MultiPaxos & Raft", verticalalignment="center")
    plt.text(0.3, 0.5, "Crossword & others", verticalalignment="center")

    for i in range(3):
        plt.plot(
            [range_xs["MultiPaxos"][i], range_xs["Crossword"][i]],
            [
                PROTOCOLS_YPOS["MultiPaxos"] - BAR_HEIGHT / 2,
                PROTOCOLS_YPOS["Crossword"] + BAR_HEIGHT / 2,
            ],
            color="dimgray",
            linestyle="--",
            linewidth=1,
        )

    plt.text(
        0.3,
        2.5,
        "due to bw save",
        verticalalignment="center",
        color="dimgray",
        fontsize=9,
    )

    plt.text(
        xmax * 0.82,
        1,
        "due to\nmore replies\nto wait for",
        verticalalignment="center",
        color="dimgray",
        fontsize=9,
    )
    plt.plot(
        [
            xmax * 0.78,
            xmax * 0.83,
        ],
        [2.42, 1.9],
        color="dimgray",
        linestyle="-",
        linewidth=1,
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.ylim((0, 4.7))
    plt.tick_params(left=False)
    plt.yticks([])

    plt.xlim((0, xmax * 1.1))
    plt.xlabel("Elapsed time (ms)")

    plt.tight_layout()

    pdf_name = f"{ldir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, ldir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 1.4),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handlelength=0.6,
        handleheight=1.5,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

    pdf_name = f"{ldir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def save_space_usage(space_usage, ldir):
    txt_name = f"{ldir}/exper-wal-space.txt"
    with open(txt_name, "w") as ftxt:
        for protocol, size_mb in space_usage.items():
            ftxt.write(f"{protocol}  {size_mb:.2f} MB\n")
    print(f"Saved: {txt_name}")


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

        state_path = f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}"
        utils.remove_files_in_dir(state_path)

    else:
        bd_stats = collect_bd_stats(args.ldir)
        # space_usage = collect_space_usage(args.sdir)
        # print_results(bd_stats, space_usage)
        print_results(bd_stats)
        handles, labels = plot_breakdown(bd_stats, args.ldir)
        plot_legend(handles, labels, args.ldir)
        # save_space_usage(space_usage, args.ldir)
