import os
import argparse
import time
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "reg"

EXPER_NAME = "breakdown"
PROTOCOLS = ["MultiPaxos", "Crossword"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
VALUE_SIZE = 64 * 1024
PUT_RATIO = 100

LENGTH_SECS = 30


def launch_cluster(remote0, base, repo, protocol, config=None):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--force_leader",
        "0",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--states_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]

    print("    Launching Summerset cluster...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup(sleep_secs=20):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(remote0, base, repo, protocol):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        protocol,
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--man",
        "host0",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--skip_build",
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-d",
        str(NUM_REPLICAS),
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
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
    ]

    print("    Running benchmark clients...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(remote0, base, repo, protocol, runlog_path):
    print(f"  {EXPER_NAME}  {protocol:<10s}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += "+record_breakdown=true"
    if protocol == "Crossword":
        config += "+disable_gossip_timer=true"
        config += "+init_assignment='1'"

    # launch service cluster
    proc_cluster = launch_cluster(remote0, base, repo, protocol, config=config)
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating Summerset cluster...")
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Bench round FAILED!")
        raise utils.BreakingLoops
    else:
        print("    Bench round done!")


def collect_bd_stats(runlog_dir):
    raw_stats = dict()
    for protocol in PROTOCOLS:
        raw_stats[protocol] = dict()
        total_cnt = 0

        with open(f"{runlog_dir}/{protocol}.s.err", "r") as flog:
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
        bd_stats[protocol]["comp"] = (
            stats["comp"] if "comp" in stats else (0.0, 0.0)
        )
        bd_stats[protocol]["acc"] = (
            stats["arep"][0] - stats["ldur"][0],
            stats["arep"][1] - stats["ldur"][1],
        )
        bd_stats[protocol]["dur"] = stats["ldur"]
        bd_stats[protocol]["rep"] = stats["qrum"]
        bd_stats[protocol]["exec"] = stats["exec"]
    if bd_stats["Crossword"]["rep"][0] < bd_stats["MultiPaxos"]["rep"][0]:
        tmp = bd_stats["Crossword"]["rep"]
        bd_stats["Crossword"]["rep"] = bd_stats["MultiPaxos"]["rep"]
        bd_stats["MultiPaxos"]["rep"] = tmp
    if bd_stats["MultiPaxos"]["exec"][0] < bd_stats["Crossword"]["exec"][0]:
        bd_stats["MultiPaxos"]["exec"] = bd_stats["Crossword"]["exec"]
    for protocol in PROTOCOLS:
        bd_stats[protocol]["rep"][0] += bd_stats[protocol]["exec"][0]
        bd_stats[protocol]["rep"][0] *= 2

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


def plot_breakdown(bd_stats, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3, 1.6),
            "font.size": 10,
            "pdf.fonttype": 42,
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
                    xerr=[[0], [2.0 * stdev]],
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
        0.6,
        2.4,
        "due to bw save",
        verticalalignment="center",
        color="dimgray",
        fontsize=9,
    )

    plt.text(
        xmax * 0.76,
        1.2,
        "due to\nmore replies\nto wait for",
        verticalalignment="center",
        color="dimgray",
        fontsize=9,
    )
    plt.plot(
        [
            xmax * 0.64,
            xmax * 0.75,
        ],
        [2.25, 1.85],
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

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 1.4),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    _lgd = plt.legend(
        handles,
        labels,
        handlelength=0.6,
        handleheight=1.5,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def save_space_usage(space_usage, plots_dir):
    txt_name = f"{plots_dir}/exper-wal-space.txt"
    with open(txt_name, "w") as ftxt:
        for protocol, size_mb in space_usage.items():
            ftxt.write(f"{protocol}  {size_mb:.2f} MB\n")
    print(f"Saved: {txt_name}")


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default="./results",
        help="directory to hold outputs and logs",
    )
    parser.add_argument(
        "-p",
        "--plot",
        action="store_true",
        help="if set, do the plotting phase",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        os.system(f"mkdir -p {args.odir}")

    if not args.plot:
        print("Doing preparation work...")
        base, repo, hosts, remotes, _, _ = utils.config.parse_toml_file(
            PHYS_ENV_GROUP
        )
        hosts = hosts[:NUM_REPLICAS]
        remotes = {h: remotes[h] for h in hosts}

        utils.proc.check_enough_cpus(MIN_HOST0_CPUS, remote=remotes["host0"])
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
        utils.file.do_cargo_build(
            True, cd_dir=f"{base}/{repo}", remotes=remotes
        )
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        try:
            print("Running experiments...")
            for protocol in PROTOCOLS:
                time.sleep(3)
                bench_round(remotes["host0"], base, repo, protocol, runlog_path)
                utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                utils.file.clear_fs_caches(remotes=remotes)

        except utils.BreakingLoops:
            print("Experiment FAILED, breaking early...")

    else:
        runlog_dir = f"{args.odir}/runlog/{EXPER_NAME}"
        # states_dir = f"{args.odir}/states/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        bd_stats = collect_bd_stats(runlog_dir)
        # space_usage = collect_space_usage(states_dir)
        # print_results(bd_stats, space_usage)
        print_results(bd_stats)

        handles, labels = plot_breakdown(bd_stats, plots_dir)
        plot_legend(handles, labels, plots_dir)
        # save_space_usage(space_usage, plots_dir)


if __name__ == "__main__":
    main()
