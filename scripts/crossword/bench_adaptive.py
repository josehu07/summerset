import sys
import os
import argparse
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib  # type: ignore
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "1dc"

EXPER_NAME = "adaptive"
PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]

MIN_HOST0_CPUS = 40
SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = 1

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"
SERVER_IFB = lambda r: f"ifb{r}"

NUM_REPLICAS = 5
NUM_CLIENTS = 16
BATCH_INTERVAL = 1
PUT_RATIO = 100

LENGTH_SECS = 75
SIZE_CHANGE_SECS = 15
ENV_CHANGE1_SECS = 30
ENV_CHANGE2_SECS = 45
ENV_CHANGE3_SECS = 60
PLOT_SECS_BEGIN = 5
PLOT_SECS_END = 70

VALUE_SIZES = [(0, 256 * 1024), (SIZE_CHANGE_SECS, 4096)]
VALUE_SIZES_PARAM = "/".join([f"{t}:{v}" for t, v in VALUE_SIZES])

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
NETEM_RATE_C = lambda _: 1
NETEM_RATE_D = lambda r: 1 if r < 3 else 0.1


def launch_cluster(remote0, base, repo, protocol, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--force_leader",
        "0",
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--use_veth",
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup():
    # print("Waiting for cluster setup...")
    # wait for 20 seconds to safely allow all nodes up
    # not relying on SSH-piped outputs here
    time.sleep(20)


def run_bench_clients(remote0, base, repo, protocol):
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
        "--skip_build",
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
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
    ]
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(remote0, base, repo, protocol):
    print(f"  {EXPER_NAME}  {protocol:<10s}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "Crossword":
        config += f"+b_to_d_threshold={0.2}"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(remote0, base, repo, protocol, config=config)
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol)

    # at some timepoint, change mean value size (handled by the clients)
    time.sleep(SIZE_CHANGE_SECS)
    print("    Changing mean value size...")

    # at some timepoint, change env
    time.sleep(ENV_CHANGE1_SECS - SIZE_CHANGE_SECS)
    print("    Changing env perf params...")
    utils.net.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_B,
        NETEM_JITTER_B,
        NETEM_RATE_B,
        involve_ifb=True,
        remote=remote0,
    )

    # at some timepoint, change env again
    time.sleep(ENV_CHANGE2_SECS - ENV_CHANGE1_SECS)
    print("    Changing env perf params...")
    utils.net.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_C,
        NETEM_JITTER_C,
        NETEM_RATE_C,
        involve_ifb=True,
        remote=remote0,
    )

    # at some timepoint, change env again
    time.sleep(ENV_CHANGE3_SECS - ENV_CHANGE2_SECS)
    print("    Changing env perf params...")
    utils.net.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_D,
        NETEM_JITTER_D,
        NETEM_RATE_D,
        involve_ifb=True,
        remote=remote0,
    )

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "ab") as fserr:
        fserr.write(serr)

    # revert env params to initial
    utils.net.set_all_tc_qdisc_netems(
        NUM_REPLICAS,
        SERVER_NETNS,
        SERVER_DEV,
        SERVER_IFB,
        NETEM_MEAN_A,
        NETEM_JITTER_A,
        NETEM_RATE_A,
        involve_ifb=True,
        remote=remote0,
    )

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(output_dir):
    results = dict()
    for protocol in PROTOCOLS:
        result = utils.output.gather_outputs(
            protocol,
            NUM_CLIENTS,
            output_dir,
            PLOT_SECS_BEGIN,
            PLOT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 20, 0, 0, 1
        if protocol == "Raft" or protocol == "CRaft":
            # due to an implementation choice, Raft clients see a spike of
            # "ghost" replies after env changes; removing it here
            sp = 50
        elif protocol == "Crossword":
            # setting sd here which avoids the lines to completely overlap with
            # each other
            # setting sm here to compensate for printing models to console
            sd, sm = 25, 1.1
        tput_list = utils.output.list_smoothing(result["tput_sum"], sd, sp, sj, sm)

        results[protocol] = {
            "time": result["time"],
            "tput": tput_list,
        }

    # do capping for other protocols to remove weird spikes/dips introduced by
    # changing netem parameters at runtime
    def result_cap(pa, pb, down):
        results[pa]["tput"] = utils.output.list_capping(
            results[pa]["tput"], results[pb]["tput"], 5, down=down
        )

    result_cap("CRaft", "RSPaxos", False)
    result_cap("CRaft", "RSPaxos", True)
    result_cap("MultiPaxos", "Raft", False)
    result_cap("MultiPaxos", "Raft", True)

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


def plot_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (5.6, 3),
            "font.size": 13,
            "pdf.fonttype": 42,
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

    draw_env_change_indicator(
        SIZE_CHANGE_SECS - PLOT_SECS_BEGIN - 2, "Data smaller", -7
    )
    draw_env_change_indicator(ENV_CHANGE1_SECS - PLOT_SECS_BEGIN - 2, "Bw drops", 8)
    draw_env_change_indicator(ENV_CHANGE2_SECS - PLOT_SECS_BEGIN - 2, "Bw frees", 10)
    draw_env_change_indicator(ENV_CHANGE3_SECS - PLOT_SECS_BEGIN - 2, "2 nodes lag", 20)

    # configuration indicators
    def draw_config_indicator(x, y, c, q, color):
        plt.annotate(
            f"[c={c},q={q}]",
            (x, y),
            xytext=(0, 0),
            ha="center",
            textcoords="offset points",
            color=color,
            fontsize=11,
        )

    draw_config_indicator(62, 460, 1, 4, "red")
    draw_config_indicator(32.5, 160, 3, 3, "forestgreen")
    draw_config_indicator(3.5, 1280, 1, 5, "steelblue")
    draw_config_indicator(16, 1630, 3, 3, "steelblue")
    draw_config_indicator(32.5, 1100, 1, 5, "steelblue")
    draw_config_indicator(62, 1350, 3, 3, "steelblue")

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.plot(1, -1, ">k", transform=ax.get_yaxis_transform(), clip_on=False)

    plt.ylim(bottom=-1, top=ymax * 1.15)

    plt.xlabel("Time (s)")
    plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
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

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default=f"./results",
        help="directory to hold outputs and logs",
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        raise RuntimeError(f"results directory {args.odir} does not exist")

    if not args.plot:
        print("Doing preparation work...")
        base, repo, _, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )
        utils.proc.check_enough_cpus(MIN_HOST0_CPUS, remote=remotes["host0"])
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
        utils.file.do_cargo_build(True, remotes=remotes)
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        print("Setting tc netem qdiscs...")
        utils.net.set_all_tc_qdisc_netems(
            NUM_REPLICAS,
            SERVER_NETNS,
            SERVER_DEV,
            SERVER_IFB,
            NETEM_MEAN_A,
            NETEM_JITTER_A,
            NETEM_RATE_A,
            involve_ifb=True,
            remote=remotes["host0"],
        )

        print("Running experiments...")
        for protocol in PROTOCOLS:
            bench_round(remotes["host0"], base, repo, protocol)
            utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
            utils.file.remove_files_in_dir(  # to free up storage space
                f"{base}/states/{EXPER_NAME}",
                remotes=remotes,
            )
            utils.file.clear_fs_caches(remotes=remotes)

        print("Clearing tc netem qdiscs...")
        utils.net.clear_all_tc_qdisc_netems(
            NUM_REPLICAS, SERVER_NETNS, SERVER_DEV, SERVER_IFB, remote=remotes["host0"]
        )

        print("Fetching client output logs...")
        utils.file.fetch_files_of_dir(
            remotes["host0"], f"{base}/output/{EXPER_NAME}", output_path
        )

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_results(results, plots_dir)
        plot_legend(handles, labels, plots_dir)
