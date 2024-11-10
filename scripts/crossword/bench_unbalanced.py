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
PHYS_ENV_GROUP = "reg"

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

RS_TOTAL_SHARDS = 15
RS_DATA_SHARDS = 9

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
VALUE_SIZE = 64 * 1024
PUT_RATIO = 100

LENGTH_SECS = 20
RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 15

NETEM_MEAN = lambda _: 0
NETEM_JITTER = lambda _: 0
NETEM_RATE = lambda r: 1 if r < 3 else 0.4 if r < 4 else 0.1


def round_midfix_str(fault_tolerance, init_assignment):
    return (
        f".{fault_tolerance}."
        + f"{'b' if init_assignment is None or len(init_assignment) == 1 else 'u'}"
    )


def launch_cluster(remote0, base, repo, protocol, midfix_str, config=None):
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
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--file_midfix",
        midfix_str,
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


def run_bench_clients(remote0, base, repo, protocol, midfix_str):
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
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        midfix_str,
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


def bench_round(
    remote0, base, repo, protocol, fault_tolerance, init_assignment, runlog_path
):
    midfix_str = round_midfix_str(fault_tolerance, init_assignment)
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "RSPaxos" or protocol == "CRaft":
        config += f"+fault_tolerance={fault_tolerance}"
    elif protocol == "Crossword":
        config += f"+rs_total_shards={RS_TOTAL_SHARDS}"
        config += f"+rs_data_shards={RS_DATA_SHARDS}"
        config += f"+init_assignment='{init_assignment}'"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, protocol, midfix_str, config=config
    )
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol, midfix_str)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating Summerset cluster...")
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Bench round FAILED!")
        sys.exit(1)
    else:
        print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for protocol, fault_tolerance, init_assignment in PROTOCOL_FT_ASSIGNS:
        midfix_str = round_midfix_str(fault_tolerance, init_assignment)
        result = utils.output.gather_outputs(
            f"{protocol}{midfix_str}",
            NUM_CLIENTS,
            output_dir,
            RESULT_SECS_BEGIN,
            RESULT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 10, 0, 0, 1
        tput_mean_list = utils.output.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
        tput_stdev_list = result["tput_stdev"]

        results[f"{protocol}{midfix_str}"] = {
            "mean": sum(tput_mean_list) / len(tput_mean_list),
            "stdev": (sum(map(lambda s: s**2, tput_stdev_list)) / len(tput_stdev_list))
            ** 0.5,
        }

    return results


def print_results(results):
    for protocol_with_midfix, result in results.items():
        print(protocol_with_midfix)
        print(f"  mean {result['mean']:7.2f}  stdev {result['stdev']:7.2f}")


def plot_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.5, 1.8),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos.2.b",
        "Raft.2.b",
        "Crossword.2.b",
        "Crossword.2.u",
        "RSPaxos.2.b",
        "CRaft.2.b",
        "RSPaxos.1.b",
        "CRaft.1.b",
    ]
    PROTOCOLS_XPOS = {
        "MultiPaxos.2.b": 1,
        "Raft.2.b": 2,
        "Crossword.2.b": 3,
        "Crossword.2.u": 4,
        "RSPaxos.2.b": 5,
        "CRaft.2.b": 6,
        "RSPaxos.1.b": 8,
        "CRaft.1.b": 9,
    }
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos.2.b": ("MultiPaxos", "darkgray", None),
        "Raft.2.b": ("Raft", "lightgreen", None),
        "Crossword.2.b": ("Cw (bal.)", "lightsteelblue", "xx"),
        "Crossword.2.u": ("Cw (unbal.)", "cornflowerblue", ".."),
        "RSPaxos.2.b": ("RSPaxos*", "salmon", "//"),
        "CRaft.2.b": ("CRaft*", "wheat", "\\\\"),
        "RSPaxos.1.b": ("RSPaxos", "pink", "//"),
        "CRaft.1.b": ("CRaft", "cornsilk", "\\\\"),
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

    plt.xticks([3.5, 8.5], ["f=2", "f=1"])
    plt.tick_params(bottom=False)

    plt.ylabel("Tput. (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 1.6),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    # handles.insert(2, matplotlib.lines.Line2D([], [], linestyle=""))
    # labels.insert(2, "")
    lgd = plt.legend(
        handles,
        labels,
        handlelength=1.0,
        handleheight=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=2,
        columnspacing=0.6,
    )
    for rec in lgd.get_texts():
        if rec.get_text() == "RSPaxos" or rec.get_text() == "CRaft":
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
        base, repo, hosts, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )
        hosts = hosts[:NUM_REPLICAS]
        remotes = {h: remotes[h] for h in hosts}

        utils.proc.check_enough_cpus(MIN_HOST0_CPUS, remote=remotes["host0"])
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
        utils.file.do_cargo_build(True, cd_dir=f"{base}/{repo}", remotes=remotes)
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        print("Setting tc netem qdiscs...")
        utils.net.set_tc_qdisc_netems_main(
            NETEM_MEAN,
            NETEM_JITTER,
            NETEM_RATE,
            involve_ifb=True,
            remotes=remotes,
        )

        print("Running experiments...")
        for protocol, fault_tolerance, init_assignment in PROTOCOL_FT_ASSIGNS:
            time.sleep(3)
            bench_round(
                remotes["host0"],
                base,
                repo,
                protocol,
                fault_tolerance,
                init_assignment,
                runlog_path,
            )
            utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
            utils.file.remove_files_in_dir(  # to free up storage space
                f"{base}/states/{EXPER_NAME}",
                remotes=remotes,
            )
            utils.file.clear_fs_caches(remotes=remotes)

        print("Clearing tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(remotes=remotes)

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
