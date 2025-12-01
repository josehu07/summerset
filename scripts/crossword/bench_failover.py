import sys
import os
import argparse
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


PHYS_ENV_GROUP = "reg"

EXPER_NAME = "failover"
PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
CLIENT_TIMEOUT_SECS = 2
VALUE_SIZE = 64 * 1024
PUT_RATIO = 100

LENGTH_SECS = 120
FAIL1_SECS = 40
FAIL2_SECS = 80
PLOT_SECS_BEGIN = 25
PLOT_SECS_END = 115


def launch_cluster(remote0, base, repo, protocol, config=None):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-r",
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
        "--timeout_ms",
        str(CLIENT_TIMEOUT_SECS * 1000),
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
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
    ]
    if protocol == "RSPaxos":
        cmd.append("--expect_halt")

    print("    Running benchmark clients...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def run_mess_client(remote0, base, repo, protocol, pauses=None, resumes=None):
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
        "--skip_build",
        "mess",
    ]
    if pauses is not None and len(pauses) > 0:
        cmd += ["--pause", pauses]
    if resumes is not None and len(resumes) > 0:
        cmd += ["--resume", resumes]
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
    if protocol == "Crossword":
        config += "+init_assignment='1'"

    # launch service cluster
    proc_cluster = launch_cluster(remote0, base, repo, protocol, config=config)
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol)

    # at the first failure point, pause current leader
    time.sleep(FAIL1_SECS)
    print("    Pausing current leader...")
    run_mess_client(remote0, base, repo, protocol, pauses="l")

    # at the second failure point, pause current leader
    time.sleep(FAIL2_SECS - FAIL1_SECS)
    print("    Pausing current leader...")
    run_mess_client(remote0, base, repo, protocol, pauses="l")

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

        sd, sp, sj, sm = 10, 0, 0, 1
        if protocol == "Crossword":
            # due to limited sampling granularity, Crossword gossiping makes
            # throughput results look a bit more "jittering" than it actually
            # is after failover; smoothing a bit more here
            # setting sd here also avoids the lines to completely overlap with
            # each other
            sd, sj = 15, 50
        tput_list = utils.output.list_smoothing(
            result["tput_sum"], sd, sp, sj, sm
        )

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


def plot_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (6, 3),
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
        "CRaft": ("CRaft (f=1, fb=ok)", "peru", ":", 1.5),
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
            x - 1,
            ymax + 42,
            0,
            -40,
            color="darkred",
            width=0.2,
            length_includes_head=True,
            head_width=1.0,
            head_length=12,
            overhang=0.5,
            clip_on=False,
        )
        plt.annotate(
            t,
            (x - 1, ymax + 60),
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
            head_width=8,
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
            head_width=8,
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

    draw_recovery_indicator(18.8, 500, 3.6, "small\ngossip\ngap", 0.8, 30)
    draw_recovery_indicator(58.5, 500, 3.6, "small\ngossip\ngap", 0.8, 30)

    plt.vlines(
        63,
        400,
        520,
        colors="gray",
        linestyles="solid",
        linewidth=0.8,
    )

    # draw_recovery_indicator(23, 50, 7, None, None, None)
    # draw_recovery_indicator(25.2, 62, 9.2, "state-send\nsnapshot int.", 4.6, -53)
    # draw_recovery_indicator(67.5, 56, 11.5, "state-send\nsnapshot int.", 2.9, -47)

    # configuration indicators
    def draw_config_indicator(x, y, c, q, color, fb=False, unavail=False):
        t = f"[c={c},q={q}]"
        if fb:
            t += "\nfb=ok"
        if unavail:
            t = "\nunavail."
        plt.annotate(
            t,
            (x, y),
            xytext=(0, 0),
            ha="center",
            textcoords="offset points",
            color=color,
            fontsize=11,
        )

    draw_config_indicator(4.6, 750, 1, 4, "red")
    draw_config_indicator(4.6, 660, 1, 5, "steelblue")
    draw_config_indicator(4.6, 570, 1, 4, "peru")
    draw_config_indicator(4.6, 280, 3, 3, "forestgreen")

    draw_config_indicator(44.2, 750, 1, 4, "red")
    draw_config_indicator(44.2, 425, 2, 4, "steelblue")
    draw_config_indicator(45.5, 50, 3, 3, "peru", fb=True)

    draw_config_indicator(88.5, 420, 3, 3, "steelblue")
    draw_config_indicator(88.5, 25, 1, 4, "red", unavail=True)

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
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        try:
            print("Running experiments...")
            for protocol in PROTOCOLS:
                time.sleep(3)
                bench_round(remotes["host0"], base, repo, protocol, runlog_path)
                utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                utils.file.remove_files_in_dir(  # to free up storage space
                    f"{base}/states/{EXPER_NAME}",
                    remotes=remotes,
                )
                utils.file.clear_fs_caches(remotes=remotes)

        except utils.BreakingLoops:
            print("Experiment FAILED, breaking early...")

        print("Fetching client output logs...")
        for remote in remotes.values():
            utils.file.fetch_files_of_dir(
                remote, f"{base}/output/{EXPER_NAME}", output_path
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
