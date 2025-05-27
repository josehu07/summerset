import sys
import os
import argparse
import time
import math

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "reg"

EXPER_NAME = "bw_utils"
PROTOCOLS = ["MultiPaxos", "Crossword", "CrosswordNoBatch", "CrosswordShards2"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 30
BATCH_INTERVAL = 1
VALUE_SIZE = 64 * 1024
PUT_RATIO = 100

LENGTH_SECS = 30

BASE_LINK_RATE = 1 / 2


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

    real_protocol = protocol
    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += f"+record_breakdown=true"
    config += f"+record_size_recv=true"
    # if "Crossword" in protocol:
    #     config += f"+gossip_timeout_min=40"
    #     config += f"+gossip_timeout_max=60"
    if protocol == "Crossword":
        config += f"+init_assignment='1'"
        config += f"+gossip_batch_size=100"
    elif protocol == "CrosswordNoBatch":
        real_protocol = "Crossword"
        config += f"+init_assignment='1'"
        config += f"+gossip_batch_size=1"
    elif protocol == "CrosswordShards2":
        real_protocol = "Crossword"
        config += f"+init_assignment='2'"
        config += f"+gossip_batch_size=100"

    # launch service cluster
    proc_cluster = launch_cluster(remote0, base, repo, real_protocol, config=config)
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, real_protocol)

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


def collect_bw_utils(runlog_dir):
    bw_utils = dict()
    for protocol in PROTOCOLS:
        l_f_utils, f_f_utils = [], []

        with open(f"{runlog_dir}/{protocol}.s.err", "r") as flog:
            for line in flog:
                if "bw period" in line:
                    line = line.strip()
                    recver = int(line[line.find("(") + 1 : line.find(")")])
                    sender = int(line[line.find("<- ") + 3 : line.rfind(":")])
                    nbytes = int(line[line.rfind(":") + 1 :])

                    if recver == 0 or sender == 0:
                        l_f_utils.append(nbytes)
                    else:
                        f_f_utils.append(nbytes)

        # take average of middle part
        l_f_utils = l_f_utils[len(l_f_utils) // 2 : -len(l_f_utils) // 4]
        f_f_utils = f_f_utils[len(f_f_utils) // 2 : -len(f_f_utils) // 4]
        l_f_nbytes = 0 if len(l_f_utils) == 0 else sum(l_f_utils) / len(l_f_utils)
        f_f_nbytes = 0 if len(f_f_utils) == 0 else sum(f_f_utils) / len(f_f_utils)
        bw_utils[protocol] = {
            "L-F": l_f_nbytes,
            "F-F": f_f_nbytes,
        }

    # account for Crossword message size profiling inaccuracies
    delta = bw_utils["MultiPaxos"]["L-F"] // math.ceil(NUM_REPLICAS / 2)
    delta -= bw_utils["Crossword"]["L-F"]
    bw_utils["Crossword"]["L-F"] += delta
    bw_utils["Crossword"]["F-F"] -= delta

    for protocol in bw_utils:
        l_f_nbytes, f_f_nbytes = bw_utils[protocol]["L-F"], bw_utils[protocol]["F-F"]
        l_f_percentage = 100 * l_f_nbytes / (BASE_LINK_RATE * (10**9))
        f_f_percentage = 100 * f_f_nbytes / (BASE_LINK_RATE * (10**9))
        bw_utils[protocol] = {
            "L-F": (l_f_nbytes, l_f_percentage),
            "F-F": (f_f_nbytes, f_f_percentage),
        }

    return bw_utils


def print_results(bw_utils):
    for protocol in bw_utils:
        print(protocol)
        print(
            f"  L-F: {bw_utils[protocol]['L-F'][0]:10.0f} {bw_utils[protocol]['L-F'][1]:3.0f}%"
        )
        print(
            f"  F-F: {bw_utils[protocol]['F-F'][0]:10.0f} {bw_utils[protocol]['F-F'][1]:3.0f}%"
        )


def plot_bw_utils(bw_utils, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.6, 1.7),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        # "CrosswordShards2",
        "CrosswordNoBatch",
        "Crossword",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        # "CrosswordShards2": ("Cw (c=2)", "lightgreen", "//"),
        "CrosswordNoBatch": ("Cw (no batch)", "steelblue", ".."),
        "Crossword": ("Cw (default)", "lightsteelblue", "xx"),
    }

    # L-F
    ax1 = plt.subplot(211)

    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        util = bw_utils[f"{protocol}"]["L-F"][1]
        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            util,
            width=1,
            color=color,
            edgecolor="black",
            linewidth=1.2,
            label=label,
            hatch=hatch,
            # yerr=result["stdev"],
            # ecolor="black",
            # capsize=1,
        )

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.ylim((0, 60))
    plt.yticks([0, 30, 60], ["0%", "30%", "60%"])
    plt.tick_params(bottom=False, labelbottom=False)

    plt.xlim((0, len(PROTOCOLS_ORDER) + 1))
    plt.xlabel("L-F")

    # F-F
    ax2 = plt.subplot(212)

    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        util = bw_utils[f"{protocol}"]["F-F"][1]
        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            util,
            width=1,
            color=color,
            edgecolor="black",
            linewidth=1.2,
            label=label,
            hatch=hatch,
            # yerr=result["stdev"],
            # ecolor="black",
            # capsize=1,
        )

    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.ylim((0, 60))
    plt.yticks([0, 30, 60], ["0%", "30%", "60%"])
    plt.tick_params(bottom=False, labelbottom=False)

    plt.xlim((0, len(PROTOCOLS_ORDER) + 1))
    plt.xlabel("F-F")

    plt.tight_layout(h_pad=0.0)

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 1.6),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handlelength=1.0,
        handleheight=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

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
        os.system(f"mkdir -p {args.odir}")

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

        bw_utils = collect_bw_utils(runlog_dir)
        print_results(bw_utils)

        handles, labels = plot_bw_utils(bw_utils, plots_dir)
        plot_legend(handles, labels, plots_dir)
