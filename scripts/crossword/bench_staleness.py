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

EXPER_NAME = "staleness"
PROTOCOLS = ["Crossword"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
VALUE_SIZE = 64 * 1024
PUT_RATIO = 50

LENGTH_SECS = 60
RESULT_SECS_END = 55


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
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
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
        "./scripts/distr_clients.py",
        "-p",
        protocol,
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--base_idx",
        str(0),
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
        "-k",
        "10",
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


def bench_round(remote0, base, repo, protocol, runlog_path):
    print(f"  {EXPER_NAME}  {protocol:<10s}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += f"+init_assignment='1'"
    config += f"+record_breakdown=true"
    config += f"+record_value_ver=true"

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
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_ver_stats(runlog_dir):
    protocol = "Crossword"
    leader = None
    ver_stats = [{"secs": [], "vers": []} for _ in range(NUM_REPLICAS)]

    def get_node_id(line):
        return int(line[line.index("(") + 1 : line.index(")")])

    candidates = set(range(NUM_REPLICAS))
    with open(f"{runlog_dir}/{protocol}.s.err", "r") as flog:
        for line in flog:
            if "becoming a leader" in line:
                if leader is not None:
                    raise RuntimeError("multiple leader step-up detected")
                leader = get_node_id(line)
            elif "ver of" in line:
                node = get_node_id(line)
                if node not in candidates:
                    continue
                segs = line.strip().split()
                sec = float(segs[-4]) / 1000.0
                ver = int(segs[-1])
                ver_stats[node]["secs"].append(sec)
                ver_stats[node]["vers"].append(ver)
                if sec > RESULT_SECS_END:
                    candidates.remove(node)

    if leader is None:
        raise RuntimeError("leader step-up not detected")
    return leader, ver_stats


def print_results(leader, ver_stats):
    assert leader >= 0 and leader < len(ver_stats)
    for node in range(len(ver_stats)):
        print(node, f"{'leader' if node == leader else 'follower':<8s}")
        print("  secs", end="")
        for sec in ver_stats[node]["secs"]:
            print(f" {sec:>5.1f}", end="")
        print()
        print("  vers", end="")
        for ver in ver_stats[node]["vers"]:
            print(f" {ver:>5d}", end="")
        print()


def plot_staleness(leader, ver_stats, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2, 2),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    ROLE_XS_YS = {
        "Leader": (ver_stats[leader]["secs"], ver_stats[leader]["vers"]),
        "Follower": (
            ver_stats[(leader + 1) % NUM_REPLICAS]["secs"],
            ver_stats[(leader + 1) % NUM_REPLICAS]["vers"],
        ),
        "RSPaxos": (
            ver_stats[leader]["secs"],
            [0 for _ in range(len(ver_stats[leader]["secs"]))],
        ),
    }
    ROLE_LABEL_COLOR_MARKER_SIZE_ZORDER = {
        "Leader": ("Leader ≈\nMultiPaxos follower", "orange", "v", 5, 10),
        "Follower": ("Crossword follower", "steelblue", "o", 5, 0),
        "RSPaxos": ("RSPaxos follower", "red", "x", 5, 0),
    }

    for role in ROLE_XS_YS:
        xs, ys = ROLE_XS_YS[role]
        label, color, marker, markersize, zorder = ROLE_LABEL_COLOR_MARKER_SIZE_ZORDER[
            role
        ]
        plt.plot(
            xs,
            ys,
            color=color,
            linewidth=1.2,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=zorder,
        )

    plt.text(
        25,
        200,
        "version diff.\n≤ 2",
        verticalalignment="center",
        horizontalalignment="center",
        color="dimgray",
        fontsize=8,
    )
    plt.plot(
        [
            26,
            31,
        ],
        [172, 155],
        color="dimgray",
        linestyle="-",
        linewidth=0.6,
    )

    plt.text(
        42,
        62,
        " stale read\ninfeasible\nat followers",
        verticalalignment="center",
        horizontalalignment="center",
        color="dimgray",
        fontsize=8,
    )
    plt.plot(
        [
            42,
            42,
        ],
        [22, 11],
        color="dimgray",
        linestyle="-",
        linewidth=0.6,
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlabel("Time (secs)")

    plt.ylim(bottom=-1)
    plt.ylabel("Value version")

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2, 1),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handlelength=1.2,
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
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        print("Running experiments...")
        for protocol in PROTOCOLS:
            time.sleep(10)
            bench_round(remotes["host0"], base, repo, protocol, runlog_path)
            utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
            utils.file.clear_fs_caches(remotes=remotes)

    else:
        runlog_dir = f"{args.odir}/runlog/{EXPER_NAME}"
        # states_dir = f"{args.odir}/states/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        leader, ver_stats = collect_ver_stats(runlog_dir)
        print_results(leader, ver_stats)

        handles, labels = plot_staleness(leader, ver_stats, plots_dir)
        plot_legend(handles, labels, plots_dir)
