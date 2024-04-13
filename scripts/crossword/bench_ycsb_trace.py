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

EXPER_NAME = "ycsb_trace"
SUMMERSET_PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]
CHAIN_PROTOCOLS = ["chain_delayed", "chain_mixed"]

YCSB_DIR = lambda base: f"{base}/ycsb"
YCSB_TRACE = "/tmp/ycsb_workloada.txt"

NUM_REPLICAS = 5
NUM_CLIENTS_LIST = list(range(1, 6))
BATCH_INTERVAL = 1
VALUE_SIZE = 64 * 1024
PUT_RATIO = 50  # YCSB-A has 50% updates + 50% reads

LENGTH_SECS = 35
RESULT_SECS_BEGIN = 10
RESULT_SECS_END = 30


def gen_ycsb_a_trace(base):
    # TODO: do on remote
    cmd = [
        f"{YCSB_DIR(base)}/bin/ycsb.sh",
        "run",
        "basic",
        "-P",
        f"{YCSB_DIR(base)}/workloads/workloada",
    ]
    proc = utils.proc.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )
    out, _ = proc.communicate()
    raw = out.decode()

    # clean the trace
    # TODO: check #keys; pick value size according to dist
    with open(YCSB_TRACE, "w+") as fout:
        for line in raw.strip().split("\n"):
            line = line.strip()
            if line.startswith("READ ") or line.startswith("UPDATE "):
                segs = line.split()
                op = segs[0]
                key = segs[2]
                fout.write(f"{op} {key}\n")


def launch_cluster_summerset(
    remote, base, repo, protocol, partition, num_clients, config=None
):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        protocol,
        "-a",
        str(partition),
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--force_leader",
        str(partition),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{partition}",
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--file_midfix",
        f".{partition}.{num_clients}",
        # NOTE: not pinning cores for this exper due to large #processes
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return utils.proc.run_process_over_ssh(
        remote,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup_summerset():
    # print("Waiting for cluster setup...")
    # wait for 20 seconds to safely allow all nodes up
    # not relying on SSH-piped outputs here
    time.sleep(20)


def run_bench_clients_summerset(remote, base, repo, protocol, partition, num_clients):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        protocol,
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{partition}",
        # NOTE: not pinning cores for this exper due to large #processes
        "--base_idx",
        str(0),
        "--skip_build",
        "bench",
        "-a",
        str(partition),
        "-n",
        str(num_clients),
        # NOTE: not distributing clients of this partition to other nodes,
        #       so the behavior mathces ChainPaxos's multithreading client
        "-f",
        str(0),  # closed-loop
        "-y",
        YCSB_TRACE,
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{partition}.{num_clients}",
    ]
    return utils.proc.run_process_over_ssh(
        remote,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round_summerset(remotes, base, repo, protocol, num_clients, runlog_path):
    print(f"  {EXPER_NAME}  {protocol:<10s}.{num_clients}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += f"+sim_read_lease=true"
    if protocol == "RSPaxos" or protocol == "CRaft":
        config += f"+fault_tolerance=2"
    if protocol == "Crossword":
        config += f"+init_assignment='1'"
        # config += f"+disable_gossip_timer=true"  # TODO: maybe?

    # launch service clusters for each partition
    procs_cluster = []
    for partition in range(NUM_REPLICAS):
        procs_cluster.append(
            launch_cluster_summerset(
                remotes[f"host{partition}"],
                base,
                repo,
                protocol,
                partition,
                num_clients,
                config=config,
            )
        )
    wait_cluster_setup_summerset()

    # start benchmarking clients for each partition
    procs_clients = []
    for partition in range(NUM_REPLICAS):
        procs_clients.append(
            run_bench_clients_summerset(
                remotes[f"host{partition}"],
                base,
                repo,
                protocol,
                partition,
                num_clients,
            )
        )

    # wait for benchmarking clients to exit
    for partition in range(NUM_REPLICAS):
        _, cerr = procs_clients[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{partition}.{num_clients}.c.err", "wb"
        ) as fcerr:
            fcerr.write(cerr)

    # terminate the clusters
    for partition in range(NUM_REPLICAS):
        procs_cluster[partition].terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=False)
    for partition in range(NUM_REPLICAS):
        _, serr = procs_cluster[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{partition}.{num_clients}.s.err", "wb"
        ) as fserr:
            fserr.write(serr)

    if any(map(lambda p: p.returncode != 0, procs_clients)):
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def launch_cluster_chain(remote, base, repo, protocol, partition, num_clients):
    cmd = [
        "python3",
        "./scripts/crossword/distr_chainapp.py",
        "-p",
        protocol,
        "-a",
        str(partition),
        "-n",
        str(NUM_REPLICAS),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--file_midfix",
        f".{partition}.{num_clients}",
        # NOTE: not pinning cores for this exper due to large #processes
    ]
    return utils.proc.run_process_over_ssh(
        remote,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup_chain():
    # print("Waiting for cluster setup...")
    # wait for 20 seconds to safely allow all nodes up
    # not relying on SSH-piped outputs here
    time.sleep(20)


def run_bench_clients_chain(remote, base, repo, protocol, partition, num_clients):
    cmd = [
        "python3",
        "./scripts/crossword/distr_chaincli.py",
        "-p",
        protocol,
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        # NOTE: not pinning cores for this exper due to large #processes
        "-a",
        str(partition),
        "-n",
        str(num_clients),
        "-v",
        str(VALUE_SIZE),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{partition}.{num_clients}",
    ]
    return utils.proc.run_process_over_ssh(
        remote,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round_chain(remotes, base, repo, protocol, num_clients, runlog_path):
    print(f"  {EXPER_NAME}  {protocol:<10s}.{num_clients}")

    # launch service clusters for each partition
    procs_cluster = []
    for partition in range(NUM_REPLICAS):
        procs_cluster.append(
            launch_cluster_chain(
                remotes[f"host{partition}"],
                base,
                repo,
                protocol,
                partition,
                num_clients,
            )
        )
    wait_cluster_setup_chain()

    # start benchmarking clients for each partition
    procs_clients = []
    for partition in range(NUM_REPLICAS):
        procs_clients.append(
            run_bench_clients_chain(
                remotes[f"host{partition}"],
                base,
                repo,
                protocol,
                partition,
                num_clients,
            )
        )

    # wait for benchmarking clients to exit
    for partition in range(NUM_REPLICAS):
        _, cerr = procs_clients[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{partition}.{num_clients}.c.err", "wb"
        ) as fcerr:
            fcerr.write(cerr)

    # terminate the clusters
    for partition in range(NUM_REPLICAS):
        procs_cluster[partition].terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=True)
    for partition in range(NUM_REPLICAS):
        _, serr = procs_cluster[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{partition}.{num_clients}.s.err", "wb"
        ) as fserr:
            fserr.write(serr)

    if any(map(lambda p: p.returncode != 0, procs_clients)):
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(odir):
    results = dict()

    for protocol in SUMMERSET_PROTOCOLS:
        result = utils.output.gather_outputs(
            protocol,
            NUM_CLIENTS,
            odir,
            RESULT_SECS_BEGIN,
            RESULT_SECS_END,
            0.1,
        )

        sd, sp, sj, sm = 20, 0, 0, 1
        if protocol == "Crossword":
            # setting sm here to compensate for printing models to console
            sm = 1.1
        tput_mean_list = utils.output.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
        tput_stdev_list = result["tput_stdev"]
        lat_mean_list = utils.output.list_smoothing(
            result["lat_avg"], sd, sp, sj, 1 / sm
        )
        lat_stdev_list = result["lat_stdev"]

        results[protocol] = {
            "tput": {
                "mean": tput_mean_list,
                "stdev": tput_stdev_list,
            },
            "lat": {
                "mean": lat_mean_list,
                "stdev": lat_stdev_list,
            },
        }

    # do capping for other protocols to remove weird spikes/dips introduced
    # by the TCP stack
    def result_cap(pa, pb, down):
        for metric in ("tput", "lat"):
            for stat in ("mean", "stdev"):
                results[f"{pa}"][metric][stat] = utils.output.list_capping(
                    results[f"{pa}"][metric][stat],
                    results[f"{pb}"][metric][stat],
                    5,
                    down=down if metric == "tput" else not down,
                )

    result_cap("RSPaxos", "CRaft", False)
    result_cap("RSPaxos", "CRaft", True)
    result_cap("MultiPaxos", "Raft", False)
    result_cap("MultiPaxos", "Raft", True)

    for protocol in SUMMERSET_PROTOCOLS:
        if protocol in results:
            tput_mean_list = results[protocol]["tput"]["mean"]
            tput_stdev_list = results[protocol]["tput"]["stdev"]
            lat_mean_list = results[protocol]["lat"]["mean"]
            lat_stdev_list = results[protocol]["lat"]["stdev"]

            results[protocol] = {
                "tput": {
                    "mean": sum(tput_mean_list) / len(tput_mean_list),
                    "stdev": (
                        sum(map(lambda s: s**2, tput_stdev_list)) / len(tput_stdev_list)
                    )
                    ** 0.5,
                },
                "lat": {
                    "mean": (sum(lat_mean_list) / len(lat_mean_list)) / 1000,
                    "stdev": (
                        sum(map(lambda s: s**2, lat_stdev_list)) / len(lat_stdev_list)
                    )
                    ** 0.5
                    / (1000 * NUM_CLIENTS / SERVER_PIN_CORES),
                },
            }

    for protocol in CHAIN_PROTOCOLS:
        results[protocol] = utils.output.parse_ycsb_log(
            protocol,
            odir,
            1,
            1,
        )
        results[protocol]["tput"]["stdev"] /= NUM_CLIENTS / SERVER_PIN_CORES
        results[protocol]["lat"]["stdev"] /= NUM_CLIENTS / SERVER_PIN_CORES

    return results


def print_results(results):
    for protocol, result in results.items():
        print(protocol)
        print(
            f"  tput  mean {result['tput']['mean']:7.2f}  stdev {result['tput']['stdev']:7.2f}"
            + f"  lat  mean {result['lat']['mean']:7.2f}  stdev {result['lat']['stdev']:7.2f}"
        )


def plot_results(results, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4.5, 2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Raft",
        "Crossword",
        "RSPaxos",
        "CRaft",
        "chain_delayed",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Raft": ("Raft", "lightgreen", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "RSPaxos": ("RSPaxos", "salmon", "//"),
        "CRaft": ("CRaft", "wheat", "\\\\"),
        "chain_delayed": ("ChainPaxos", "plum", "--"),
    }

    ax1 = plt.subplot(121)
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol]["tput"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
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

    plt.tick_params(bottom=False, labelbottom=False)
    plt.ylabel("Throughput (reqs/s)")

    ax2 = plt.subplot(122)
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol]["lat"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            result["mean"],
            width=1,
            color=color,
            edgecolor="gray",
            linewidth=1.4,
            hatch=hatch,
            yerr=result["stdev"],
            ecolor="black",
            capsize=1,
        )

    plt.tick_params(bottom=False, labelbottom=False)
    plt.ylabel("Latency (ms)")

    for ax in fig.axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.tight_layout()

    pdf_name = f"{odir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_legend(handles, labels, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2, 2),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handleheight=0.9,
        handlelength=1.3,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

    pdf_name = f"{odir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-t", "--trace", action="store_true", help="if set, do YCSB trace generation"
    )
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

    if args.trace:
        print("Generating YCSB-A trace...")
        base, _, _, _, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )
        if os.path.isfile(YCSB_TRACE):
            print(f"  {YCSB_TRACE} already there, skipped")
        else:
            gen_ycsb_a_trace(base)
            print(f"  Done: {YCSB_TRACE}")

    elif not args.plot:
        print("Doing preparation work...")
        base, repo, hosts, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )
        hosts = hosts[:NUM_REPLICAS]
        remotes = {h: remotes[h] for h in hosts}

        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=False)
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=True)
        utils.file.do_cargo_build(True, cd_dir=f"{base}/{repo}", remotes=remotes)
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        for num_clients in NUM_CLIENTS_LIST:
            print(f"Running experiments {num_clients}...")

            PROTOCOL_FUNCS = [(p, bench_round_summerset) for p in SUMMERSET_PROTOCOLS]
            PROTOCOL_FUNCS += [(p, bench_round_chain) for p in CHAIN_PROTOCOLS]
            for protocol, bench_round_func in PROTOCOL_FUNCS:
                time.sleep(10)
                bench_round_func(
                    remotes, base, repo, protocol, num_clients, runlog_path
                )
                utils.proc.kill_all_distr_procs(
                    PHYS_ENV_GROUP, chain=(protocol in CHAIN_PROTOCOLS)
                )
                utils.file.remove_files_in_dir(  # to free up storage space
                    f"{base}/states/{EXPER_NAME}",
                    remotes=remotes,
                )
                utils.file.clear_fs_caches(remotes=remotes)

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_results(results, plots_dir)
        plot_legend(handles, labels, plots_dir)
