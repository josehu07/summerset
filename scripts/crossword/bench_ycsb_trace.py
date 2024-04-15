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

GEN_YCSB_SCRIPT = "crossword/gen_ycsb_a_trace.py"
YCSB_TRACE = "/tmp/ycsb_workloada.txt"

NUM_REPLICAS = 5
NUM_CLIENTS_LIST = list(range(1, 11))
BATCH_INTERVAL = 1
PUT_RATIO = 50  # YCSB-A has 50% updates + 50% reads

LENGTH_SECS = 60
RESULT_SECS_BEGIN = 10
RESULT_SECS_END = 50

SIZE_S = 8
SIZE_L = 128 * 1024
SIZE_M = 64 * 1024
SIZE_MIXED = [
    (0, SIZE_L),
    (LENGTH_SECS // 6, SIZE_S),
    ((LENGTH_SECS // 6) * 2, SIZE_L),
    ((LENGTH_SECS // 6) * 3, SIZE_S),
    ((LENGTH_SECS // 6) * 4, SIZE_L),
    ((LENGTH_SECS // 6) * 5, SIZE_S),
]
SIZE_MIXED = "/".join([f"{t}:{v}" for t, v in SIZE_MIXED])

NETEM_MEAN = lambda _: 1
NETEM_JITTER = lambda _: 2
NETEM_RATE = lambda r: 1  # no effect given the original bandwidth


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
        f".{num_clients}",
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
        "--man",
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
        #       so the behavior matches ChainPaxos's multithreading client
        "-f",
        str(0),  # closed-loop
        "-y",
        YCSB_TRACE,
        "-v",
        SIZE_MIXED,
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{num_clients}",
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
        config += f"+b_to_d_threshold={0.08}"  # TODO: tune this
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
            f"{runlog_path}/{protocol}.{num_clients}.{partition}.c.err", "wb"
        ) as fcerr:
            fcerr.write(cerr)

    # terminate the clusters
    for partition in range(NUM_REPLICAS):
        procs_cluster[partition].terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=False)
    for partition in range(NUM_REPLICAS):
        _, serr = procs_cluster[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{num_clients}.{partition}.s.err", "wb"
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
        f"host{partition}",
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--file_midfix",
        f".{num_clients}",
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
        "-n",
        str(NUM_REPLICAS),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{partition}",
        # NOTE: not pinning cores for this exper due to large #processes
        "-a",
        str(partition),
        "-t",
        str(num_clients),
        "-v",
        str(SIZE_M),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{num_clients}",
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
    print(f"  {EXPER_NAME}  {protocol:<13s}.{num_clients}")

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
            f"{runlog_path}/{protocol}.{num_clients}.{partition}.c.err", "wb"
        ) as fcerr:
            fcerr.write(cerr)

    # terminate the clusters
    for partition in range(NUM_REPLICAS):
        procs_cluster[partition].terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, chain=True)
    for partition in range(NUM_REPLICAS):
        _, serr = procs_cluster[partition].communicate()
        with open(
            f"{runlog_path}/{protocol}.{num_clients}.{partition}.s.err", "wb"
        ) as fserr:
            fserr.write(serr)

    if any(map(lambda p: p.returncode != 0, procs_clients)):
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(output_dir):
    results = dict()

    for protocol in SUMMERSET_PROTOCOLS:
        results[protocol] = {"tputs": [], "lats": []}
        for num_clients in NUM_CLIENTS_LIST:
            part_tputs, part_lats = [], []
            for partition in range(NUM_REPLICAS):
                print("!!!", protocol, num_clients, partition)
                result = utils.output.gather_outputs(
                    f"{protocol}.{num_clients}",
                    num_clients,
                    output_dir,
                    RESULT_SECS_BEGIN,
                    RESULT_SECS_END,
                    0.1,
                    partition=partition,
                )

                sd, sp, sj, sm = 10, 0, 0, 1
                # if protocol == "Crossword":
                #     # setting sm here to compensate for printing models to console
                #     sm = 1.1
                tput_mean_list = utils.output.list_smoothing(
                    result["tput_sum"], sd, sp, sj, sm
                )
                lat_mean_list = utils.output.list_smoothing(
                    result["lat_avg"], sd, sp, sj, 1 / sm
                )

                part_tputs.append(sum(tput_mean_list) / len(tput_mean_list))
                part_lats.append((sum(lat_mean_list) / len(lat_mean_list)) / 1000)

            results[protocol]["tputs"].append(sum(part_tputs))
            results[protocol]["lats"].append(sum(part_lats) / len(part_lats))

    for protocol in CHAIN_PROTOCOLS:
        results[protocol] = {"tputs": [], "lats": []}
        for num_clients in NUM_CLIENTS_LIST:
            part_tputs, part_lats = [], []
            for partition in range(NUM_REPLICAS):
                print("!!!", protocol, num_clients, partition)
                result = utils.output.parse_ycsb_log(
                    f"{protocol}.{num_clients}",
                    output_dir,
                    1,
                    1,
                    partition=partition,
                )

                part_tputs.append(result["tput"]["mean"])
                part_lats.append(result["lat"]["mean"])

            results[protocol]["tputs"].append(sum(part_tputs))
            results[protocol]["lats"].append(sum(part_lats) / len(part_lats))

    return results


def print_results(results):
    for protocol, result in results.items():
        print(protocol)
        print("  tputs", end="")
        for tput in result["tputs"]:
            print(f"  {tput:7.2f}", end="")
        print()
        print("  lats ", end="")
        for lat in result["lats"]:
            print(f"  {lat:7.2f}", end="")
        print()


def plot_results(results, plots_dir):
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


def plot_legend(handles, labels, plots_dir):
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
        base, repo, hosts, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )
        hosts = hosts[:NUM_REPLICAS]
        remotes = {h: remotes[h] for h in hosts}

        trace_procs = []
        for host in hosts:
            trace_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["python3", f"./scripts/{GEN_YCSB_SCRIPT}"],
                    cd_dir=f"{base}/{repo}",
                    capture_stdout=True,
                    capture_stderr=True,
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(trace_procs, names=hosts)

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

        print("Clearing tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(remotes=remotes)

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

        # handles, labels = plot_results(results, plots_dir)
        # plot_legend(handles, labels, plots_dir)