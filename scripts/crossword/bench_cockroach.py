import sys
import os
import argparse
import time
import math

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib  # type: ignore
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "1dc"

EXPER_NAME = "cockroach"
PROTOCOLS = ["Raft", "Crossword"]

# MIN_HOST0_CPUS = 30
# SERVER_PIN_CORES = 20
# CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
MIN_RANGE_ID = 70
MIN_PAYLOAD = 4096

WLOAD_NAME = "tpcc"
WLOAD_CONCURRENCY = 400
WLOAD_WAREHOUSES = 200
WLOAD_VALUE_SIZE = 8  # capacity scale

LENGTH_SECS = 40

NETEM_MEAN = lambda _: 1
NETEM_JITTER = lambda _: 2
NETEM_RATE = lambda _: 1  # no effect given the original bandwidth


def launch_cluster(remote0, base, repo, protocol):
    cmd = [
        "python3",
        "./scripts/crossword/distr_cockroach.py",
        "-p",
        protocol,
        "-n",
        str(NUM_REPLICAS),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "-v",
        str(WLOAD_VALUE_SIZE),
        "--min_range_id",
        str(MIN_RANGE_ID),
        "--min_payload",
        str(MIN_PAYLOAD),
        "--fixed_num_voters",
        str(NUM_REPLICAS),
    ]

    print("    Launching CockroachDB cluster...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup(sleep_secs=120):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def load_cock_workload(remote0, base, repo, protocol):
    cmd = [
        "python3",
        "./scripts/crossword/distr_cockwload.py",
        "-p",
        protocol,
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "-w",
        WLOAD_NAME,
        "-c",
        str(WLOAD_CONCURRENCY),
        "-s",
        str(WLOAD_WAREHOUSES),
        "-v",
        str(WLOAD_VALUE_SIZE),
        "-i",
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
    ]

    print(f"    Doing {WLOAD_NAME} workload load...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def run_cock_workload(remote0, base, repo, protocol):
    cmd = [
        "python3",
        "./scripts/crossword/distr_cockwload.py",
        "-p",
        protocol,
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "-w",
        WLOAD_NAME,
        "-c",
        str(WLOAD_CONCURRENCY),
        "-s",
        str(WLOAD_WAREHOUSES),
        "-v",
        str(WLOAD_VALUE_SIZE),
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
    ]

    print(f"    Doing {WLOAD_NAME} workload run...")
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

    # launch CockroachDB cluster
    proc_cluster = launch_cluster(remote0, base, repo, protocol)
    wait_cluster_setup()

    # start workload loading client
    proc_load = load_cock_workload(remote0, base, repo, protocol)

    # wait for loading client to exit
    _, cerr = proc_load.communicate()
    with open(f"{runlog_path}/{protocol}.c.load.err", "wb") as fcerr:
        fcerr.write(cerr)

    # start workload running client
    proc_run = run_cock_workload(remote0, base, repo, protocol)

    # wait for running client to exit
    _, cerr = proc_run.communicate()
    with open(f"{runlog_path}/{protocol}.c.run.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating CockroachDB cluster...")
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    sout, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.out", "wb") as fsout:
        fsout.write(sout)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_run.returncode != 0:
        print("    Bench round FAILED!")
        sys.exit(1)
    else:
        print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for protocol in PROTOCOLS:
        results[protocol] = utils.output.gather_tpcc_outputs(
            f"{protocol}.{WLOAD_NAME}",
            output_dir,
        )

    # normalize throughput according to #txns succeeded
    txns_ratio = (
        results["Raft"]["payment"]["txns"] - results["Raft"]["payment"]["errors"]
    ) / (
        results["Crossword"]["payment"]["txns"]
        - results["Crossword"]["payment"]["errors"]
    )
    for txn_type in results["Raft"]:
        results["Crossword"][txn_type]["tput"] = (
            results["Crossword"][txn_type]["tput"] * txns_ratio
        )

    return results


def print_results(results):
    for protocol, txn_result in results.items():
        print(protocol)
        for txn_type, result in txn_result.items():
            print(
                f"  {txn_type:>11s}  tput(txn/s) {result['tput']:6.1f}  avg(ms) {result['lat_avg']:6.1f}"
                f"  p50(ms) {result['lat_p50']:6.1f}  p95(ms) {result['lat_p95']:6.1f}  p99(ms) {result['lat_p99']:6.1f}"
            )


def plot_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (6.0, 1.5),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-value_size")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Crossword",
        "RSPaxos",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "RSPaxos": ("RSPaxos", "pink", "//"),
    }

    protocol_results = {p: [] for p in PROTOCOLS_ORDER}
    for protocol in PROTOCOLS_ORDER:
        for value_size in VALUE_SIZES:
            midfix_str = f".{value_size}"
            protocol_results[protocol].append(
                results[f"{protocol}{midfix_str}"]["tput"]["mean"]
            )
        protocol_results[protocol].sort(reverse=True)

    xpos = 1
    for i in range(len(VALUE_SIZES)):
        for protocol in PROTOCOLS_ORDER:
            result = protocol_results[protocol][i]
            label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
            bar = plt.bar(
                xpos,
                result,
                width=1,
                color=color,
                edgecolor="black",
                linewidth=1.4,
                label=label if i == 0 else None,
                hatch=hatch,
            )
            xpos += 1
        xpos += 1

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)

    assert len(VALUE_SIZES) == len(VALUE_SIZES_STRS)
    plt.xticks([2 + 4 * i for i in range(len(VALUE_SIZES))], VALUE_SIZES_STRS)
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
            "figure.figsize": (4, 0.5),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=len(labels),
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=1.1,
    )
    for rec in lgd.get_texts():
        if "RSPaxos" in rec.get_text():
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

        # NOTE: this script assumes that CockroachDB has been built on all nodes
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, cockroach=True)
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

        print(f"Running experiments...")
        for protocol in PROTOCOLS:
            time.sleep(3)
            bench_round(
                remotes["host0"],
                base,
                repo,
                protocol,
            )
            utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, cockroach=True)
            utils.file.clear_fs_caches(remotes=remotes)

        print("Clearing tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(remotes=remotes)

        print("Fetching workload output logs...")
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
