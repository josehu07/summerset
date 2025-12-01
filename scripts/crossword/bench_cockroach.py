import os
import argparse
import time
import math
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "reg"

EXPER_NAME = "cockroach"
PROTOCOLS = ["Raft", "Crossword", "CRaft"]

# MIN_HOST0_CPUS = 30
# SERVER_PIN_CORES = 20
# CLIENT_PIN_CORES = 2

CLUSTER_SIZE = 12
NUM_REPLICAS = 5
MIN_RANGE_ID = 70
MIN_PAYLOAD = 4096

WLOAD_NAME = "tpcc"
WLOAD_CONCURRENCY = 400
WLOAD_WAREHOUSES = 200
WLOAD_VALUE_SIZE = 8  # capacity scale

LENGTH_SECS = 45

NETEM_MEAN = lambda _: 1
NETEM_JITTER = lambda _: 2
NETEM_RATE = lambda _: 1  # no effect given the original bandwidth


def launch_cluster(remote0, base, repo, protocol):
    cmd = [
        "python3",
        "./scripts/crossword/distr_cockroach.py",
        "-p",
        protocol,
        "-c",
        str(NUM_REPLICAS),
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
        "--output_prefix",
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
        "--output_prefix",
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


def bench_round(remote0, base, repo, protocol, runlog_path):
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
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, cockroach=True)
    sout, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}.s.out", "wb") as fsout:
        fsout.write(sout)
    with open(f"{runlog_path}/{protocol}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_run.returncode != 0:
        print("    Bench round FAILED!")
        raise utils.BreakingLoops
    else:
        print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for protocol in PROTOCOLS:
        results[protocol] = utils.output.gather_tpcc_outputs(
            f"{protocol}.{WLOAD_NAME}",
            output_dir,
        )
        for txn_type in results[protocol]:
            results[protocol][txn_type]["txns"] *= CLUSTER_SIZE / NUM_REPLICAS
            results[protocol][txn_type]["tput"] *= CLUSTER_SIZE / NUM_REPLICAS

    # normalize throughput according to #txns succeeded
    txns_ratio = (
        results["Raft"]["payment"]["txns"]
        - results["Raft"]["payment"]["errors"]
    ) / (
        results["Crossword"]["payment"]["txns"]
        - results["Crossword"]["payment"]["errors"]
    )
    txns_ratio *= (
        results["Raft"]["aggregate"]["lat_avg"]
        / results["Crossword"]["aggregate"]["lat_avg"]
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
            "figure.figsize": (5.4, 2.5),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    _fig = plt.figure("Exper-cockroach")

    TXN_TYPES_ORDER = [
        "newOrder",
        "payment",
        "orderStatus",
        "delivery",
        "stockLevel",
        "aggregate",
    ]
    TXN_TYPES_LABEL_RATIO = {
        "newOrder": ("NO-45%", 10 / 23),
        "payment": ("PM-43%", 10 / 23),
        "orderStatus": ("OS-4%", 1 / 23),
        "delivery": ("DL-4%", 1 / 23),
        "stockLevel": ("SL-4%", 1 / 23),
        "aggregate": ("Agg.", 23 / 23),
    }
    PROTOCOLS_ORDER = [
        "Raft",
        "Crossword",
        "CRaft",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "Raft": ("Vanilla", "lightgreen", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "CRaft": ("1-Shard", "wheat", "\\\\"),
    }

    # throughput
    ax1 = plt.subplot(211)

    ymaxl = 0.0
    for t, txn_type in enumerate(TXN_TYPES_ORDER):
        for i, protocol in enumerate(PROTOCOLS_ORDER):
            xpos = t * (len(PROTOCOLS_ORDER) + 1.6) + i + 1
            tput = results[protocol][txn_type]["tput"] / 1000.0
            if txn_type != "aggregate" and tput > ymaxl:
                ymaxl = tput

            label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
            _bar = plt.bar(
                xpos,
                tput,
                width=1,
                color=color,
                edgecolor="black",
                linewidth=1.4,
                label=label if t == 0 else None,
                hatch=hatch,
                # yerr=result["stdev"],
                # ecolor="black",
                # capsize=1,
            )

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)

    plt.xlim((-0.4, len(TXN_TYPES_ORDER) * (len(PROTOCOLS_ORDER) + 1.6) - 0.4))

    plt.ylabel("Throughput\n(k txns/s)")
    # ytickmax = math.ceil(ymaxl / 10) * 10
    plt.ylim(0.0, ymaxl * 1.1)
    plt.yticks([0, ymaxl / 2, ymaxl], ["0", f"{ymaxl / 2:.1f}", f"{ymaxl:.1f}"])
    ax1.yaxis.set_label_coords(-0.105, 0.5)

    plt.text(
        11,
        ymaxl * 0.88,
        f"Agg. tput.:\n"
        f" [{results['Raft']['aggregate']['tput'] / 1000.0:.2f}k,"
        f" {results['Crossword']['aggregate']['tput'] / 1000.0:.2f}k,"
        f" {results['CRaft']['aggregate']['tput'] / 1000.0:.2f}k]",
        ha="left",
        va="center",
        fontsize=13,
    )

    # latency
    ax2 = plt.subplot(212)

    ymaxl = 0.0
    for t, txn_type in enumerate(TXN_TYPES_ORDER):
        for i, protocol in enumerate(PROTOCOLS_ORDER):
            xpos = t * (len(PROTOCOLS_ORDER) + 1.6) + i + 1
            avg, p95 = (
                results[protocol][txn_type]["lat_avg"],
                results[protocol][txn_type]["lat_p95"],
            )
            if p95 > ymaxl:
                ymaxl = p95

            label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
            _bar = plt.bar(
                xpos,
                avg,
                width=1,
                color=color,
                edgecolor="gray",
                linewidth=1.4,
                # label=label,
                hatch=hatch,
                yerr=p95 - avg,
                ecolor="black",
                capsize=2,
                error_kw={"lolims": True},
            )

    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)

    plt.xlim((-0.4, len(TXN_TYPES_ORDER) * (len(PROTOCOLS_ORDER) + 1.6) - 0.4))
    xticks = [2 + 4.6 * i for i in range(len(TXN_TYPES_ORDER))]
    xticks[0] -= 0.3
    ax2.set_xticks(
        xticks,
        [TXN_TYPES_LABEL_RATIO[t][0] for t in TXN_TYPES_ORDER],
    )

    plt.ylabel("Latency\n(ms)")
    ytickmax = math.ceil(ymaxl / 10) * 10
    plt.ylim(0.0, ytickmax * 1.1)
    plt.yticks([0, ytickmax // 2, ytickmax])

    plt.tight_layout(pad=0.2)

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3, 1.5),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    _lgd = plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=1.1,
    )

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


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

        # NOTE: this script assumes that CockroachDB has been built on all nodes
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, cockroach=True)
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        print("Setting tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(
            remotes=remotes, capture_stderr=True
        )
        utils.net.set_tc_qdisc_netems_main(
            NETEM_MEAN,
            NETEM_JITTER,
            NETEM_RATE,
            involve_ifb=True,
            remotes=remotes,
        )

        try:
            print("Running experiments...")
            for protocol in PROTOCOLS:
                time.sleep(3)
                bench_round(
                    remotes["host0"],
                    base,
                    repo,
                    protocol,
                    runlog_path,
                )
                utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, cockroach=True)
                utils.file.clear_fs_caches(remotes=remotes)

        except utils.BreakingLoops:
            print("Experiment FAILED, breaking early...")

        print("Clearing tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(remotes=remotes)

        print("Fetching workload output logs...")
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


if __name__ == "__main__":
    main()
