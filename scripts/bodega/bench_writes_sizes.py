import sys
import os
import argparse
import time
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "writes_sizes"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
]
PROTOCOLS_BSNAME_CONFIGS_RESPONDERS = {
    "PQRLeaderLs": (
        "MultiPaxos",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
            "enable_quorum_reads=true",
            "urgent_commit_notice=true",
        ],
        [
            "enable_quorum_reads=true",
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        None,
    ),
    "QuorumLs": (
        "QuorumLeases",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
            "urgent_commit_notice=true",
            "no_lease_retraction=false",
        ],
        [
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        {0, 1, 3, 4},
    ),
    "QuorumLsCtn": (
        "QuorumLeases",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
            "urgent_commit_notice=true",
            "no_lease_retraction=true",
        ],
        [
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        {0, 1, 3, 4},
    ),
    "Bodega": (
        "Bodega",
        [
            "lease_expire_ms=2500",
            "urgent_commit_notice=true",
            "urgent_accept_notice=true",
        ],
        [
            "local_read_unhold_ms=250",
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        {0, 1, 3, 4},
    ),
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 5  # will be x10
NUM_KEYS = 1  # will be x10
VALUE_SIZES = [32, 128, 512, 2048, 8192]
PUT_RATIOS = [1, 2, 5, 10, 25, 50]
BATCH_INTERVAL = 1

LENGTH_SECS = 120
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 100


def launch_cluster(
    remote0, base, repo, pcname, value_size, put_ratio, config=None
):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0],
        "-n",
        str(NUM_REPLICAS),
        "-r",
        "--force_leader",
        str(FORCE_LEADER),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--states_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--states_midfix",
        f".{pcname}.{value_size}.{put_ratio}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--launch_wait",
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


def wait_cluster_setup(sleep_secs=60):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(
    remotec, base, repo, pcname, value_size, put_ratio, config=None
):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0],
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{NUM_REPLICAS}",  # place clients on host5~9
        "--man",
        "host0",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    cmd += [
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-m",
        str(NUM_REPLICAS),
        "-d",
        str(NUM_REPLICAS),
        "-f",
        str(0),  # closed-loop
        "-k",
        str(NUM_KEYS),
        "-v",
        str(value_size * 5 * 10),  # scaled for all
        "-w",
        str(put_ratio),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{pcname}.{value_size}.{put_ratio}",
    ]

    print(f"    Running benchmark clients ({LENGTH_SECS}s)...")
    return utils.proc.run_process_over_ssh(
        remotec,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def run_mess_client(
    remotec, base, repo, protocol, leader=None, key_range=None, responder=None
):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        protocol,
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{NUM_REPLICAS}",
        "--man",
        "host0",
        "--skip_build",
        "mess",
    ]
    if leader is not None and len(leader) > 0:
        cmd += ["--leader", leader]
    if key_range is not None and len(key_range) > 0:
        cmd += ["--key_range", key_range]
    if responder is not None and len(responder) > 0:
        cmd += ["--responder", responder]

    return utils.proc.run_process_over_ssh(
        remotec,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(
    remote0, remotec, base, repo, pcname, value_size, put_ratio, runlog_path
):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    responders = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3]
    midfix_str = f".{pcname}.{value_size}.{put_ratio}"
    print(f"  {EXPER_NAME}  {pcname:<12s}{midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in COMMON_SERVER_CONFIGS:
        server_config += f"+{cfg}"
    for cfg in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][1]:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, pcname, value_size, put_ratio, config=server_config
    )
    wait_cluster_setup()

    # if protocol has responders config, do it now
    if responders is not None:
        print(f"    Marking responders {responders}...")
        proc_mess = run_mess_client(
            remotec,
            base,
            repo,
            protocol,
            leader=str(FORCE_LEADER),
            key_range="full",
            responder=",".join(list(map(str, responders))),
        )
        mout, merr = proc_mess.communicate()
        with open(f"{runlog_path}/{protocol}{midfix_str}.m.out", "wb") as fmout:
            fmout.write(mout)
        with open(f"{runlog_path}/{protocol}{midfix_str}.m.err", "wb") as fmerr:
            fmerr.write(merr)
        time.sleep(5)

    # client-side configs
    client_config = "+".join(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][2])

    # start benchmarking clients
    proc_clients = run_bench_clients(
        remotec, base, repo, pcname, value_size, put_ratio, config=client_config
    )

    # wait for benchmarking clients to exit
    cout, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.out", "wb") as fcout:
        fcout.write(cout)
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating Summerset cluster...")
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    sout, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.out", "wb") as fsout:
        fsout.write(sout)
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Bench round FAILED!")
        raise utils.BreakingLoops
    else:
        print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for value_size in VALUE_SIZES:
        results[value_size] = dict()
        for put_ratio in PUT_RATIOS:
            if put_ratio != 5 and value_size != 128:
                continue
            results[value_size][put_ratio] = dict()
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]

                result = utils.output.gather_outputs(
                    f"{protocol}.{pcname}.{value_size}.{put_ratio}",
                    NUM_CLIENTS,
                    output_dir,
                    RESULT_SECS_BEGIN,
                    RESULT_SECS_END,
                    0.1,
                    # skip near-leader & non-responder clients for this exper report
                    client_skips=({0, 2}, NUM_REPLICAS),
                )

                sd, sp, sj, sm = 10, 0, 0, 1
                # setting sm here to compensate for unstabilities of printing
                # things to console
                tput_list = utils.output.list_smoothing(
                    result["tput_sum"], sd, sp, sj, sm
                )
                wlat_list = utils.output.list_smoothing(
                    [v for v in result["wlat_avg"] if v > 0.0],
                    sd,
                    sp,
                    sj,
                    1 / sm,
                )
                rlat_list = utils.output.list_smoothing(
                    [v for v in result["rlat_avg"] if v > 0.0],
                    sd,
                    sp,
                    sj,
                    1 / sm,
                )

                results[value_size][put_ratio][pcname] = {
                    "tput": tput_list,
                    "wlat": wlat_list,
                    "rlat": rlat_list,
                }

    for value_size in VALUE_SIZES:
        for put_ratio in results[value_size]:
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                curr_results = results[value_size][put_ratio][pcname]
                tput_list = [  # scaled kops/s for all
                    5 * tput / 1000.0 for tput in curr_results["tput"]
                ]
                wlat_list = sorted(lat / 1000 for lat in curr_results["wlat"])
                rlat_list = sorted(lat / 1000 for lat in curr_results["rlat"])

                results[value_size][put_ratio][pcname] = {
                    "tput": {
                        "mean": sum(tput_list) / len(tput_list),
                        # "stdev": (
                        #     sum(map(lambda s: s**2, tput_stdev_list))
                        #     / len(tput_stdev_list)
                        # )
                        # ** 0.5,
                    },
                    "wlat": {
                        "sorted": wlat_list,
                        "p50": (
                            None
                            if len(wlat_list) == 0
                            else wlat_list[len(wlat_list) // 2]
                        ),
                        "p99": (
                            None
                            if len(wlat_list) == 0
                            else np.percentile(wlat_list, 99)
                        ),
                        "mean": (
                            None
                            if len(wlat_list) == 0
                            else sum(wlat_list) / len(wlat_list)
                        ),
                    },
                    "rlat": {
                        "sorted": rlat_list,
                        "p50": (
                            None
                            if len(rlat_list) == 0
                            else rlat_list[len(rlat_list) // 2]
                        ),
                        "p99": (
                            None
                            if len(rlat_list) == 0
                            else np.percentile(rlat_list, 99)
                        ),
                        "mean": (
                            None
                            if len(rlat_list) == 0
                            else sum(rlat_list) / len(rlat_list)
                        ),
                    },
                }

    return results


def print_results(results):
    for value_size in VALUE_SIZES:
        print(f"size {value_size}")
        for put_ratio in results[value_size]:
            print(f"  puts {put_ratio}")
            for pcname, result in results[value_size][put_ratio].items():
                print(f"    {pcname}")
                print(f"      tput  mean {result['tput']['mean']:7.2f}", end="")
                if result["wlat"]["mean"] is not None:
                    print(
                        f"  wlat  mean {result['wlat']['mean']:7.2f}  p50 {result['wlat']['p50']:7.2f}  p99 {result['wlat']['p99']:7.2f}",
                        end="",
                    )
                else:
                    print(
                        f"  wlat  mean {'-':7}  p50 {'-':7}  p99 {'-':7}",
                        end="",
                    )
                if result["rlat"]["mean"] is not None:
                    print(
                        f"  rlat  mean {result['rlat']['mean']:7.2f}  p50 {result['rlat']['p50']:7.2f}  p99 {result['rlat']['p99']:7.2f}",
                        end="",
                    )
                else:
                    print(
                        f"  rlat  mean {'-':7}  p50 {'-':7}  p99 {'-':7}",
                        end="",
                    )
                print()


def plot_put_ratios_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.4, 1.6),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-put_ratios")

    PCNAMES_ORDER = [
        "PQRLeaderLs",
        "QuorumLs",
        "QuorumLsCtn",
        "Bodega",
    ]
    PCNAMES_LABEL_COLOR_MARKER_SIZE = {
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "firebrick", "^", 3.5),
        "QuorumLs": ("Quorum Leases", "mediumseagreen", "x", 4.0),
        "QuorumLsCtn": ("Qrm Ls (passive)", "forestgreen", "p", 3.8),
        "Bodega": ("Bodega", "steelblue", "*", 5.2),
    }

    for pcname in PCNAMES_ORDER:
        result = [
            results[128][put_ratio][pcname]["tput"]["mean"]
            for put_ratio in PUT_RATIOS
        ]
        label, color, marker, markersize = PCNAMES_LABEL_COLOR_MARKER_SIZE[
            pcname
        ]

        plt.plot(
            PUT_RATIOS,
            result,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xscale("symlog")
    plt.xticks([1, 2, 5, 10, 25, 50], list(map(str, [1, 2, 5, 10, 25, 50])))
    plt.minorticks_off()
    plt.xlabel("Write ratio% (log scale)")

    plt.ylim(bottom=0)
    plt.ylabel("Tput (k reqs/s)")

    # reverse the order of legends?
    handles, labels = ax.get_legend_handles_labels()
    handles = handles[::-1]
    labels = labels[::-1]

    plt.legend(
        handles,
        labels,
        fontsize=8,
        handleheight=0.8,
        handlelength=1.4,
        markerscale=1.2,
        loc="upper right",
        bbox_to_anchor=(1.04, 1.15),
        ncol=1,
        borderpad=0.3,
        handletextpad=0.5,
        columnspacing=2.0,
        frameon=False,
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-puts.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_value_sizes_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.3, 1.52),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-value_sizes")

    PCNAMES_ORDER = [
        "PQRLeaderLs",
        "QuorumLs",
        "QuorumLsCtn",
        "Bodega",
    ]
    PCNAMES_LABEL_COLOR_MARKER_SIZE = {
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "firebrick", "^", 3.5),
        "QuorumLs": ("Quorum Leases", "mediumseagreen", "x", 4.0),
        "QuorumLsCtn": ("Qrm Ls (passive)", "forestgreen", "p", 3.8),
        "Bodega": ("Bodega", "steelblue", "*", 5.2),
    }

    VALUE_SIZE_STRS = ["32", "128", "512", "2K", "8K"]

    for pcname in PCNAMES_ORDER:
        result = [
            results[value_size][5][pcname]["tput"]["mean"]
            for value_size in VALUE_SIZES
        ]
        label, color, marker, markersize = PCNAMES_LABEL_COLOR_MARKER_SIZE[
            pcname
        ]

        plt.plot(
            VALUE_SIZES,
            result,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xscale("symlog")
    plt.xticks(VALUE_SIZES, VALUE_SIZE_STRS)
    plt.minorticks_off()
    plt.xlabel("Value size (log scale)")

    plt.ylim(bottom=0)
    plt.ylabel("Tput (k reqs/s)")

    # plt.legend(
    #     fontsize=8,
    #     handleheight=0.8,
    #     handlelength=1.4,
    #     markerscale=1.6,
    #     loc="upper right",
    #     bbox_to_anchor=(1.04, 1.13),
    #     ncol=1,
    #     borderpad=0.3,
    #     handletextpad=0.5,
    #     columnspacing=2.0,
    #     frameon=False,
    # )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-size.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


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
        "-f",
        "--fetch",
        type=str,
        default="",
        help="host from which to fetch results to local",
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

    if not args.plot and len(args.fetch) == 0:
        print("Doing preparation work...")
        base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
            PHYS_ENV_GROUP
        )

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
            fixed_value_size = 128
            for put_ratio in PUT_RATIOS:
                print(
                    f"Running experiments val = {fixed_value_size}, w% = {put_ratio}..."
                )
                for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                    time.sleep(3)

                    bench_round(
                        remotes["host0"],
                        remotes[f"host{NUM_REPLICAS}"],
                        base,
                        repo,
                        pcname,
                        fixed_value_size,
                        put_ratio,
                        runlog_path,
                    )
                    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                    utils.file.remove_files_in_dir(  # to free up storage space
                        f"{base}/states/{EXPER_NAME}",
                        remotes=remotes,
                    )
                    utils.file.clear_fs_caches(remotes=remotes)

            fixed_put_ratio = 5
            for value_size in VALUE_SIZES:
                if value_size == fixed_value_size:
                    continue
                print(
                    f"Running experiments val = {value_size}, w% = {fixed_put_ratio}..."
                )
                for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                    time.sleep(3)

                    bench_round(
                        remotes["host0"],
                        remotes[f"host{NUM_REPLICAS}"],
                        base,
                        repo,
                        pcname,
                        value_size,
                        fixed_put_ratio,
                        runlog_path,
                    )
                    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                    utils.file.remove_files_in_dir(  # to free up storage space
                        f"{base}/states/{EXPER_NAME}",
                        remotes=remotes,
                    )
                    utils.file.clear_fs_caches(remotes=remotes)

        except utils.BreakingLoops:
            print("Experiment FAILED, breaking early...")
            utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)

        print("Fetching client output logs...")
        for host in hosts[NUM_REPLICAS : 2 * NUM_REPLICAS]:
            utils.file.fetch_files_of_dir(
                remotes[host], f"{base}/output/{EXPER_NAME}", output_path
            )

    elif len(args.fetch) > 0:
        print(f"Fetching outputs & runlogs (& plots) <- {args.fetch}...")
        base, repo, _, remotes, _, ipaddrs = utils.config.parse_toml_file(
            PHYS_ENV_GROUP
        )

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        plots_path = f"{args.odir}/plots/{EXPER_NAME}"
        for path in (runlog_path, output_path, plots_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        utils.file.fetch_files_of_dir(
            remotes[args.fetch],
            f"{base}/{repo}/results/output/{EXPER_NAME}",
            output_path,
        )
        utils.file.fetch_files_of_dir(
            remotes[args.fetch],
            f"{base}/{repo}/results/runlog/{EXPER_NAME}",
            runlog_path,
        )

        try:
            utils.file.fetch_files_of_dir(
                remotes[args.fetch],
                f"{base}/{repo}/results/plots/{EXPER_NAME}",
                plots_path,
            )
        except RuntimeError:
            print("  plots not found, skipped...")

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_put_ratios_results(results, plots_dir)
        handles, labels = plot_value_sizes_results(results, plots_dir)
