import sys
import os
import argparse
import time
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib  # type: ignore
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "reg"

EXPER_NAME = "loc_wr_grid"

COMMON_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=150",
]
PROTOCOLS_NAME_CONFIGS = {
    "LeaderLs": (
        "MultiPaxos",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
        ],
        [],
    ),
    "EPaxos": (
        "EPaxos",
        [],
        [
            "near_server_id=0",  # placeholder; set by distr_clients.py
        ],
    ),
    "PQR": (
        "MultiPaxos",
        [
            "enable_quorum_reads=true",
            "urgent_commit_notice=true",
        ],
        [
            "enable_quorum_reads=true",
            "near_server_id=0",  # placeholder; set by distr_clients.py
        ],
    ),
    "LeaderLsPQR": (
        "MultiPaxos",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
            "enable_quorum_reads=true",
            "urgent_commit_notice=true",
        ],
        [
            "enable_quorum_reads=true",
            "near_server_id=0",  # placeholder; set by distr_clients.py
        ],
    ),
    "QuorumLeases": (
        "QuorumLeases",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
            "urgent_commit_notice=true",
        ],
        [
            "near_server_id=0",  # placeholder; set by distr_clients.py
        ],
    ),
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 50
VALUE_SIZE = 128
PUT_RATIOS = [0, 15, 50, 100]
BATCH_INTERVAL = 1

LENGTH_SECS = 30
RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 25

NETEM_MEAN = lambda _: 10
NETEM_JITTER = lambda _: 0
NETEM_RATE = lambda _: 1  # no effect


def launch_cluster(remote0, base, repo, pcname, put_ratio, config=None):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        PROTOCOLS_NAME_CONFIGS[pcname][0],
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
        f".{pcname}.{put_ratio}",
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


def wait_cluster_setup(sleep_secs=30):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(remote0, base, repo, pcname, put_ratio, config=None):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        PROTOCOLS_NAME_CONFIGS[pcname][0],
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
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    cmd += [
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
        str(put_ratio),
        "-l",
        str(LENGTH_SECS),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{pcname}.{put_ratio}",
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


def bench_round(remote0, base, repo, pcname, put_ratio, runlog_path):
    protocol = PROTOCOLS_NAME_CONFIGS[pcname][0]
    midfix_str = f".{pcname}.{put_ratio}"
    print(f"  {EXPER_NAME}  {pcname:<10s}{midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in COMMON_CONFIGS:
        server_config += f"+{cfg}"
    for cfg in PROTOCOLS_NAME_CONFIGS[pcname][1]:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, pcname, put_ratio, config=server_config
    )
    wait_cluster_setup()

    # client-side configs
    client_config = "+".join(PROTOCOLS_NAME_CONFIGS[pcname][2])

    # start benchmarking clients
    proc_clients = run_bench_clients(
        remote0, base, repo, pcname, put_ratio, config=client_config
    )

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
    for put_ratio in PUT_RATIOS:
        results[put_ratio] = dict()
        for cgroup in range(NUM_REPLICAS):
            results[put_ratio][cgroup] = dict()
            for pcname in PROTOCOLS_NAME_CONFIGS:
                protocol = PROTOCOLS_NAME_CONFIGS[pcname][0]

                result = utils.output.gather_outputs(
                    f"{protocol}.{pcname}.{put_ratio}",
                    NUM_CLIENTS,
                    output_dir,
                    RESULT_SECS_BEGIN,
                    RESULT_SECS_END,
                    0.1,
                    client_start=cgroup,
                    client_step=NUM_REPLICAS,
                )

                sd, sp, sj, sm = 10, 0, 0, 1
                # setting sm here to compensate for unstabilities of printing
                # things to console
                tput_mean_list = utils.output.list_smoothing(
                    result["tput_sum"], sd, sp, sj, sm
                )
                tput_stdev_list = result["tput_stdev"]
                wlat_mean_list = utils.output.list_smoothing(
                    result["wlat_avg"], sd, sp, sj, 1 / sm
                )
                wlat_stdev_list = result["wlat_stdev"]
                rlat_mean_list = utils.output.list_smoothing(
                    result["rlat_avg"], sd, sp, sj, 1 / sm
                )
                rlat_stdev_list = result["rlat_stdev"]

                results[put_ratio][cgroup][pcname] = {
                    "tput": {
                        "mean": tput_mean_list,
                        "stdev": tput_stdev_list,
                    },
                    "wlat": {
                        "mean": wlat_mean_list,
                        "stdev": wlat_stdev_list,
                    },
                    "rlat": {
                        "mean": rlat_mean_list,
                        "stdev": rlat_stdev_list,
                    },
                }

    ymax = {"tput": 0.0, "wlat": 0.0, "rlat": 0.0}
    for put_ratio in PUT_RATIOS:
        for cgroup in range(NUM_REPLICAS):
            for pcname in PROTOCOLS_NAME_CONFIGS:
                curr_results = results[put_ratio][cgroup][pcname]
                tput_mean_list = curr_results["tput"]["mean"]
                tput_stdev_list = curr_results["tput"]["stdev"]
                wlat_mean_list = sorted(
                    lat / 1000 for lat in curr_results["wlat"]["mean"]
                )
                rlat_mean_list = sorted(
                    lat / 1000 for lat in curr_results["rlat"]["mean"]
                )

                results[put_ratio][cgroup][pcname] = {
                    "tput": {
                        "mean": sum(tput_mean_list) / len(tput_mean_list),
                        "stdev": (
                            sum(map(lambda s: s**2, tput_stdev_list))
                            / len(tput_stdev_list)
                        )
                        ** 0.5,
                    },
                    "wlat": {
                        "sorted": wlat_mean_list,
                        "median": wlat_mean_list[len(wlat_mean_list) // 2],
                    },
                    "rlat": {
                        "sorted": rlat_mean_list,
                        "median": rlat_mean_list[len(rlat_mean_list) // 2],
                    },
                }
                curr_results = results[put_ratio][cgroup][pcname]
                if curr_results["tput"]["mean"] > ymax["tput"]:
                    ymax["tput"] = curr_results["tput"]["mean"]
                if curr_results["wlat"]["sorted"][-1] > ymax["wlat"]:
                    ymax["wlat"] = curr_results["wlat"]["sorted"][-1]
                if curr_results["rlat"]["sorted"][-1] > ymax["rlat"]:
                    ymax["rlat"] = curr_results["rlat"]["sorted"][-1]

    return results, ymax


def print_results(results):
    for put_ratio in PUT_RATIOS:
        print(f"puts {put_ratio}")
        for cgroup in results[put_ratio]:
            print(f"  cg {cgroup}")
            for pcname, result in results[put_ratio][cgroup].items():
                print(f"    {pcname}")
                print(
                    f"      tput  mean {result['tput']['mean']:7.2f}"
                    + f"  stdev {result['tput']['stdev']:7.2f}"
                    + f"  wlat  median {result['wlat']['median']:7.2f}"
                    + f"  rlat  median {result['rlat']['median']:7.2f}"
                )


def plot_put_ratio_results(results, put_ratio, plots_dir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.5, 3.2),
            "font.size": 11,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Exper-{put_ratio}")

    PCNAMES_ORDER = [
        "LeaderLs",
        "PQR",
        "LeaderLsPQR",
    ]
    PCNAMES_LABEL_COLOR_HATCH = {
        "LeaderLs": ("Leader Leases", "lightslategray", None),
        "PQR": ("PQR", "cornsilk", "//"),
        "LeaderLsPQR": ("PQR+", "pink", "//"),
    }

    # throughput
    ax1 = plt.subplot(311)

    xpos, xticks, xticklabels, ymaxl = 1, [], [], 0.0
    for cgroup in results[put_ratio]:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(cgroup)

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["tput"]
            if result["mean"] > ymaxl:
                ymaxl = result["mean"]

            label, color, hatch = PCNAMES_LABEL_COLOR_HATCH[pcname]
            bar = plt.bar(
                xpos,
                result["mean"],
                width=1,
                color=color,
                edgecolor="black",
                linewidth=1,
                label=label,
                hatch=hatch,
                # yerr=result["stdev"],
                # ecolor="black",
                # capsize=1,
            )

            xpos += 1
        xpos += 1.5

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)
    plt.xlim((-0.5, xpos - 1))

    plt.ylabel("Tput. (ops/s)")
    # ax1.yaxis.set_label_coords(-0.7, 0.5)

    if ymax is not None:
        plt.ylim(0.0, ymax["tput"] * 1.1)
    else:
        # ytickmax = math.ceil(ymaxl / 10) * 10
        # plt.yticks([0, ytickmax // 2, ytickmax])
        # plt.ylim(0.0, ytickmax * 1.2)
        plt.ylim(0.0, ymaxl * 1.1)

    # put latency
    ax1 = plt.subplot(312)

    xpos, xticks, xticklabels = 1, [], []
    for cgroup in results[put_ratio]:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(cgroup)

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["wlat"]
            p99_wlat = np.percentile(result["sorted"], 99)

            label, color, hatch = PCNAMES_LABEL_COLOR_HATCH[pcname]
            bar = plt.bar(
                xpos,
                result["median"],
                width=1,
                color=color,
                edgecolor="dimgray",
                linewidth=1,
                label=label,
                hatch=hatch,
                yerr=p99_wlat - result["median"],
                ecolor="black",
                capsize=1,
                error_kw={"lolims": True},
            )
            # box = plt.boxplot(
            #     result,
            #     positions=[xpos],
            #     widths=[1],
            #     vert=True,
            #     whis=(1, 99),
            #     showfliers=False,
            #     patch_artist=True,
            #     label=label,
            # )
            # box["boxes"][0].set_facecolor(color)
            # box["boxes"][0].set_edgecolor("gray")
            # box["boxes"][0].set_linewidth(1)
            # box["boxes"][0].set_hatch(hatch)

            xpos += 1
        xpos += 1.5

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)
    plt.xlim((-0.5, xpos - 1))

    plt.ylabel("W Lat. (ms)")
    # ax1.yaxis.set_label_coords(-0.7, 0.5)

    if ymax is not None:
        plt.ylim(0.0, ymax["wlat"] * 1.1)
    else:
        # ytickmax = math.ceil(ymaxl / 10) * 10
        # plt.yticks([0, ytickmax // 2, ytickmax])
        # plt.ylim(0.0, ytickmax * 1.2)
        plt.ylim(bottom=0.0)

    # get latency
    ax1 = plt.subplot(313)

    xpos, xticks, xticklabels = 1, [], []
    for cgroup in results[put_ratio]:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(f"CG{cgroup}")

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["rlat"]
            p99_rlat = np.percentile(result["sorted"], 99)

            label, color, hatch = PCNAMES_LABEL_COLOR_HATCH[pcname]
            bar = plt.bar(
                xpos,
                result["median"],
                width=1,
                color=color,
                edgecolor="dimgray",
                linewidth=1,
                label=label,
                hatch=hatch,
                yerr=p99_rlat - result["median"],
                ecolor="black",
                capsize=1,
                error_kw={"lolims": True},
            )
            # box = plt.boxplot(
            #     result,
            #     positions=[xpos],
            #     widths=[1],
            #     vert=True,
            #     whis=(1, 99),
            #     showfliers=False,
            #     patch_artist=True,
            #     label=label,
            # )
            # box["boxes"][0].set_facecolor(color)
            # box["boxes"][0].set_edgecolor("gray")
            # box["boxes"][0].set_linewidth(1)
            # box["boxes"][0].set_hatch(hatch)

            xpos += 1
        xpos += 1.5

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)
    plt.xlim((-0.5, xpos - 1))
    plt.xticks(xticks, xticklabels)

    plt.ylabel("R Lat. (ms)")
    # ax1.yaxis.set_label_coords(-0.7, 0.5)

    if ymax is not None:
        plt.ylim(0.0, ymax["rlat"] * 1.1)
    else:
        # ytickmax = math.ceil(ymaxl / 10) * 10
        # plt.yticks([0, ytickmax // 2, ytickmax])
        # plt.ylim(0.0, ytickmax * 1.2)
        plt.ylim(bottom=0.0)

    # fig.subplots_adjust(left=0.5)
    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-w{put_ratio}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


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

        for put_ratio in PUT_RATIOS:
            print(f"Running experiments {put_ratio}...")
            for pcname in PROTOCOLS_NAME_CONFIGS:
                time.sleep(3)
                bench_round(
                    remotes["host0"],
                    base,
                    repo,
                    pcname,
                    put_ratio,
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

        results, ymax = collect_outputs(output_dir)
        print_results(results)

        for put_ratio in PUT_RATIOS:
            handles, labels = plot_put_ratio_results(
                results, put_ratio, plots_dir, ymax=ymax
            )
        # plot_legend(handles, labels, plots_dir)
