import sys
import os
import argparse
import time
import statistics
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "wan"

EXPER_NAME = "conf_coverage"

FORCE_LEADER = 0
SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
    "lease_expire_ms=2500",
    "urgent_commit_notice=true",
    "urgent_accept_notice=true",
]
CLIENT_CONFIGS = [
    "local_read_unhold_ms=250",
    "near_server_id=x",  # placeholder; set by distr_clients.py
]

RESPONDER_CONFS = {
    "0-0": {0},
    "0-1": {0, 3},
    "0-2": {0, 3, 2},
    "0-3": {0, 3, 2, 4},
    "0-4": {0, 3, 2, 4, 1},
    "def": {0, 1, 3, 4},
}

KEY_LEN = 8  # hardcoded
KEY_ITH = lambda i: f"k{i:0{KEY_LEN-1}d}"
NUM_KEYS = 1000
KEY_RANGES = {
    "0": None,
    "20": f"{KEY_ITH(0)}-{KEY_ITH(199)}",
    "40": f"{KEY_ITH(0)}-{KEY_ITH(399)}",
    "60": f"{KEY_ITH(0)}-{KEY_ITH(599)}",
    "80": f"{KEY_ITH(0)}-{KEY_ITH(799)}",
    "100": f"full",
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 5
VALUE_SIZE = 128
PUT_RATIO = 10
BATCH_INTERVAL = 1

LENGTH_SECS = 120
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 100


def launch_cluster(remote0, base, repo, confname, keyrname, config=None):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        "Bodega",
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
        f".{confname}.{keyrname}",
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


def run_bench_clients(remotec, base, repo, confname, keyrname, config=None):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        "Bodega",
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
        str(VALUE_SIZE),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{confname}.{keyrname}",
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


def run_mess_client(remotec, base, repo, leader=None, key_range=None, responder=None):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        "Bodega",
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


def bench_round(remote0, remotec, base, repo, confname, keyrname, runlog_path):
    midfix_str = f".{confname}.{keyrname}"
    print(f"  {EXPER_NAME}  {midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in SERVER_CONFIGS:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, confname, keyrname, config=server_config
    )
    wait_cluster_setup()

    # set responders config
    key_range, responders = KEY_RANGES[keyrname], RESPONDER_CONFS[confname]
    if key_range is not None:
        print(f"    Marking responders {responders}...")
        proc_mess = run_mess_client(
            remotec,
            base,
            repo,
            leader=str(FORCE_LEADER),
            key_range=key_range,
            responder=",".join(list(map(str, responders))),
        )
        mout, merr = proc_mess.communicate()
        with open(f"{runlog_path}/Bodega{midfix_str}.m.out", "wb") as fmout:
            fmout.write(mout)
        with open(f"{runlog_path}/Bodega{midfix_str}.m.err", "wb") as fmerr:
            fmerr.write(merr)
        time.sleep(5)

    # client-side configs
    client_config = "+".join(CLIENT_CONFIGS)

    # start benchmarking clients
    proc_clients = run_bench_clients(
        remotec, base, repo, confname, keyrname, config=client_config
    )

    # wait for benchmarking clients to exit
    cout, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/Bodega{midfix_str}.c.out", "wb") as fcout:
        fcout.write(cout)
    with open(f"{runlog_path}/Bodega{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating Summerset cluster...")
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    sout, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/Bodega{midfix_str}.s.out", "wb") as fsout:
        fsout.write(sout)
    with open(f"{runlog_path}/Bodega{midfix_str}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Bench round FAILED!")
        raise utils.BreakingLoops
    else:
        print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for confname in RESPONDER_CONFS:
        results[confname] = dict()
        for keyrname in KEY_RANGES:
            if confname != "def" and keyrname != "100":
                continue
            results[confname][keyrname] = dict()

            result = utils.output.gather_outputs(
                f"Bodega.{confname}.{keyrname}",
                NUM_CLIENTS,
                output_dir,
                RESULT_SECS_BEGIN,
                RESULT_SECS_END,
                0.1,
            )

            sd, sp, sj, sm = 10, 0, 0, 1
            # setting sm here to compensate for unstabilities of printing
            # things to console
            tput_list = utils.output.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
            wlat_list = utils.output.list_smoothing(
                [v for v in result["wlat_avg"] if v > 0.0], sd, sp, sj, 1 / sm
            )
            rlat_list = utils.output.list_smoothing(
                [v for v in result["rlat_avg"] if v > 0.0], sd, sp, sj, 1 / sm
            )

            results[confname][keyrname] = {
                "tput": tput_list,
                "wlat": wlat_list,
                "rlat": rlat_list,
            }

    for confname in RESPONDER_CONFS:
        for keyrname in results[confname]:
            curr_results = results[confname][keyrname]
            tput_list = curr_results["tput"]
            wlat_list = sorted(lat / 1000 for lat in curr_results["wlat"])
            rlat_list = sorted(lat / 1000 for lat in curr_results["rlat"])

            results[confname][keyrname] = {
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
                        None if len(wlat_list) == 0 else wlat_list[len(wlat_list) // 2]
                    ),
                    "p99": (
                        None if len(wlat_list) == 0 else np.percentile(wlat_list, 99)
                    ),
                    "mean": (
                        None if len(wlat_list) == 0 else sum(wlat_list) / len(wlat_list)
                    ),
                    "stdev": (
                        None if len(wlat_list) == 0 else statistics.stdev(wlat_list)
                    ),
                },
                "rlat": {
                    "sorted": rlat_list,
                    "p50": (
                        None if len(rlat_list) == 0 else rlat_list[len(rlat_list) // 2]
                    ),
                    "p99": (
                        None if len(rlat_list) == 0 else np.percentile(rlat_list, 99)
                    ),
                    "mean": (
                        None if len(rlat_list) == 0 else sum(rlat_list) / len(rlat_list)
                    ),
                    "stdev": (
                        None if len(rlat_list) == 0 else statistics.stdev(rlat_list)
                    ),
                },
            }

    return results


def print_results(results):
    for confname in RESPONDER_CONFS:
        print(f"responders {RESPONDER_CONFS[confname]}")
        for keyrname in results[confname]:
            print(f"  key_range {KEY_RANGES[keyrname]}")
            result = results[confname][keyrname]
            print(f"    tput  mean {result['tput']['mean']:7.2f}", end="")
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


def plot_responders_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 1.6),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Exper-site")

    CONF_NAMES = ["0-0", "0-1", "0-2", "0-3", "0-4"]
    CONF_LABELS = ["WI", "+MA", "+SC", "+APT", "+UT"]
    SERIES_LABEL_COLORS = {
        "wlat": ("Write", "cornflowerblue", "navy"),
        "rlat": ("Read", "lightsteelblue", "steelblue"),
    }

    BAR_WIDTH = 0.9

    xpos, xticks = 1, []
    for confname in CONF_NAMES:
        for series in SERIES_LABEL_COLORS:
            result = results[confname]["100"][series]
            label, color, ecolor = SERIES_LABEL_COLORS[series]

            bar = plt.bar(
                xpos,
                result["mean"],
                width=BAR_WIDTH,
                label=label if xpos <= 2 else None,
                color=color,
                edgecolor="0.35",
                linewidth=0,
                yerr=max(result["p99"] - result["mean"], 0.0),
                ecolor=color,
                error_kw={"elinewidth": 2, "zorder": -15},
                zorder=-10,
            )
            # plt.plot(
            #     [xpos - 0.25, xpos + 0.25],
            #     [result["p99"], result["p99"]],
            #     color=ecolor,
            #     linestyle="-",
            #     linewidth=1.5,
            #     zorder=10,
            # )

            xpos += 1

        xticks.append(xpos - 1.5)
        xpos += 1

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)
    plt.xlim((0.0, xpos - 1.0))
    plt.xticks(xticks, CONF_LABELS)
    plt.xlabel("Responders Set")

    plt.ylim((0.0, 82.0))
    plt.yticks([0, 20, 40, 60, 80], list(map(str, [0, 20, 40, 60, 80])))
    plt.ylabel("Write Lat (ms)")

    plt.legend(
        handleheight=0.8,
        handlelength=1.0,
        loc="upper left",
        bbox_to_anchor=(0.02, 1.08),
        borderpad=0.3,
        handletextpad=0.3,
        ncol=2,
        columnspacing=0.9,
        frameon=False,
        fontsize=8,
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-site.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_key_ranges_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.2, 1.6),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Exper-keys")

    KEYR_NAMES = ["0", "20", "40", "60", "80", "100"]
    KEYR_LABELS = ["0%", "20%", "40%", "60%", "80%", "100%"]
    SERIES_ORDER = [
        "wlat",
        "rlat",
    ]
    SERIES_LABEL_COLORS_MARKER_SIZE = {
        "wlat": ("Write", "navy", "lavender", "h", 4.2),
        "rlat": ("Read", "steelblue", "aliceblue", "*", 5.2),
    }

    key_ranges = list(map(int, KEYR_NAMES))
    for series in SERIES_ORDER:
        mean_list = [results["def"][keyr][series]["mean"] for keyr in KEYR_NAMES]
        stdev_list = [results["def"][keyr][series]["stdev"] for keyr in KEYR_NAMES]
        label, color, ecolor, marker, markersize = SERIES_LABEL_COLORS_MARKER_SIZE[
            series
        ]

        plt.plot(
            key_ranges,
            mean_list,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )
        plt.fill_between(
            key_ranges,
            [mean - stdev for mean, stdev in zip(mean_list, stdev_list)],
            [mean + stdev for mean, stdev in zip(mean_list, stdev_list)],
            color=ecolor,
            zorder=-10,
        )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # plt.xlim((0.0, xpos - 0.5))
    plt.xticks(key_ranges, KEYR_LABELS)
    plt.xlabel("Coverage of Keys")

    # plt.ylim((0.0, 82.0))
    # plt.yticks([0, 20, 40, 60, 80], list(map(str, [0, 20, 40, 60, 80])))
    plt.ylabel("Latency (ms)")

    plt.legend(
        handleheight=0.8,
        handlelength=1.0,
        loc="center right",
        bbox_to_anchor=(1.08, 0.4),
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=0.9,
        frameon=False,
        fontsize=8,
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-keys.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.6, 2.0),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    # swap location of Bodega
    hb = handles.pop()
    handles.insert(3, hb)
    lb = labels.pop()
    labels.insert(3, lb)

    lgd = plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.0,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=2,
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=0.9,
        frameon=False,
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
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        os.system(f"mkdir -p {args.odir}")

    if not args.plot and len(args.fetch) == 0:
        print("Doing preparation work...")
        base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
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

        try:
            fixed_keyrname = "100"
            for confname in RESPONDER_CONFS:
                if confname == "def":
                    continue
                print(
                    f"Running experiments key% = {fixed_keyrname}, conf = {confname}..."
                )
                time.sleep(3)

                bench_round(
                    remotes["host0"],
                    remotes[f"host{NUM_REPLICAS}"],
                    base,
                    repo,
                    confname,
                    fixed_keyrname,
                    runlog_path,
                )
                utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                utils.file.remove_files_in_dir(  # to free up storage space
                    f"{base}/states/{EXPER_NAME}",
                    remotes=remotes,
                )
                utils.file.clear_fs_caches(remotes=remotes)

            fixed_confname = "def"
            for keyrname in KEY_RANGES:
                print(
                    f"Running experiments key% = {keyrname}, conf = {fixed_confname}..."
                )
                time.sleep(3)

                bench_round(
                    remotes["host0"],
                    remotes[f"host{NUM_REPLICAS}"],
                    base,
                    repo,
                    fixed_confname,
                    keyrname,
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
            TOML_FILENAME, PHYS_ENV_GROUP
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

        handle, label = plot_responders_results(results, plots_dir)
        handle, label = plot_key_ranges_results(results, plots_dir)
