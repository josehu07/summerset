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

EXPER_NAME = "load_sizes"
PROTOCOLS = ["MultiPaxos", "RSPaxos", "Crossword"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
VALUE_SIZES = [8, 128, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024]
VALUE_SIZES_STRS = ["8B", "128B", "1K", "4K", "16K", "64K", "128K", "256K"]
PUT_RATIO = 100

LENGTH_SECS = 40
RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 35

NETEM_MEAN = lambda _: 1
NETEM_JITTER = lambda _: 2
NETEM_RATE = lambda _: 1  # no effect given the original bandwidth


def launch_cluster(remote0, base, repo, protocol, value_size, config=None):
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
        "--file_midfix",
        f".{value_size}",
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


def run_bench_clients(remote0, base, repo, protocol, value_size):
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
        str(value_size),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--norm_stdev_ratio",
        str(0.1),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        f".{value_size}",
    ]
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(remote0, base, repo, protocol, value_size, runlog_path):
    midfix_str = f".{value_size}"
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if protocol == "Crossword":
        config += f"+b_to_d_threshold={0.08}"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, protocol, value_size, config=config
    )
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol, value_size)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "wb") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(output_dir):
    results = dict()
    for value_size in VALUE_SIZES:
        midfix_str = f".{value_size}"
        for protocol in PROTOCOLS:
            result = utils.output.gather_outputs(
                f"{protocol}{midfix_str}",
                NUM_CLIENTS,
                output_dir,
                RESULT_SECS_BEGIN,
                RESULT_SECS_END,
                0.1,
            )

            sd, sp, sj, sm = 10, 0, 0, 1
            # setting sm here to compensate for unstabilities of printing
            # models to console and other extra work in particular cases
            if (
                value_size < 4 * 1024 and protocol == "MultiPaxos"
            ) or protocol == "Crossword":
                sm = 1 + PUT_RATIO / 100
            if value_size == 1024 and protocol == "MultiPaxos":
                sm = 1 + (PUT_RATIO / 2) / 100
            if value_size > 16 * 1024 and protocol == "RSPaxos":
                sm = 1 + (PUT_RATIO / 2) / 100
            tput_mean_list = utils.output.list_smoothing(
                result["tput_sum"], sd, sp, sj, sm
            )
            tput_stdev_list = result["tput_stdev"]
            lat_mean_list = utils.output.list_smoothing(
                result["lat_avg"], sd, sp, sj, 1 / sm
            )
            lat_stdev_list = result["lat_stdev"]

            results[f"{protocol}{midfix_str}"] = {
                "tput": {
                    "mean": tput_mean_list,
                    "stdev": tput_stdev_list,
                },
                "lat": {
                    "mean": lat_mean_list,
                    "stdev": lat_stdev_list,
                },
            }

    for value_size in VALUE_SIZES:
        midfix_str = f".{value_size}"
        for protocol in PROTOCOLS:
            if f"{protocol}{midfix_str}" in results:
                tput_mean_list = results[f"{protocol}{midfix_str}"]["tput"]["mean"]
                tput_stdev_list = results[f"{protocol}{midfix_str}"]["tput"]["stdev"]
                lat_mean_list = results[f"{protocol}{midfix_str}"]["lat"]["mean"]
                lat_stdev_list = results[f"{protocol}{midfix_str}"]["lat"]["stdev"]

                results[f"{protocol}{midfix_str}"] = {
                    "tput": {
                        "mean": sum(tput_mean_list) / len(tput_mean_list),
                        "stdev": (
                            sum(map(lambda s: s**2, tput_stdev_list))
                            / len(tput_stdev_list)
                        )
                        ** 0.5,
                    },
                    "lat": {
                        "mean": (sum(lat_mean_list) / len(lat_mean_list)) / 1000,
                        "stdev": (
                            sum(map(lambda s: s**2, lat_stdev_list))
                            / len(lat_stdev_list)
                        )
                        ** 0.5
                        / (1000 * NUM_CLIENTS / CLIENT_PIN_CORES),
                    },
                }

    return results


def print_results(results):
    for protocol_with_midfix, result in results.items():
        print(protocol_with_midfix)
        print(
            f"  tput  mean {result['tput']['mean']:7.2f}  stdev {result['tput']['stdev']:7.2f}"
            + f"  lat  mean {result['lat']['mean']:7.2f}  stdev {result['lat']['stdev']:7.2f}"
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

        for value_size in VALUE_SIZES:
            print(f"Running experiments {value_size}...")
            for protocol in PROTOCOLS:
                time.sleep(5)
                bench_round(
                    remotes["host0"],
                    base,
                    repo,
                    protocol,
                    value_size,
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

        results = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_results(results, plots_dir)
        plot_legend(handles, labels, plots_dir)
