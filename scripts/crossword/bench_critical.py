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

EXPER_NAME = "critical"
PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_CLIENTS = 15
BATCH_INTERVAL = 1

LENGTH_SECS = 30
RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 25


class EnvSetting:
    def __init__(self, group, delay, jitter, rate):
        self.group = group
        self.delay = delay
        self.jitter = jitter
        self.rate = rate


class RoundParams:
    def __init__(
        self,
        num_replicas,
        value_size,
        put_ratio,
        env_setting,
        paxos_only,
        tags,
        read_lease=True,
    ):
        self.num_replicas = num_replicas
        self.value_size = value_size
        self.put_ratio = put_ratio
        self.env_setting = env_setting
        self.paxos_only = paxos_only
        self.tags = tags
        self.read_lease = read_lease

    def __str__(self):
        return (
            f".{self.num_replicas}.{'mixed' if isinstance(self.value_size, str) else self.value_size}."
            + f"{self.put_ratio}{'rl' if self.read_lease else ''}.{self.env_setting.group}"
        )


# fmt: off
SIZE_S = 8
SIZE_L = 128 * 1024
SIZE_MIXED = [
    (0, SIZE_L),
    (LENGTH_SECS // 6, SIZE_S),
    ((LENGTH_SECS // 6) * 2, SIZE_L),
    ((LENGTH_SECS // 6) * 3, SIZE_S),
    ((LENGTH_SECS // 6) * 4, SIZE_L),
    ((LENGTH_SECS // 6) * 5, SIZE_S),
]
SIZE_MIXED = '/'.join([f"{t}:{v}" for t, v in SIZE_MIXED])

ENV_1DC = EnvSetting(
    "1dc",
    lambda _: 1,
    lambda _: 2,
    lambda _: 1,  # no effect given the original bandwidth
)
ENV_WAN = EnvSetting(
    "wan",
    lambda _: 1,
    lambda _: 2,  
    lambda _: 0.2,  # negligible given the original WAN delay
)

ROUNDS_PARAMS = [
    RoundParams(5, SIZE_S,     50,  ENV_1DC, False, ["single"]),
    RoundParams(5, SIZE_L,     50,  ENV_1DC, False, ["single"]),
    RoundParams(5, SIZE_MIXED, 50,  ENV_1DC, False, ["single", "cluster-5", "ratio-50"]),
    RoundParams(5, SIZE_S,     50,  ENV_WAN, False, ["single"]),
    RoundParams(5, SIZE_L,     50,  ENV_WAN, False, ["single"]),
    RoundParams(5, SIZE_MIXED, 50,  ENV_WAN, False, ["single"]),
    RoundParams(3, SIZE_MIXED, 50,  ENV_1DC, True,  ["cluster-3"]),
    RoundParams(7, SIZE_MIXED, 50,  ENV_1DC, True,  ["cluster-7"]),
    RoundParams(9, SIZE_MIXED, 50,  ENV_1DC, True,  ["cluster-9"]),
    RoundParams(5, SIZE_MIXED, 10,  ENV_1DC, True,  ["ratio-10"]),
    RoundParams(5, SIZE_MIXED, 100, ENV_1DC, True,  ["ratio-100"]),
]
# fmt: on


def launch_cluster(remote0, base, repo, protocol, round_params, config=None):
    cmd = [
        "python3",
        "./scripts/distr_cluster.py",
        "-p",
        protocol,
        "-n",
        str(round_params.num_replicas),
        "-r",
        "--force_leader",
        "0",
        "-g",
        round_params.env_setting.group,
        "--me",
        "host0",
        "--file_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--file_midfix",
        str(round_params),
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
    # wait for 30 seconds to safely allow all nodes up
    # not relying on SSH-piped outputs here
    time.sleep(30)


def run_bench_clients(remote0, base, repo, protocol, round_params):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        protocol,
        "-r",
        "-g",
        round_params.env_setting.group,
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
        str(round_params.num_replicas),
        "-f",
        str(0),  # closed-loop
        "-v",
        str(round_params.value_size),
        "-w",
        str(round_params.put_ratio),
        "-l",
        str(LENGTH_SECS),
        "--norm_stdev_ratio",
        str(0.1),
        "--file_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--file_midfix",
        str(round_params),
    ]
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(remote0, base, repo, protocol, round_params, runlog_path):
    midfix_str = str(round_params)
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if round_params.read_lease:
        config += f"+sim_read_lease=true"
    if protocol == "Crossword":
        # TODO: tune this
        config += f"+b_to_d_threshold={0.08}"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, protocol, round_params, config=config
    )
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(remote0, base, repo, protocol, round_params)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(round_params.env_setting.group)
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
    for round_params in ROUNDS_PARAMS:
        midfix_str = str(round_params)
        for protocol in PROTOCOLS:
            if round_params.paxos_only and "Raft" in protocol:
                continue

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
            # models to console
            if (
                round_params.value_size == SIZE_S
                and (
                    protocol == "MultiPaxos"
                    or protocol == "Raft"
                    or protocol == "Crossword"
                )
            ) or (isinstance(round_params.value_size, str) and protocol == "Crossword"):
                sm = 1 + (round_params.put_ratio / 100)
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

    for round_params in ROUNDS_PARAMS:
        midfix_str = str(round_params)
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


def plot_single_case_results(results, round_params, plots_dir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 2.2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    midfix_str = str(round_params)
    fig = plt.figure(f"Exper-{midfix_str}")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Raft",
        "Crossword",
        "RSPaxos",
        "CRaft",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Raft": ("Raft", "lightgreen", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "RSPaxos": ("RSPaxos (f=1)", "pink", "//"),
        "CRaft": ("CRaft (f=1)", "cornsilk", "\\\\"),
    }

    # throughput
    ax1 = plt.subplot(211)

    ymaxl = 0.0
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[f"{protocol}{midfix_str}"]["tput"]
        if result["mean"] > ymaxl:
            ymaxl = result["mean"]

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

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)

    plt.ylabel(" \n ")
    ax1.yaxis.set_label_coords(-0.7, 0.5)

    if ymax is not None:
        plt.ylim(0.0, ymax["tput"] * 1.1)
    else:
        ytickmax = math.ceil(ymaxl / 10) * 10
        plt.yticks([0, ytickmax // 2, ytickmax])
        plt.ylim(0.0, ytickmax * 1.2)

    # latency
    ax2 = plt.subplot(212)

    ymaxl = 0.0
    for i, protocol in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[f"{protocol}{midfix_str}"]["lat"]
        if result["mean"] > ymaxl:
            ymaxl = result["mean"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol]
        bar = plt.bar(
            xpos,
            result["mean"],
            width=1,
            color=color,
            edgecolor="gray",
            linewidth=1.4,
            label=label,
            hatch=hatch,
            yerr=result["stdev"],
            ecolor="black",
            capsize=1,
        )

    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)

    plt.ylabel(" \n ")
    ax2.yaxis.set_label_coords(-0.7, 0.5)

    if ymax is not None:
        plt.ylim(0.0, ymax["lat"] * 1.1)
    else:
        ytickmax = math.ceil(ymaxl / 10) * 10
        plt.yticks([0, ytickmax // 2, ytickmax])
        plt.ylim(0.0, ytickmax * 1.2)

    fig.subplots_adjust(left=0.5)
    # plt.tight_layout()

    pdf_midfix = (
        f"{round_params.num_replicas}."
        + f"{'small' if round_params.value_size == SIZE_S else 'large' if round_params.value_size == SIZE_L else 'mixed'}."
        + f"{round_params.put_ratio}.{round_params.env_setting.group}"
    )
    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-{pdf_midfix}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_single_rounds_results(results, rounds_params, plots_dir):
    env_ymax = dict()
    for round_params in rounds_params:
        env_name = round_params.env_setting.group
        if env_name not in env_ymax:
            env_ymax[env_name] = {
                "tput": 0.0,
                "lat": 0.0,
            }
        for protocol in PROTOCOLS:
            midfix_str = str(round_params)
            tput_mean = results[f"{protocol}{midfix_str}"]["tput"]["mean"]
            lat_mean = results[f"{protocol}{midfix_str}"]["lat"]["mean"]
            if tput_mean > env_ymax[env_name]["tput"]:
                env_ymax[env_name]["tput"] = tput_mean
            if lat_mean > env_ymax[env_name]["lat"]:
                env_ymax[env_name]["lat"] = lat_mean

    common_plotted = False
    for round_params in rounds_params:
        if "single" in round_params.tags:
            handles, labels = plot_single_case_results(
                results,
                round_params,
                plots_dir,
                None,
                # env_ymax[round_params.env_setting.group],
            )
            if not common_plotted:
                plot_major_ylabels(["Throughput\n(reqs/s)", "Latency\n(ms)"], plots_dir)
                plot_major_legend(handles, labels, plots_dir)
                common_plotted = True


def plot_cluster_size_results(results, rounds_params, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.5, 2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-cluster_size")

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

    rounds_params.sort(key=lambda rp: rp.num_replicas)
    protocol_results = {p: [] for p in PROTOCOLS_ORDER}
    for protocol in PROTOCOLS_ORDER:
        for round_params in rounds_params:
            midfix_str = str(round_params)
            protocol_results[protocol].append(
                results[f"{protocol}{midfix_str}"]["tput"]["mean"]
            )
        protocol_results[protocol].sort(reverse=True)

    xpos = 1
    for i in range(len(rounds_params)):
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

    plt.xticks([2, 6, 10, 14], [f"n={3}", f"n={5}", f"n={7}", f"n={9}"])
    plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-cluster_size.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_write_ratio_results(results, rounds_params, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.8, 2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-write_ratio")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Crossword",
        "RSPaxos",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
        "RSPaxos": ("RSPaxos (f=1)", "pink", "//"),
    }

    rounds_params.sort(key=lambda rp: rp.num_replicas)
    protocol_results = {p: [] for p in PROTOCOLS_ORDER}
    for protocol in PROTOCOLS_ORDER:
        for round_params in rounds_params:
            midfix_str = str(round_params)
            protocol_results[protocol].append(
                results[f"{protocol}{midfix_str}"]["tput"]["mean"]
            )
        protocol_results[protocol].sort(reverse=True)

    xpos = 1
    for i in range(len(rounds_params)):
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

    plt.xticks([2, 6, 10], [f"{10}%", f"{50}%", f"{100}%"])
    # plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-write_ratio.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_major_ylabels(ylabels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.5, 2.2),
            "font.size": 12,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Ylabels")

    assert len(ylabels) == 2

    ax1 = plt.subplot(211)
    plt.ylabel(ylabels[0])
    for spine in ax1.spines.values():
        spine.set_visible(False)
    plt.tick_params(bottom=False, labelbottom=False, left=False, labelleft=False)

    ax2 = plt.subplot(212)
    plt.ylabel(ylabels[1])
    for spine in ax2.spines.values():
        spine.set_visible(False)
    plt.tick_params(bottom=False, labelbottom=False, left=False, labelleft=False)

    fig.subplots_adjust(left=0.5)

    fig.align_labels()

    pdf_name = f"{plots_dir}/ylabels-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_major_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (5.6, 0.5),
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
        handlelength=1.0,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=len(labels),
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=0.9,
    )
    for rec in lgd.get_texts():
        if "f=1" in rec.get_text():
            rec.set_fontstyle("italic")
        # if "Crossword" in rec.get_text():
        #     rec.set_fontweight("bold")

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_minor_legend(handles, labels, plots_dir):
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

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME}-minor.pdf"
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
        base, repo, _, remotes_1dc, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, "1dc"
        )
        _, _, _, remotes_wan, _, _ = utils.config.parse_toml_file(TOML_FILENAME, "wan")
        remotes = {"1dc": remotes_1dc, "wan": remotes_wan}
        for group in ("1dc", "wan"):
            utils.proc.check_enough_cpus(MIN_HOST0_CPUS, remote=remotes[group]["host0"])
            utils.proc.kill_all_distr_procs(group)
            utils.file.do_cargo_build(
                True, cd_dir=f"{base}/{repo}", remotes=remotes[group]
            )
            utils.file.clear_fs_caches(remotes=remotes[group])

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        last_env_group = None
        for round_params in ROUNDS_PARAMS:
            print(f"Running experiments {round_params}...")

            this_env = round_params.env_setting
            if this_env.group != last_env_group:
                print("Setting tc netem qdiscs...")
                utils.net.set_tc_qdisc_netems_main(
                    this_env.delay,
                    this_env.jitter,
                    this_env.rate,
                    involve_ifb=True,
                    remotes=remotes[this_env.group],
                )
                last_env_group = this_env.group

            for protocol in PROTOCOLS:
                if round_params.paxos_only and "Raft" in protocol:
                    continue
                time.sleep(10)
                bench_round(
                    remotes[this_env.group]["host0"],
                    base,
                    repo,
                    protocol,
                    round_params,
                    runlog_path,
                )
                utils.proc.kill_all_distr_procs(this_env.group)
                utils.file.remove_files_in_dir(  # to free up storage space
                    f"{base}/states/{EXPER_NAME}",
                    remotes=remotes[this_env.group],
                )
                utils.file.clear_fs_caches(remotes=remotes[this_env.group])

        print("Clearing tc netem qdiscs...")
        for group in ("1dc", "wan"):
            utils.net.clear_tc_qdisc_netems_main(remotes=remotes[group])

        print("Fetching client output logs...")
        for group in ("1dc", "wan"):
            utils.file.fetch_files_of_dir(
                remotes[group]["host0"], f"{base}/output/{EXPER_NAME}", output_path
            )

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = collect_outputs(output_dir)
        print_results(results)

        single_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "single" in t, rp.tags))
        ]
        plot_single_rounds_results(results, single_rounds, plots_dir)

        cluster_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "cluster" in t, rp.tags))
        ]
        cluster_rounds.sort(key=lambda rp: rp.num_replicas)
        handles, labels = plot_cluster_size_results(results, cluster_rounds, plots_dir)
        plot_minor_legend(handles, labels, plots_dir)

        ratio_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "ratio" in t, rp.tags))
        ]
        ratio_rounds.sort(key=lambda rp: rp.put_ratio)
        plot_write_ratio_results(results, ratio_rounds, plots_dir)
