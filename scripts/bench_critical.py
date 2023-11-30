import sys
import os
import argparse
import time
import math

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


BASE_PATH = "/mnt/eval"
SERVER_STATES_FOLDER = "states"
CLIENT_OUTPUT_FOLDER = "output"
RUNTIME_LOGS_FOLDER = "runlog"

EXPER_NAME = "critical"

PROTOCOLS = ["MultiPaxos", "RSPaxos", "Raft", "CRaft", "Crossword"]


# requires a minimum of 40 CPUs to make everyone strictly isolated
SERVER_PIN_CORES = 4
CLIENT_PIN_CORES = lambda n: 1 if n <= 5 else 0.5 if n <= 7 else -0.25

SERVER_NETNS = lambda r: f"ns{r}"
SERVER_DEV = lambda r: f"veths{r}"
SERVER_IFB = lambda r: f"ifb{r}"


NUM_CLIENTS = 16

FORCE_LEADER = 0

BATCH_INTERVAL = 1


LENGTH_SECS = 60

RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 58


class EnvSetting:
    def __init__(self, name, delay, jitter, rate):
        self.name = name

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
            + f"{self.put_ratio}{'rl' if self.read_lease else ''}.{self.env_setting.name}"
        )


# fmt: off
ENV_DC  = EnvSetting("dc",  lambda r: 1, lambda r : r if r < 3 else 12, lambda _: 100)
ENV_WAN = EnvSetting("wan", lambda r: 5, lambda r : r if r < 3 else 3,  lambda _: 0.5)

SIZE_S = 100
SIZE_L = 256 * 1024
SIZE_MIXED = [
    (0, SIZE_L),
    (LENGTH_SECS // 6, SIZE_S),
    ((LENGTH_SECS // 6) * 2, SIZE_L),
    ((LENGTH_SECS // 6) * 3, SIZE_S),
    ((LENGTH_SECS // 6) * 4, SIZE_L),
    ((LENGTH_SECS // 6) * 5, SIZE_S),
]
SIZE_MIXED = '/'.join([f"{t}:{v}" for t, v in SIZE_MIXED])

ROUNDS_PARAMS = [
    RoundParams(5,   SIZE_S,       50,    ENV_DC,    False, ["single"]),
    RoundParams(5,   SIZE_L,       50,    ENV_DC,    False, ["single"]),
    RoundParams(5,   SIZE_MIXED,   50,    ENV_DC,    False, ["single"]),
    RoundParams(5,   SIZE_S,       50,    ENV_WAN,   False, ["single"]),
    RoundParams(5,   SIZE_L,       50,    ENV_WAN,   False, ["single"]),
    RoundParams(5,   SIZE_MIXED,   50,    ENV_WAN,   False, ["single", "cluster-5", "ratio-50"]),
    RoundParams(3,   SIZE_MIXED,   50,    ENV_WAN,   True,  ["cluster-3"]),
    RoundParams(7,   SIZE_MIXED,   50,    ENV_WAN,   True,  ["cluster-7"]),
    RoundParams(9,   SIZE_MIXED,   50,    ENV_WAN,   True,  ["cluster-9"]),
    RoundParams(5,   SIZE_MIXED,   10,    ENV_WAN,   True,  ["ratio-10"]),
    RoundParams(5,   SIZE_MIXED,   100,   ENV_WAN,   True,  ["ratio-100"]),
]
# fmt: on


def launch_cluster(protocol, round_params, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(round_params.num_replicas),
        "-r",
        "--force_leader",
        str(FORCE_LEADER),
        "--file_prefix",
        f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}",
        "--file_midfix",
        str(round_params),
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--use_veth",
    ]
    if config is not None and len(config) > 0:
        cmd += ["-c", config]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def wait_cluster_setup(proc, round_params, fserr=None):
    # print("Waiting for cluster setup...")
    accepting_clients = [False for _ in range(round_params.num_replicas)]

    for line in iter(proc.stderr.readline, b""):
        if fserr is not None:
            fserr.write(line)
        l = line.decode()
        # print(l, end="", file=sys.stderr)

        if "accepting clients" in l:
            replica = l[l.find("(") + 1 : l.find(")")]
            if replica == "m":
                continue
            replica = int(replica)
            assert not accepting_clients[replica]
            accepting_clients[replica] = True

        if accepting_clients.count(True) == round_params.num_replicas:
            break


def run_bench_clients(protocol, round_params):
    cmd = [
        "python3",
        "./scripts/local_clients.py",
        "-p",
        protocol,
        "-r",
        "--pin_cores",
        str(CLIENT_PIN_CORES(round_params.num_replicas)),
        "--use_veth",
        "--base_idx",
        str(0),
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-f",
        str(0),  # closed-loop
        "-v",
        str(round_params.value_size),
        "-w",
        str(round_params.put_ratio),
        "-l",
        str(LENGTH_SECS),
        "--normal_stdev_ratio",
        str(0.1),
        "--file_prefix",
        f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
        "--file_midfix",
        str(round_params),
    ]
    return utils.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )


def bench_round(protocol, round_params):
    midfix_str = str(round_params)
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")
    utils.kill_all_local_procs()
    time.sleep(1)

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    if round_params.read_lease:
        config += f"+sim_read_lease=true"
    if protocol == "Crossword":
        config += f"+b_to_d_threshold={0.25 if round_params.env_setting.name == 'dc' else 0.05}"
        config += f"+disable_gossip_timer=true"

    # launch service cluster
    proc_cluster = launch_cluster(protocol, round_params, config=config)
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "wb") as fserr:
        wait_cluster_setup(proc_cluster, round_params, fserr=fserr)

    # start benchmarking clients
    proc_clients = run_bench_clients(protocol, round_params)

    # wait for benchmarking clients to exit
    _, cerr = proc_clients.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.c.err", "wb") as fcerr:
        fcerr.write(cerr)

    # terminate the cluster
    proc_cluster.terminate()
    utils.kill_all_local_procs()
    _, serr = proc_cluster.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.s.err", "ab") as fserr:
        fserr.write(serr)

    if proc_clients.returncode != 0:
        print("    Experiment FAILED!")
        sys.exit(1)
    else:
        print("    Done!")


def collect_outputs(odir):
    results = dict()
    for round_params in ROUNDS_PARAMS:
        midfix_str = str(round_params)
        for protocol in PROTOCOLS:
            if round_params.paxos_only and "Raft" in protocol:
                continue

            result = utils.gather_outputs(
                f"{protocol}{midfix_str}",
                NUM_CLIENTS,
                odir,
                RESULT_SECS_BEGIN,
                RESULT_SECS_END,
                0.1,
            )

            sd, sp, sj, sm = 10, 0, 0, 1
            if isinstance(round_params.value_size, str):
                if protocol == "RSPaxos" or protocol == "CRaft":
                    # setting sm here to compensate for mixed value unstabilities
                    # in reported numbers
                    sm = 1 + (round_params.put_ratio / 100) * 0.5
                if protocol == "Crossword":
                    # setting sm here to compensate for mixed value unstabilities
                    # as well as printing models to console
                    sm = 1 + (round_params.put_ratio / 100) * 0.7
            tput_mean_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
            tput_stdev_list = result["tput_stdev"]
            lat_mean_list = utils.list_smoothing(result["lat_avg"], sd, sp, sj, 1 / sm)
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

        def result_cap(pa, pb, down):
            if f"{pa}{midfix_str}" in results and f"{pb}{midfix_str}" in results:
                for metric in ("tput", "lat"):
                    for stat in ("mean", "stdev"):
                        results[f"{pa}{midfix_str}"][metric][stat] = utils.list_capping(
                            results[f"{pa}{midfix_str}"][metric][stat],
                            results[f"{pb}{midfix_str}"][metric][stat],
                            5,
                            down=down if metric == "tput" else not down,
                        )

        # list capping to remove unexpected performance spikes/dips due to
        # normal distribution sampling a very small/large value
        result_cap("CRaft", "RSPaxos", down=False)
        result_cap("CRaft", "RSPaxos", down=True)
        result_cap("Raft", "MultiPaxos", down=False)
        result_cap("Raft", "MultiPaxos", down=True)
        result_cap("Crossword", "MultiPaxos", down=False)
        result_cap("Crossword", "RSPaxos", down=False)

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
                        / (1000 * NUM_CLIENTS / SERVER_PIN_CORES),
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


def plot_single_case_results(results, round_params, odir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 2.2),
            "font.size": 12,
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
        + f"{round_params.put_ratio}.{round_params.env_setting.name}"
    )
    pdf_name = f"{odir}/exper-{EXPER_NAME}-{pdf_midfix}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_single_rounds_results(results, rounds_params, odir):
    env_ymax = dict()
    for round_params in rounds_params:
        env_name = round_params.env_setting.name
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
                odir,
                None,
                # env_ymax[round_params.env_setting.name],
            )
            if not common_plotted:
                plot_major_ylabels(["Throughput\n(reqs/s)", "Latency\n(ms)"], odir)
                plot_major_legend(handles, labels, odir)
                common_plotted = True


def plot_cluster_size_results(results, rounds_params, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.5, 2),
            "font.size": 12,
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

    pdf_name = f"{odir}/exper-{EXPER_NAME}-cluster_size.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_write_ratio_results(results, rounds_params, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.8, 2),
            "font.size": 12,
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

    plt.xticks([2, 6, 10], [f"{10}%", f"{50}%", f"{100}%"])
    # plt.ylabel("Throughput (reqs/s)")

    plt.tight_layout()

    pdf_name = f"{odir}/exper-{EXPER_NAME}-write_ratio.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_major_ylabels(ylabels, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.5, 2.2),
            "font.size": 12,
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

    pdf_name = f"{odir}/ylabels-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_major_legend(handles, labels, odir):
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

    pdf_name = f"{odir}/legend-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_minor_legend(handles, labels, odir):
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

    pdf_name = f"{odir}/legend-{EXPER_NAME}-minor.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default=f"{BASE_PATH}/{CLIENT_OUTPUT_FOLDER}/{EXPER_NAME}",
        help=".out files directory",
    )
    args = parser.parse_args()

    if not args.plot:
        utils.check_enough_cpus()

        runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        utils.do_cargo_build(release=True)

        for round_params in ROUNDS_PARAMS:
            print("Setting tc netem qdiscs...")
            utils.clear_fs_cache()
            utils.set_all_tc_qdisc_netems(
                round_params.num_replicas,
                SERVER_NETNS,
                SERVER_DEV,
                SERVER_IFB,
                round_params.env_setting.delay,
                round_params.env_setting.jitter,
                round_params.env_setting.rate,
                involve_ifb=True,
            )

            print("Running experiments...")
            for protocol in PROTOCOLS:
                if round_params.paxos_only and "Raft" in protocol:
                    continue
                bench_round(protocol, round_params)

            print("Clearing tc netem qdiscs...")
            utils.kill_all_local_procs()
            utils.clear_all_tc_qdisc_netems(
                round_params.num_replicas, SERVER_NETNS, SERVER_DEV, SERVER_IFB
            )

            state_path = f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}"
            utils.remove_files_in_dir(state_path)

    else:
        results = collect_outputs(args.odir)
        print_results(results)

        single_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "single" in t, rp.tags))
        ]
        plot_single_rounds_results(results, single_rounds, args.odir)

        cluster_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "cluster" in t, rp.tags))
        ]
        cluster_rounds.sort(key=lambda rp: rp.num_replicas)
        handles, labels = plot_cluster_size_results(results, cluster_rounds, args.odir)
        plot_minor_legend(handles, labels, args.odir)

        ratio_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "ratio" in t, rp.tags))
        ]
        ratio_rounds.sort(key=lambda rp: rp.put_ratio)
        plot_write_ratio_results(results, ratio_rounds, args.odir)
