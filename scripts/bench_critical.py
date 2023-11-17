import sys
import os
import argparse
import time
import statistics

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

BATCH_INTERVAL = 1


class RoundParams:
    def __init__(
        self,
        num_replicas,
        value_dist,
        value_size,
        put_ratio,
        delay,
        jitter,
        rate,
        paxos_only,
        tags,
    ):
        self.num_replicas = num_replicas

        self.value_dist = value_dist
        self.value_size = value_size

        self.put_ratio = put_ratio

        self.delay = delay
        self.jitter = jitter
        self.rate = rate

        self.paxos_only = paxos_only
        self.tags = tags

    def __str__(self):
        return (
            f".{self.num_replicas}.{'n' if self.value_dist == 'normal' else 'u'}.{self.value_size}."
            + f"{self.put_ratio}.{self.delay}.{self.jitter}.{self.rate}"
        )


# fmt: off
ROUNDS_PARAMS = [
    RoundParams(5,   "normal",  128,          100,   0,  0, 100,   False, ["single"]),
    # RoundParams(5,   "normal",  256 * 1024,   50,    1,  0, 100,   False, ["single"]),
    # RoundParams(5,   "uniform", 300 * 1024,   50,    1,  0, 100,   False, ["single"]),
    # RoundParams(5,   "normal",  128,          50,    10, 5, 1,     False, ["single"]),
    # RoundParams(5,   "normal",  256 * 1024,   50,    10, 5, 1,     False, ["single"]),
    # RoundParams(5,   "uniform", 300 * 1024,   50,    10, 5, 1,     False, ["single", "cluster-5", "ratio-50"]),
    # RoundParams(3,   "uniform", 300 * 1024,   50,    10, 5, 1,     True,  ["cluster-3"]),
    # RoundParams(7,   "uniform", 300 * 1024,   50,    10, 5, 1,     True,  ["cluster-7"]),
    # RoundParams(9,   "uniform", 300 * 1024,   50,    10, 5, 1,     True,  ["cluster-9"]),
    # RoundParams(5,   "uniform", 300 * 1024,   5,     10, 5, 1,     True,  ["ratio-5"]),
    # RoundParams(5,   "uniform", 300 * 1024,   100,   10, 5, 1,     True,  ["ratio-100"]),
]
# fmt: on


LENGTH_SECS = 20

RESULT_SECS_BEGIN = 5
RESULT_SECS_END = 18


def launch_cluster(protocol, round_params, config=None):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(round_params.num_replicas),
        "-r",
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
    ]
    # if round_params.value_dist == "normal":
    #     cmd += [
    #         "--normal_stdev_ratio",
    #         str(0.1),
    #     ]
    # else:
    #     cmd += [
    #         "--unif_interval_ms",
    #         str(1),
    #         "--unif_upper_bound",
    #         str(round_params.value_size),
    #     ]
    cmd += [
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
    if protocol == "Crossword":
        config += f"+b_to_d_threshold={0.2}"
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
        for protocol in PROTOCOLS:
            if round_params.paxos_only and "Raft" in protocol:
                continue

            midfix_str = str(round_params)
            result = utils.gather_outputs(
                f"{protocol}{midfix_str}",
                NUM_CLIENTS,
                odir,
                RESULT_SECS_BEGIN,
                RESULT_SECS_END,
                0.1,
            )

            sd, sp, sj, sm = 20, 0, 0, 1
            tput_list = utils.list_smoothing(result["tput_sum"], sd, sp, sj, sm)
            lat_list = utils.list_smoothing(result["lat_avg"], sd, sp, sj, sm)

            results[f"{protocol}{midfix_str}"] = {
                "tput": {
                    "mean": sum(tput_list) / len(tput_list),
                    "stdev": statistics.stdev(tput_list),
                },
                "lat": {
                    "mean": (sum(lat_list) / len(lat_list)) / 1000,
                    "stdev": (statistics.stdev(lat_list)) / 1000,
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


def plot_single_case_results(results, round_params, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4, 3),
            "font.size": 12,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "MultiPaxos",
        "Raft",
        "RSPaxos",
        "CRaft",
        "Crossword",
    ]
    PROTOCOLS_LABEL_COLOR_HATCH = {
        "MultiPaxos": ("MultiPaxos", "darkgray", None),
        "Raft": ("Raft", "lightgreen", None),
        "RSPaxos": ("RSPaxos", "pink", "//"),
        "CRaft": ("CRaft", "cornsilk", "\\\\"),
        "Crossword": ("Crossword", "lightsteelblue", "xx"),
    }

    # throughput
    ax1 = plt.subplot(211)

    for i, protocol_with_midfix in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol_with_midfix]["tput"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol_with_midfix]
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

    plt.ylabel("Throughput (reqs/s)")

    # latency
    ax2 = plt.subplot(212)

    for i, protocol_with_midfix in enumerate(PROTOCOLS_ORDER):
        xpos = i + 1
        result = results[protocol_with_midfix]["lat"]

        label, color, hatch = PROTOCOLS_LABEL_COLOR_HATCH[protocol_with_midfix]
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

    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tick_params(bottom=False, labelbottom=False)

    plt.ylabel("Latency (ms)")

    plt.tight_layout()

    pdf_name = f"{odir}/exper-{EXPER_NAME}-{str(round_params)}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_cluster_size_results(results, rounds_params, odir):
    pass


def plot_write_ratio_results(results, rounds_params, odir):
    pass


def plot_legend(handles, labels, odir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.4, 2.2),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    handles.insert(-2, matplotlib.lines.Line2D([], [], linestyle=""))
    labels.insert(-2, "")  # insert spacing between groups
    lgd = plt.legend(
        handles,
        labels,
        handleheight=1.2,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
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


if __name__ == "__main__":
    utils.check_proper_cwd()
    utils.check_enough_cpus()

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
        runlog_path = f"{BASE_PATH}/{RUNTIME_LOGS_FOLDER}/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        utils.do_cargo_build(release=True)

        for round_params in ROUNDS_PARAMS:
            print("Setting tc netem qdiscs...")
            utils.clear_fs_cache()
            # utils.set_all_tc_qdisc_netems(
            #     round_params.num_replicas,
            #     SERVER_NETNS,
            #     SERVER_DEV,
            #     SERVER_IFB,
            #     lambda _: round_params.delay,
            #     lambda _: round_params.jitter,
            #     lambda _: round_params.rate,
            #     involve_ifb=True,
            # )

            print("Running experiments...")
            for protocol in PROTOCOLS:
                if round_params.paxos_only and "Raft" in protocol:
                    continue
                bench_round(protocol, round_params)

            print("Clearing tc netem qdiscs...")
            utils.kill_all_local_procs()
            # utils.clear_all_tc_qdisc_netems(
            #     round_params.num_replicas, SERVER_NETNS, SERVER_DEV, SERVER_IFB
            # )

            state_path = f"{BASE_PATH}/{SERVER_STATES_FOLDER}/{EXPER_NAME}"
            utils.remove_files_in_dir(state_path)

    else:
        results = collect_outputs(args.odir)
        print_results(results)

        legend_plotted = False
        for round_params in ROUNDS_PARAMS:
            if "single" in round_params.tags:
                handles, labels = plot_single_case_results(
                    results, round_params, args.odir
                )
                if not legend_plotted:
                    plot_legend(handles, labels, args.odir)
                    legend_plotted = True

        cluster_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "cluster" in t, rp.tags))
        ]
        cluster_rounds.sort(key=lambda rp: rp.num_replicas)
        plot_cluster_size_results(results, cluster_rounds, args.odir)

        ratio_rounds = [
            rp for rp in ROUNDS_PARAMS if any(map(lambda t: "ratio" in t, rp.tags))
        ]
        ratio_rounds.sort(key=lambda rp: rp.put_ratio)
        plot_write_ratio_results(results, ratio_rounds, args.odir)
