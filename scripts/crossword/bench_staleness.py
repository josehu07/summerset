import os
import argparse
import time
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "reg"

EXPER_NAME = "staleness"
PROTOCOL_GAPS = [
    ("MultiPaxos", None),
    ("Crossword", 0),
    ("Crossword", 100),
    ("Crossword", 200),
]

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 20
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 15
BATCH_INTERVAL = 1
VALUE_SIZE = 4096
PUT_RATIO = 100
NUM_KEYS_LIST = [(2**i) for i in range(6)]

LENGTH_SECS = 45
RESULT_SECS_BEGIN = 10
RESULT_SECS_END = 35


def round_midfix_str(gossip_gap, num_keys):
    return f".{0 if gossip_gap is None else gossip_gap}.{num_keys}"


def launch_cluster(remote0, base, repo, protocol, midfix_str, config=None):
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
        "--states_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--states_midfix",
        midfix_str,
        "--pin_cores",
        str(SERVER_PIN_CORES),
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


def wait_cluster_setup(sleep_secs=20):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(remote0, base, repo, protocol, num_keys, midfix_str):
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
        "--skip_build",
        "bench",
        "-n",
        str(NUM_CLIENTS),
        "-d",
        str(NUM_REPLICAS),
        "-f",
        str(0),  # closed-loop
        "-v",
        str(VALUE_SIZE),
        "-k",
        str(num_keys),
        "-w",
        str(PUT_RATIO),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        midfix_str,
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


def bench_round(
    remote0, base, repo, protocol, gossip_gap, num_keys, runlog_path
):
    midfix_str = round_midfix_str(gossip_gap, num_keys)
    print(f"  {EXPER_NAME}  {protocol:<10s}{midfix_str}")

    config = f"batch_interval_ms={BATCH_INTERVAL}"
    config += "+record_breakdown=true"
    config += "+record_value_ver=true"
    if protocol == "Crossword":
        config += "+init_assignment='1'"
        config += f"+gossip_tail_ignores={gossip_gap}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, protocol, midfix_str, config=config
    )
    wait_cluster_setup()

    # start benchmarking clients
    proc_clients = run_bench_clients(
        remote0, base, repo, protocol, num_keys, midfix_str
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
        raise utils.BreakingLoops
    else:
        print("    Bench round done!")


def collect_ver_stats(runlog_dir):
    ver_stats = dict()

    def get_node_id(line):
        return int(line[line.index("(") + 1 : line.index(")")])

    for num_keys in NUM_KEYS_LIST:
        for protocol, gossip_gap in PROTOCOL_GAPS:
            midfix_str = round_midfix_str(gossip_gap, num_keys)

            candidates = set(range(NUM_REPLICAS))
            leader, sec0 = None, None
            result = [{"secs": [], "vers": []} for _ in range(NUM_REPLICAS)]
            with open(
                f"{runlog_dir}/{protocol}{midfix_str}.s.err", "r"
            ) as flog:
                for line in flog:
                    if "becoming a leader" in line:
                        if leader is not None:
                            raise RuntimeError(
                                "multiple leader step-up detected"
                            )
                        leader = get_node_id(line)
                    elif "ver of" in line:
                        node = get_node_id(line)
                        if node not in candidates:
                            continue

                        segs = line.strip().split()
                        sec = float(segs[-4]) / 1000.0
                        if sec0 is None:
                            sec0 = sec
                        sec -= sec0
                        if sec < RESULT_SECS_BEGIN:
                            continue

                        ver = int(segs[-1])
                        result[node]["secs"].append(sec)
                        result[node]["vers"].append(ver)

                        if sec > RESULT_SECS_END:
                            if leader is None:
                                raise RuntimeError(
                                    "leader step-up not detected"
                                )
                            candidates.remove(node)
                            ver_stats[f"{protocol}{midfix_str}"] = {
                                "leader": leader,
                                "result": result,
                            }
                            break

    diff_stats = dict()
    for num_keys in NUM_KEYS_LIST:
        for protocol, gossip_gap in PROTOCOL_GAPS:
            midfix_str = round_midfix_str(gossip_gap, num_keys)

            leader, result = (
                ver_stats[f"{protocol}{midfix_str}"]["leader"],
                ver_stats[f"{protocol}{midfix_str}"]["result"],
            )
            assert leader >= 0 and leader < len(result)

            dresult = {"secs": [], "diffs": []}
            for i, lsec in enumerate(result[leader]["secs"]):
                lver = result[leader]["vers"][i]
                diffs = []
                for node in range(NUM_REPLICAS):
                    if node != leader:
                        for j, fsec in enumerate(result[node]["secs"]):
                            fver = result[node]["vers"][j]
                            if abs(fsec - lsec) < 1.0:  # allow an error margin
                                diffs.append(lver - fver)
                                break
                if len(diffs) == NUM_REPLICAS - 1:
                    # remove out-of-quorum stragglers impact
                    diffs = sorted(diffs)[: NUM_REPLICAS // 2]
                    avg_diff = max(sum(diffs) / len(diffs), 0.0)
                    dresult["secs"].append(lsec)
                    dresult["diffs"].append(avg_diff)

            mid_diffs = sorted(dresult["diffs"])[1:-1]
            assert len(mid_diffs) > 0
            avg_diff = sum(mid_diffs) / len(mid_diffs)
            diff_stats[f"{protocol}{midfix_str}"] = {
                "avg": avg_diff,
                "result": dresult,
            }

    return ver_stats, diff_stats


def print_results(ver_stats, diff_stats):
    for protocol_with_midfix in ver_stats:
        print(protocol_with_midfix)
        _leader, _result, davg, dresult = (
            ver_stats[protocol_with_midfix]["leader"],
            ver_stats[protocol_with_midfix]["result"],
            diff_stats[protocol_with_midfix]["avg"],
            diff_stats[protocol_with_midfix]["result"],
        )

        # for node in range(len(result)):
        #     print(f"  {node} {'leader' if node == leader else 'follower':<8s}")
        #     print("    secs", end="")
        #     for sec in result[node]["secs"]:
        #         print(f" {sec:>5.1f}", end="")
        #     print()
        #     print("    vers", end="")
        #     for ver in result[node]["vers"]:
        #         print(f" {ver:>5d}", end="")
        #     print()

        print("    secs", end="")
        for sec in dresult["secs"]:
            print(f" {sec:>5.1f}", end="")
        print(f" {'avg':>5s}")
        print("   diffs", end="")
        for diff in dresult["diffs"]:
            print(f" {diff:>5.1f}", end="")
        print(f" {davg:>5.1f}")


def plot_staleness(diff_stats, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.4, 1.6),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PROTOCOLS_ORDER = [
        "RSPaxos.None",
        "Crossword.200",
        "Crossword.100",
        "Crossword.0",
        "MultiPaxos.None",
    ]
    PROTOCOLS_LABEL_COLOR_MARKER_ZORDER = {
        "MultiPaxos.None": ("MultiPaxos", "dimgray", "v", 0),
        "Crossword.200": ("Cw (800KB)", "royalblue", "p", 5),
        "Crossword.100": ("Cw (400KB)", "steelblue", "o", 10),
        "Crossword.0": ("Cw (0)", "lightsteelblue", "2", 5),
        "RSPaxos.None": ("RSPaxos", "red", "x", 0),
    }
    TIME_INTERVAL_UNIT = 3
    MARKER_SIZE = 4

    xmin = TIME_INTERVAL_UNIT - 0.5
    ymax, protocol_ys = 0.0, dict()
    for protocol, gossip_gap in PROTOCOL_GAPS + [("RSPaxos", None)]:
        ys = None
        if protocol != "RSPaxos":
            ys = [
                diff_stats[f"{protocol}{round_midfix_str(gossip_gap, k)}"][
                    "avg"
                ]
                for k in NUM_KEYS_LIST
            ]
            if max(ys) > ymax:
                ymax = max(ys)
        else:
            ys = [ymax * 1.6 for _ in NUM_KEYS_LIST]
        ys.sort(reverse=True)
        protocol_ys[f"{protocol}.{gossip_gap}"] = ys

    for protocol_with_gap in PROTOCOLS_ORDER:
        label, color, marker, zorder = PROTOCOLS_LABEL_COLOR_MARKER_ZORDER[
            protocol_with_gap
        ]
        plt.plot(
            [k * TIME_INTERVAL_UNIT for k in NUM_KEYS_LIST],
            protocol_ys[protocol_with_gap],
            color=color,
            linewidth=1.2,
            marker=marker,
            markersize=(
                MARKER_SIZE
                if ".0" not in protocol_with_gap
                else MARKER_SIZE + 3
            ),
            label=label,
            zorder=zorder,
        )

    def draw_yaxis_break(yloc):
        ypb, ypt = yloc - 8, yloc + 8
        ys = [ypb, ypb, ypt, ypt]
        xs = [xmin - 0.2, xmin + 0.2, xmin + 0.2, xmin - 0.2]
        plt.fill(xs, ys, "w", fill=True, linewidth=0, zorder=10, clip_on=False)
        plt.plot(
            [xmin - 0.2, xmin + 0.2],
            [ypb + 3, ypb - 3],
            color="k",
            linewidth=1,
            zorder=20,
            clip_on=False,
        )
        plt.plot(
            [xmin - 0.2, xmin + 0.2],
            [ypt + 3, ypt - 3],
            color="k",
            linewidth=1,
            zorder=20,
            clip_on=False,
        )
        plt.text(
            xmin,
            yloc - 1,
            "~",
            fontsize=8,
            zorder=30,
            clip_on=False,
            ha="center",
            va="center",
        )

    draw_yaxis_break(ymax * 1.3)

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xscale("log")
    plt.xlim(left=xmin)
    plt.xlabel("Time between writes\n(ms, log-scale)")

    xticks = [TIME_INTERVAL_UNIT, 10, 100]
    xticklabels = [str(x) for x in xticks]
    plt.xticks(xticks, xticklabels)

    plt.ylim(bottom=-3)
    plt.ylabel("Δ #ver.")
    ax.yaxis.set_label_coords(-0.3, 0.4)

    max_ytick = int((ymax // 50) * 50)
    yticks = list(range(0, max_ytick + 1, 50))
    yticklabels = [str(y) for y in yticks]
    yticks += [ymax * 1.6]
    yticklabels += ["∞"]
    plt.yticks(yticks, yticklabels)

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2, 1.6),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    lgd = plt.legend(
        handles,
        labels,
        handlelength=1.0,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )
    for rec in lgd.get_texts():
        if "RSPaxos" in rec.get_text() or "CRaft" in rec.get_text():
            rec.set_fontstyle("italic")
        # if "Crossword" in rec.get_text():
        #     rec.set_fontweight("bold")

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

        utils.proc.check_enough_cpus(MIN_HOST0_CPUS, remote=remotes["host0"])
        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
        utils.file.do_cargo_build(
            True, cd_dir=f"{base}/{repo}", remotes=remotes
        )
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        if not os.path.isdir(runlog_path):
            os.system(f"mkdir -p {runlog_path}")

        try:
            for num_keys in NUM_KEYS_LIST:
                print(f"Running experiments {num_keys}...")

                for protocol, gossip_gap in PROTOCOL_GAPS:
                    time.sleep(3)
                    bench_round(
                        remotes["host0"],
                        base,
                        repo,
                        protocol,
                        gossip_gap,
                        num_keys,
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

    else:
        runlog_dir = f"{args.odir}/runlog/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        ver_stats, diff_stats = collect_ver_stats(runlog_dir)
        print_results(ver_stats, diff_stats)

        handles, labels = plot_staleness(diff_stats, plots_dir)
        plot_legend(handles, labels, plots_dir)
