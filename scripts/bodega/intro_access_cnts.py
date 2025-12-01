import os
import argparse
import time
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "access_cnts"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
    "record_breakdown=true",
    "record_node_cnts=true",
]
PROTOCOLS_BSNAME_CONFIGS_RESPONDERS = {
    "MultiPaxos": (
        "MultiPaxos",
        [],
        [],
        None,
    ),
    "LeaderLs": (
        "MultiPaxos",
        [
            "lease_expire_ms=2500",
            "enable_leader_leases=true",
        ],
        [],
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
        {0, 2, 3},
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
        {0, 2, 3},
    ),
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 5
NUM_KEYS = 1
VALUE_SIZE = 128
PUT_RATIO = 10
BATCH_INTERVAL = 1

CLIENT_NODE = 8  # near server 3

LENGTH_SECS = 120
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 100


def launch_cluster(remote0, base, repo, pcname, config=None):
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
        f".{pcname}",
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


def run_bench_clients(remotec, base, repo, pcname, config=None):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0],
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{CLIENT_NODE}",
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
        "1",
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
        f".{pcname}",
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
        f"host{CLIENT_NODE}",
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
    remote0, remotec, base, repo, pcname, runlog_path, responders=None
):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    midfix_str = f".{pcname}"
    print(f"  {EXPER_NAME}  {pcname:<12s}{midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in COMMON_SERVER_CONFIGS:
        server_config += f"+{cfg}"
    for cfg in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][1]:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, pcname, config=server_config
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
        remotec, base, repo, pcname, config=client_config
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


def collect_access_cnts(runlog_dir, flat_style):
    access_cnts = dict()
    for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
        protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
        node_cnts = {n: 0 for n in range(NUM_REPLICAS)}

        with open(f"{runlog_dir}/{protocol}.{pcname}.s.err", "r") as flog:
            for line in flog:
                if "node cnts" in line:
                    line = line.strip()
                    line = line[line.find("node cnts stats") + 15 :]
                    segs = line.strip().split()

                    for seg in segs:
                        seg = seg.split(":")
                        node = int(seg[0])
                        cnt = int(seg[1])
                        node_cnts[node] += cnt

        node_cnts = [node_cnts[i] for i in range(NUM_REPLICAS)]
        total_cnt = sum(node_cnts) if flat_style else max(node_cnts)
        percents = [(cnt / total_cnt) * 100 for cnt in node_cnts]
        access_cnts[pcname] = {
            "cnts": node_cnts,
            "percents": percents,
        }

    return access_cnts


def print_access_cnts(access_cnts):
    for pcname, stat in access_cnts.items():
        print(f"{pcname}")
        for node in range(NUM_REPLICAS):
            print(
                f"  {node} {stat['cnts'][node]:>6d} {int(stat['percents'][node]):>3d}%",
                end="",
            )
        print()


def plot_access_cnts_flat(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.5, 1.35),
            "font.size": 8.5,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-access_cnts")

    PCNAMES_ORDER = [
        "MultiPaxos",
        "LeaderLs",
        "QuorumLs",
        "Bodega",
    ]
    PCNAMES_LABEL = {
        "MultiPaxos": "MultiPaxos",
        "LeaderLs": "Leader Leases",
        "QuorumLs": "Quorum Leases",
        "Bodega": "Bodega",
    }

    NODES_COLOR = ["peachpuff", "salmon", "powderblue", "honeydew", "thistle"]
    NODES_NAME = ["S0", "S1", "S3", "S4", "S2"]

    BAR_HEIGHT = 0.8

    for pci, pcname in enumerate(PCNAMES_ORDER):
        y = len(PCNAMES_ORDER) - pci - 1
        percents = results[pcname]["percents"]
        accumulated = 0

        for nodei in range(NUM_REPLICAS):
            percent, color, nodename = (
                percents[nodei],
                NODES_COLOR[nodei],
                NODES_NAME[nodei],
            )

            plt.barh(
                [y],
                [percent],
                left=accumulated,
                height=BAR_HEIGHT,
                color=color,
                edgecolor="gray",
                linewidth=0.5,
            )

            # name of node
            if percent > 5:
                plt.text(
                    accumulated + percent / 2,
                    y,
                    nodename,
                    ha="center",
                    va="center_baseline",
                    color="0.35",
                    fontsize=7.5,
                )

            accumulated += percent

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xticks([0, 50, 100], ["0", "50", "100"], fontsize=7.5)
    plt.xlabel("%nodes touched by the client's reads")
    ax.xaxis.set_label_coords(0.10, -0.4)

    plt.yticks(
        list(reversed(range(len(PCNAMES_ORDER)))),
        [PCNAMES_LABEL[pcname] for pcname in PCNAMES_ORDER],
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/intro-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_access_cnts(results, plots_dir):
    """For each exper, the highest count across servers should be the 100%."""
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.1, 1.35),
            "font.size": 8.5,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-access_cnts")

    PCNAMES_ORDER = [
        "MultiPaxos",
        "LeaderLs",
        "QuorumLs",
        "Bodega",
    ]
    PCNAMES_LABEL = {
        "MultiPaxos": "MultiPaxos",
        "LeaderLs": "LeaderLs",
        "QuorumLs": "QuorumLs",
        "Bodega": "Bodega",
    }

    NODES_COLOR = ["peachpuff", "salmon", "powderblue", "honeydew", "thistle"]
    NODES_NAME = ["S0", "S1", "S3", "S4", "S2"]
    # NUM_PLOTTED_REPLICAS = NUM_REPLICAS - 2
    NUM_PLOTTED_REPLICAS = NUM_REPLICAS

    x = np.arange(len(PCNAMES_ORDER))
    width = 0.16

    pi = 0
    for n in range(NUM_REPLICAS):
        i = NODES_NAME.index(f"S{n}")
        # skip S1 and S2 bars (hardcoded) as they are always zero
        # if NODES_NAME[i] in ["S1", "S2"]:
        #     continue

        percents = [results[pcname]["percents"][i] for pcname in PCNAMES_ORDER]
        plt.bar(
            x + (pi - NUM_PLOTTED_REPLICAS / 2 + 0.5) * width,
            percents,
            width,
            color=NODES_COLOR[i],
            edgecolor="gray",
            linewidth=0.8,
        )

        for j, percent in enumerate(percents):
            node_name = NODES_NAME[i]
            # if j == 0:
            #     if node_name == "S0":
            #         node_name += " (leader)"
            #     elif node_name == "S4":
            #         node_name += " (nearest)"

            plt.text(
                x[j] + (pi - NUM_PLOTTED_REPLICAS / 2 + 0.5) * width + 0.01,
                12,
                node_name,
                ha="center",
                va="bottom",
                color="gray",
                fontsize=6.5,
                rotation=90,
            )
            # if percent > 25:

        pi += 1

    for i in range(len(PCNAMES_ORDER) - 1):
        plt.axvline(
            x=i + 0.5,
            color="black",
            linestyle=":",
            # alpha=0.8,
            linewidth=0.8,
        )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.ylim(0, 101)
    plt.ylabel("P(node touched)", fontsize=8)
    ax.yaxis.set_label_coords(-0.15, 0.15)
    plt.yticks(
        [0, 50, 100],
        ["0%", "50%", "100%"],
        fontsize=7,
        # rotation=90,
    )
    plt.tick_params(axis="y", which="both", pad=1)

    plt.xlim(
        width * (-NUM_PLOTTED_REPLICAS / 2) - width / 2,
        len(PCNAMES_ORDER) - width * (NUM_PLOTTED_REPLICAS / 2) - width / 2,
    )
    plt.xticks(
        # x - 0.3 * width,
        x,
        [PCNAMES_LABEL[pcname] for pcname in PCNAMES_ORDER],
        # rotation=22,
        fontsize=7.5,
    )
    plt.tick_params(axis="x", which="both", bottom=False, pad=0)

    # hardcoded for now, improve later
    plt.text(
        0,
        -14,
        "\nnever\nlocal",
        clip_on=False,
        ha="center",
        va="top",
        fontsize=7.5,
    )
    plt.text(
        1,
        -14,
        "\nonly at\nleader",
        clip_on=False,
        ha="center",
        va="top",
        fontsize=7.5,
    )
    plt.text(
        2,
        -14,
        "\nwhen\nquiescent",
        clip_on=False,
        ha="center",
        va="top",
        fontsize=7.5,
    )
    plt.text(
        3,
        -14,
        "\nalways\nlocal",
        clip_on=False,
        ha="center",
        va="top",
        fontsize=7.5,
    )
    plt.text(3.4, -15, "\n✓", clip_on=False, ha="center", va="top", fontsize=12)

    plt.tight_layout()

    pdf_name = f"{plots_dir}/intro-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches="tight")
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
    parser.add_argument(
        "-l",
        "--flat",
        action="store_true",
        help="if set, plot alternative flat-style plot",
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
            print("Running experiments...")
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                time.sleep(3)

                bench_round(
                    remotes["host0"],
                    remotes[f"host{CLIENT_NODE}"],
                    base,
                    repo,
                    pcname,
                    runlog_path,
                    responders=PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3],
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
        utils.file.fetch_files_of_dir(
            remotes[f"host{CLIENT_NODE}"],
            f"{base}/output/{EXPER_NAME}",
            output_path,
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
        runlog_dir = f"{args.odir}/runlog/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        access_cnts = collect_access_cnts(runlog_dir, args.flat)
        print_access_cnts(access_cnts)

        if args.flat:
            plot_access_cnts_flat(access_cnts, plots_dir)
        else:
            plot_access_cnts(access_cnts, plots_dir)


if __name__ == "__main__":
    main()
