import os
import argparse
import time
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "tput_lat_curve"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
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
    "EPaxos": (
        "EPaxos",
        [],
        [
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        None,
    ),
    "PQR": (
        "MultiPaxos",
        [
            "enable_quorum_reads=true",
            "urgent_commit_notice=true",
        ],
        [
            "enable_quorum_reads=true",
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        None,
    ),
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
PROTOCOL_MAX_NUM_CLIENTS = {
    "MultiPaxos": 35,
    "LeaderLs": 30,
    "EPaxos": 35,
    "PQR": 35,
    "PQRLeaderLs": 50,
    "QuorumLs": 99,
    "QuorumLsCtn": 99,
    "Bodega": 99,
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
# CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
# NUM_CLIENTS = 10
NUM_CLIENTS_LIST = [
    5,
    10,
    15,
    20,
    25,
    30,
    35,
    40,
    50,
    75,
    99,
]  # => will be x10
# FREQ_TARGETS = [
#     100,
#     250,
#     500,
#     1000,
#     3000,
#     5000,
#     7500,
#     10000,
# ]  # => will be adjusted
NUM_KEYS = 1  # => will be x10
VALUE_SIZE = 128
PUT_RATIO = 10
BATCH_INTERVAL = 1

LENGTH_SECS = 150
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 130


def launch_cluster(remote0, base, repo, pcname, num_clients, config=None):
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.distr_cluster",
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
        f".{pcname}.{num_clients}",
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


def run_bench_clients(remotec, base, repo, pcname, num_clients, config=None):
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.distr_clients",
        "-p",
        PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0],
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{NUM_REPLICAS}",  # place clients on host5~9
        "--man",
        "host0",
        # "--pin_cores",
        # str(CLIENT_PIN_CORES),
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    cmd += [
        "bench",
        "-n",
        str(num_clients),
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
        f".{pcname}.{num_clients}",
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
        "uv",
        "run",
        "-m",
        "scripts.distr_clients",
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


def outputs_exist(output_dir, pcname, num_clients):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    prefix = f"{output_dir}/{protocol}.{pcname}.{num_clients}"
    return all(os.path.isfile(f"{prefix}.{c}.out") for c in range(num_clients))


def bench_round(remote0, remotec, base, repo, pcname, num_clients, runlog_path):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    responders = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3]
    midfix_str = f".{pcname}.{num_clients}"
    print(f"  {EXPER_NAME}  {pcname:<12s}{midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in COMMON_SERVER_CONFIGS:
        server_config += f"+{cfg}"
    for cfg in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][1]:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, pcname, num_clients, config=server_config
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
        remotec, base, repo, pcname, num_clients, config=client_config
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
    for num_clients in NUM_CLIENTS_LIST:
        results[num_clients] = dict()
        for cgroup in range(NUM_REPLICAS):
            results[num_clients][cgroup] = dict()
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                if num_clients > PROTOCOL_MAX_NUM_CLIENTS.get(pcname, 999):
                    continue
                protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]

                try:
                    result = utils.output.gather_outputs(
                        f"{protocol}.{pcname}.{num_clients}",
                        num_clients,
                        output_dir,
                        RESULT_SECS_BEGIN,
                        RESULT_SECS_END,
                        0.1,
                        client_start=cgroup,
                        client_step=NUM_REPLICAS,
                    )
                except FileNotFoundError:
                    print(
                        f"skipping {pcname} @ clients {num_clients} (cg {cgroup})..."
                    )
                    continue

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

                results[num_clients][cgroup][pcname] = {
                    "tput": tput_list,
                    "wlat": wlat_list,
                    "rlat": rlat_list,
                }

    ymax = {"tput": 0.0, "wlat": 0.0, "rlat": 0.0}
    for num_clients in NUM_CLIENTS_LIST:
        for cgroup in range(NUM_REPLICAS):
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                if pcname not in results[num_clients][cgroup]:
                    continue

                curr_results = results[num_clients][cgroup][pcname]
                tput_list = curr_results["tput"]
                wlat_list = sorted(lat / 1000 for lat in curr_results["wlat"])
                rlat_list = sorted(lat / 1000 for lat in curr_results["rlat"])

                results[num_clients][cgroup][pcname] = {
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
                curr_results = results[num_clients][cgroup][pcname]
                if curr_results["tput"]["mean"] > ymax["tput"]:
                    ymax["tput"] = curr_results["tput"]["mean"]
                if (
                    curr_results["wlat"]["mean"] is not None
                    and curr_results["wlat"]["sorted"][-1] > ymax["wlat"]
                ):
                    ymax["wlat"] = curr_results["wlat"]["sorted"][-1]
                if (
                    curr_results["rlat"]["mean"] is not None
                    and curr_results["rlat"]["sorted"][-1] > ymax["rlat"]
                ):
                    ymax["rlat"] = curr_results["rlat"]["sorted"][-1]

    return results, ymax


def aggregate_results(results):
    agg_results = {
        pcname: {"clients": [], "tput": [], "alat": []}
        for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS
    }

    for num_clients in NUM_CLIENTS_LIST:
        print(f"clients {num_clients}")
        for cgroup, pc_results in results.get(num_clients, {}).items():
            print(f"  cg {cgroup}")
            if len(pc_results) == 0:
                print("    (no data)")
                continue
            for pcname, result in pc_results.items():
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

    print("aggregated results:")
    for pcname in agg_results:
        print(f"  {pcname}")
        for num_clients in NUM_CLIENTS_LIST:
            agg_tput, rlats, wlats = 0.0, [], []
            for cgroup, pc_results in results.get(num_clients, {}).items():
                if pcname not in pc_results:
                    continue
                agg_tput += pc_results[pcname]["tput"]["mean"]
                if (
                    pc_results[pcname]["rlat"]["mean"] is not None
                    and pc_results[pcname]["wlat"]["mean"] is not None
                ):
                    rlats.append(pc_results[pcname]["rlat"]["mean"])
                    wlats.append(pc_results[pcname]["wlat"]["mean"])

            if len(rlats) == 0:
                continue
            rlats.sort()
            wlats.sort()

            w_ratio = PUT_RATIO / 100.0
            r_ratio = 1.0 - w_ratio
            alats = [r * r_ratio + w * w_ratio for r, w in zip(rlats, wlats)]

            agg_results[pcname]["clients"].append(num_clients)
            agg_results[pcname]["tput"].append(agg_tput)
            agg_results[pcname]["alat"].append(sum(alats) / len(alats))

        if len(agg_results[pcname]["tput"]) == 0:
            print("    (no data)")
            continue

        print("    tput", end="")
        for num_clients, tput in zip(
            agg_results[pcname]["clients"], agg_results[pcname]["tput"]
        ):
            print(f"  [{num_clients}] {tput:7.2f}", end="")
        print()
        print("    alat", end="")
        for num_clients, alat in zip(
            agg_results[pcname]["clients"], agg_results[pcname]["alat"]
        ):
            print(f"  [{num_clients}] {alat:7.2f}", end="")
        print()

    return agg_results


def plot_curves_results(agg_results, plots_dir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (
                4.6,
                1.9,
            ),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper-curves")

    PCNAMES_ORDER = [
        "MultiPaxos",
        "LeaderLs",
        "EPaxos",
        "PQR",
        "PQRLeaderLs",
        "QuorumLs",
        "QuorumLsCtn",
        "Bodega",
    ]
    PCNAMES_LABEL_COLOR_STYLE_MARKER_SIZE = {
        "MultiPaxos": ("MultiPaxos", "gray", ":", "o", 3.5),
        "LeaderLs": ("Leader Leases", "0.4", "-", "v", 3.5),
        "EPaxos": ("EPaxos", "lightseagreen", "-", "s", 3.0),
        "PQR": ("PQR", "palevioletred", ":", "d", 3.8),
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "firebrick", "-", "^", 3.5),
        "QuorumLs": ("Quorum Leases", "mediumseagreen", ":", "X", 3.8),
        "QuorumLsCtn": ("Qrm Ls (passive)", "forestgreen", "-", "p", 3.8),
        "Bodega": ("Bodega", "royalblue", "-", "*", 4.4),
    }

    # plot one series per protocol
    for pcname in PCNAMES_ORDER:
        if pcname not in agg_results:
            continue
        label, color, linestyle, marker, markersize = (
            PCNAMES_LABEL_COLOR_STYLE_MARKER_SIZE[pcname]
        )

        tputs = agg_results[pcname]["tput"]
        lats = agg_results[pcname]["alat"]
        if len(tputs) == 0 or len(lats) == 0:
            continue

        # convert throughput to k reqs/s
        tputs_k = [t / 1000.0 for t in tputs]

        plt.plot(
            tputs_k,
            lats,
            color=color,
            linestyle=linestyle,
            linewidth=1.2,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )

    ax1 = fig.axes[0]
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.xlabel("Throughput (k reqs/s)")
    plt.ylabel("Avg. Latency (ms)")

    # axis ranges with a small padding
    xmax = max(
        (
            max(agg_results[pc]["tput"]) / 1000.0
            for pc in agg_results
            if len(agg_results[pc]["tput"]) > 0
        ),
        default=0.0,
    )
    ymax_local = max(
        (
            max(agg_results[pc]["alat"])
            for pc in agg_results
            if len(agg_results[pc]["alat"]) > 0
        ),
        default=0.0,
    )
    if ymax is not None:
        xmax = max(xmax, ymax.get("tput", 0.0) / 1000.0)
        ymax_local = max(ymax_local, ymax.get("alat", 0.0))

    if xmax > 0:
        plt.xlim(left=0.0, right=xmax * 1.05)
    if ymax_local > 0:
        plt.ylim(bottom=0.0, top=ymax_local * 1.05)

    plt.grid(which="major", axis="both", color="lightgray", zorder=5)

    handles, labels = ax1.get_legend_handles_labels()
    plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.2,
        markerscale=1.1,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        borderpad=0.3,
        handletextpad=0.4,
        columnspacing=1.2,
        labelspacing=0.6,
        frameon=False,
        fontsize=8,
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return handles, labels


# def plot_legend(handles, labels, plots_dir):
#     matplotlib.rcParams.update(
#         {
#             "figure.figsize": (3.6, 2.0),
#             "font.size": 10,
#             "pdf.fonttype": 42,
#         }
#     )
#     plt.figure("Legend")

#     plt.axis("off")

#     # swap location of Bodega?
#     # hb = handles.pop()
#     # handles.insert(3, hb)
#     # lb = labels.pop()
#     # labels.insert(3, lb)

#     _lgd = plt.legend(
#         handles,
#         labels,
#         handleheight=0.8,
#         handlelength=1.0,
#         loc="center",
#         bbox_to_anchor=(0.5, 0.5),
#         ncol=2,
#         borderpad=0.3,
#         handletextpad=0.3,
#         columnspacing=0.9,
#         frameon=False,
#     )

#     pdf_name = f"{plots_dir}/legend-{EXPER_NAME}.pdf"
#     plt.savefig(pdf_name, bbox_inches=0)
#     plt.close()
#     print(f"Plotted: {pdf_name}")


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
        "-s",
        "--skip",
        action="store_true",
        help="if set, skip rounds with existing outputs",
    )
    parser.add_argument(
        "-c",
        "--continue",
        dest="cont",
        action="store_true",
        help="if set together with --skip, keep going after a failed round (sleep 3s then retry loop)",
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

        while True:
            try:
                for num_clients in NUM_CLIENTS_LIST:
                    print(f"Running experiments clients = {num_clients}...")
                    for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                        if num_clients > PROTOCOL_MAX_NUM_CLIENTS.get(
                            pcname, 999
                        ):
                            print(
                                f"  skipping {pcname} @ clients {num_clients} (> {PROTOCOL_MAX_NUM_CLIENTS[pcname]})"
                            )
                            continue
                        if args.skip and outputs_exist(
                            output_path, pcname, num_clients
                        ):
                            print(
                                f"  skipping {pcname} @ clients {num_clients} (outputs exist)"
                            )
                            continue
                        time.sleep(3)

                        bench_round(
                            remotes["host0"],
                            remotes[f"host{NUM_REPLICAS}"],
                            base,
                            repo,
                            pcname,
                            num_clients,
                            runlog_path,
                        )
                        utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                        utils.file.remove_files_in_dir(  # to free up storage space
                            f"{base}/states/{EXPER_NAME}",
                            remotes=remotes,
                        )
                        utils.file.clear_fs_caches(remotes=remotes)

                break  # finished all loops without failures

            except utils.BreakingLoops:
                utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP)
                if args.skip and args.cont:
                    print(
                        "Experiment FAILED, sleeping 3s and retrying loop (--continue enabled)..."
                    )
                    time.sleep(3)
                    continue

                print("Experiment FAILED, breaking early...")
                break

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

        results, _ = collect_outputs(output_dir)
        agg_results = aggregate_results(results)

        handles, labels = plot_curves_results(agg_results, plots_dir)
        # plot_legend(handles, labels, plots_dir)


if __name__ == "__main__":
    main()
