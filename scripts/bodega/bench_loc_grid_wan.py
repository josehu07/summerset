import os
import argparse
import time
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "loc_grid_wan"

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

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 5  # => will be x10
NUM_KEYS = 1  # => will be x10
VALUE_SIZE = 128
PUT_RATIOS = [0, 1, 10]
BATCH_INTERVAL = 1

LENGTH_SECS = 150
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 130


def launch_cluster(remote0, base, repo, pcname, put_ratio, config=None):
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


def wait_cluster_setup(sleep_secs=60):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(remotec, base, repo, pcname, put_ratio, config=None):
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
        str(put_ratio),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{pcname}.{put_ratio}",
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


def bench_round(remote0, remotec, base, repo, pcname, put_ratio, runlog_path):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    responders = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3]
    midfix_str = f".{pcname}.{put_ratio}"
    print(f"  {EXPER_NAME}  {pcname:<12s}{midfix_str}")

    # server-side configs
    server_config = f"batch_interval_ms={BATCH_INTERVAL}"
    for cfg in COMMON_SERVER_CONFIGS:
        server_config += f"+{cfg}"
    for cfg in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][1]:
        server_config += f"+{cfg}"

    # launch service cluster
    proc_cluster = launch_cluster(
        remote0, base, repo, pcname, put_ratio, config=server_config
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
        remotec, base, repo, pcname, put_ratio, config=client_config
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
    for put_ratio in PUT_RATIOS:
        results[put_ratio] = dict()
        for cgroup in range(NUM_REPLICAS):
            results[put_ratio][cgroup] = dict()
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]

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

                results[put_ratio][cgroup][pcname] = {
                    "tput": tput_list,
                    "wlat": wlat_list,
                    "rlat": rlat_list,
                }

    ymax = {"tput": 0.0, "wlat": 0.0, "rlat": 0.0}
    for put_ratio in PUT_RATIOS:
        for cgroup in range(NUM_REPLICAS):
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                curr_results = results[put_ratio][cgroup][pcname]
                tput_list = curr_results["tput"]
                wlat_list = sorted(lat / 1000 for lat in curr_results["wlat"])
                rlat_list = sorted(lat / 1000 for lat in curr_results["rlat"])

                results[put_ratio][cgroup][pcname] = {
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
                curr_results = results[put_ratio][cgroup][pcname]
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


def print_results(results):
    for put_ratio in PUT_RATIOS:
        print(f"puts {put_ratio}")
        for cgroup in results[put_ratio]:
            print(f"  cg {cgroup}")
            for pcname, result in results[put_ratio][cgroup].items():
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


def plot_put_ratio_results(results, put_ratio, plots_dir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (
                3.7 if put_ratio > 0 else 2.85,
                2.5,
            ),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    _fig = plt.figure(f"Exper-{put_ratio}")

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
    PCNAMES_LABEL_COLORS_HATCH = {
        "MultiPaxos": ("MultiPaxos", "lightgray", "gray", None),
        "LeaderLs": ("Leader Leases", "0.55", "0.4", None),
        "EPaxos": ("EPaxos", "paleturquoise", "mediumturquoise", "..."),
        "PQR": ("PQR", "pink", "hotpink", "///"),
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "lightcoral", "indianred", "///"),
        "QuorumLs": ("Quorum Leases", "palegreen", "limegreen", "\\\\\\"),
        "QuorumLsCtn": (
            "Qrm Ls (passive)",
            "darkseagreen",
            "forestgreen",
            "\\\\\\",
        ),
        "Bodega": ("Bodega", "lightsteelblue", "steelblue", "xxx"),
    }

    # CLIENT_GROUPS_ORDER = [0, 3, 1, 4, 2]
    if put_ratio > 0:
        CLIENT_GROUPS_ORDER = [0, 3, 1, 2]
        CLIENT_GROUPS_LABEL_COLOR = {
            0: ("WI", "darkorange"),
            3: ("MA", "red"),
            1: ("UT/APT", "red"),
            # 4: ("APT", "red"),
            2: ("SC", "0.3"),
        }
    else:
        CLIENT_GROUPS_ORDER = [0, 1, 2]
        CLIENT_GROUPS_LABEL_COLOR = {
            0: ("WI", "darkorange"),
            # 3: ("MA", "red"),
            1: ("MA/UT/APT", "red"),
            # 4: ("APT", "red"),
            2: ("SC", "0.3"),
        }

    BAR_WIDTH = 0.92

    # throughput
    ax1 = plt.subplot(311)

    xpos, xticks, xticklabels, ymaxl = 1, [], [], 0.0
    for cgroup in CLIENT_GROUPS_ORDER:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(CLIENT_GROUPS_LABEL_COLOR[cgroup][0])

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["tput"]
            norm_tput = (
                result["mean"]
                / results[put_ratio][cgroup]["LeaderLs"]["tput"]["mean"]
            )
            if norm_tput > ymaxl:
                ymaxl = norm_tput

            if put_ratio == 0:  # manual y-axis break
                if norm_tput > 27.5:
                    norm_tput -= 27.5

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                norm_tput,
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.25",
                linewidth=0,
                label=label if cgroup == 1 else None,
                hatch=hatch,
                # yerr=result["stdev"],
                # ecolor="black",
                # capsize=1,
                zorder=-10,
            )

            xpos += 1
        xpos += 1.5

    # 1.0 line
    plt.axhline(
        1.0,
        xmin=0.0,
        xmax=xpos - 1.5,
        color="tomato",
        linestyle="--",
        linewidth=0.8,
        zorder=-20,
    )

    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)
    plt.xlim((0.0, xpos - 1.5))

    plt.xticks(xticks, xticklabels)
    for cgi, cgroup in enumerate(CLIENT_GROUPS_ORDER):
        ax1.get_xticklabels()[cgi].set_alpha(0.0)  # don't show

    # hardcoded y-axis limits
    if put_ratio == 0:
        for break_x in (0.0, 15.5, 16.5, 17.5):
            plt.plot(
                [break_x - 0.5, break_x + 0.5],
                [2.0, 2.0],
                color="black",
                linewidth=0.8,
                clip_on=False,
            )
            plt.plot(
                [break_x - 0.5, break_x + 0.5],
                [2.4, 2.4],
                color="black",
                linewidth=0.8,
                clip_on=False,
            )
            plt.fill(
                [
                    break_x - 0.55,
                    break_x + 0.55,
                    break_x + 0.55,
                    break_x - 0.55,
                ],
                [2.02, 2.02, 2.38, 2.38],
                "w",
                fill=True,
                linewidth=0,
                zorder=10,
                clip_on=False,
            )
            plt.text(
                break_x,
                2.17,
                "~",
                fontsize=8,
                zorder=30,
                clip_on=False,
                ha="center",
                va="center",
                color="gray",
            )

        plt.ylim(0.0, 31.2 - 27.5)
        plt.yticks([1.0, 31.0 - 27.5], ["1x", "31x"])
    elif put_ratio == 1:
        plt.ylim(0.0, 17.0)
        plt.yticks([1.0, 8.0, 16.0], ["1x", "8x", "16x"])
    elif put_ratio == 10:
        plt.ylim(0.0, 4.2)
        plt.yticks([1.0, 2.0, 4.0], ["1x", "2x", "4x"])

    # get latency
    ax2 = plt.subplot(312)

    xpos, xticks, xticklabels = 1, [], []
    for cgroup in CLIENT_GROUPS_ORDER:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(CLIENT_GROUPS_LABEL_COLOR[cgroup][0])

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["rlat"]
            if result["mean"] is None:
                xpos += 1
                continue

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                result["mean"],
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.35",
                linewidth=0,
                # label=label,
                hatch=hatch,
                yerr=max(result["p99"] - result["mean"], 0.0),
                ecolor=color,
                error_kw={"elinewidth": 2, "zorder": -15},
                zorder=-10,
            )
            plt.plot(
                [xpos - 0.25, xpos + 0.25],
                [result["p99"], result["p99"]],
                color=ecolor,
                linestyle="-",
                linewidth=1.5,
                zorder=10,
            )

            xpos += 1
        xpos += 1.5

    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)
    plt.xlim((0.0, xpos - 1.5))

    if put_ratio == 0:
        plt.xticks(xticks, xticklabels)
        for cgi, cgroup in enumerate(CLIENT_GROUPS_ORDER):
            ax2.get_xticklabels()[cgi].set_color(
                CLIENT_GROUPS_LABEL_COLOR[cgroup][1]
            )
            # ax2.get_xticklabels()[cgi].set_weight("bold")
    else:
        plt.xticks(xticks, xticklabels)
        for cgi, cgroup in enumerate(CLIENT_GROUPS_ORDER):
            ax2.get_xticklabels()[cgi].set_alpha(0.0)  # don't show

    plt.ylim(0.0, 100.0)
    plt.yticks([0, 40, 80], list(map(str, [0, 40, 80])))

    if put_ratio == 0:
        # group separators
        for x in [x + (len(PCNAMES_ORDER) / 2) + 0.75 for x in xticks[:-1]]:
            plt.axvline(
                x,
                ymin=-0.4,
                ymax=2.22,
                clip_on=False,
                color="black",
                linewidth=0.6,
                linestyle=":",
            )
        for x in [0, xticks[-1] + (len(PCNAMES_ORDER) / 2) + 0.5]:
            plt.axvline(
                x,
                ymin=-0.4,
                ymax=0,
                clip_on=False,
                color="black",
                linewidth=0.6,
                linestyle=":",
            )

    # put latency
    ax3 = plt.subplot(313)

    xpos, xticks, xticklabels = 1, [], []
    for cgroup in CLIENT_GROUPS_ORDER:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(CLIENT_GROUPS_LABEL_COLOR[cgroup][0])

        for pcname in PCNAMES_ORDER:
            result = results[put_ratio][cgroup][pcname]["wlat"]
            if result["mean"] is None:
                xpos += 1
                continue

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                result["mean"],
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.35",
                linewidth=0,
                # label=label,
                hatch=hatch,
                yerr=max(result["p99"] - result["mean"], 0.0),
                ecolor=color,
                error_kw={"elinewidth": 2, "zorder": -15},
                zorder=-10,
            )
            plt.plot(
                [xpos - 0.25, xpos + 0.25],
                [result["p99"], result["p99"]],
                color=ecolor,
                linestyle="-",
                linewidth=1.5,
                zorder=10,
            )

            xpos += 1
        xpos += 1.5

    if put_ratio == 0:
        for spine in ax3.spines.values():
            spine.set_visible(False)

        plt.xticks(xticks, xticklabels)
        for cgi, cgroup in enumerate(CLIENT_GROUPS_ORDER):
            ax3.get_xticklabels()[cgi].set_alpha(0.0)  # don't show
        plt.tick_params(bottom=False, left=False, labelleft=False)

        ax3.set_zorder(-50)

    else:
        ax3.spines["top"].set_visible(False)
        ax3.spines["right"].set_visible(False)

        plt.tick_params(bottom=False)
        plt.xlim((0.0, xpos - 1.5))

        plt.xticks(xticks, xticklabels)
        for cgi, cgroup in enumerate(CLIENT_GROUPS_ORDER):
            ax3.get_xticklabels()[cgi].set_color(
                CLIENT_GROUPS_LABEL_COLOR[cgroup][1]
            )
            # ax3.get_xticklabels()[cgi].set_weight("bold")

        plt.ylim(0.0, 100.0)
        plt.yticks([0, 40, 80], list(map(str, [0, 40, 80])))

    if put_ratio > 0:
        # group separators
        for x in [x + (len(PCNAMES_ORDER) / 2) + 0.75 for x in xticks[:-1]]:
            plt.axvline(
                x,
                ymin=-0.4,
                ymax=3.45,
                clip_on=False,
                color="black",
                linewidth=0.6,
                linestyle=":",
            )
        for x in [0, xticks[-1] + (len(PCNAMES_ORDER) / 2) + 0.5]:
            plt.axvline(
                x,
                ymin=-0.4,
                ymax=0,
                clip_on=False,
                color="black",
                linewidth=0.6,
                linestyle=":",
            )

    plt.tight_layout(h_pad=-0.8)

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-w{put_ratio}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax1.get_legend_handles_labels()


def plot_ylabels(plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.0, 2.5),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Ylabels")

    ylabels = [
        "Norm Tput\n(to Ldr Ls)",
        "Read Lat\n(ms)",
        "Write Lat\n(ms)",
    ]

    ax1 = plt.subplot(311)
    plt.ylabel(ylabels[0])
    for spine in ax1.spines.values():
        spine.set_visible(False)
    plt.xticks([0], ["Dumb"])
    ax1.get_xticklabels()[0].set_alpha(0.0)  # don't show
    plt.tick_params(bottom=False, left=False, labelleft=False)

    ax2 = plt.subplot(312)
    plt.ylabel(ylabels[1])
    for spine in ax2.spines.values():
        spine.set_visible(False)
    plt.xticks([0], ["Dumb"])
    ax2.get_xticklabels()[0].set_alpha(0.0)  # don't show
    plt.tick_params(bottom=False, left=False, labelleft=False)

    ax3 = plt.subplot(313)
    plt.ylabel(ylabels[2])
    for spine in ax3.spines.values():
        spine.set_visible(False)
    plt.xticks([0], ["Dumb"])
    ax3.get_xticklabels()[0].set_alpha(0.0)  # don't show
    plt.tick_params(bottom=False, left=False, labelleft=False)

    fig.align_labels()

    fig.tight_layout(h_pad=-0.8)

    pdf_name = f"{plots_dir}/ylabels-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


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

    # swap location of Bodega?
    # hb = handles.pop()
    # handles.insert(3, hb)
    # lb = labels.pop()
    # labels.insert(3, lb)

    _lgd = plt.legend(
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
            for put_ratio in PUT_RATIOS:
                print(f"Running experiments w% = {put_ratio}...")
                for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                    time.sleep(3)

                    bench_round(
                        remotes["host0"],
                        remotes[f"host{NUM_REPLICAS}"],
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

        results, _ = collect_outputs(output_dir)
        print_results(results)

        for put_ratio in PUT_RATIOS:
            handles, labels = plot_put_ratio_results(
                results, put_ratio, plots_dir
            )
        plot_ylabels(plots_dir)
        plot_legend(handles, labels, plots_dir)


if __name__ == "__main__":
    main()
