import os
import argparse
import time
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "wlats_on_conf"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
]
PROTOCOLS_BSNAME_CONFIGS_RESPONDERS = {
    # "QuorumLs": (
    #     "QuorumLeases",
    #     [
    #         "lease_expire_ms=2500",
    #         "enable_leader_leases=true",
    #         "urgent_commit_notice=true",
    #         "no_lease_retraction=false",
    #     ],
    #     [
    #         "near_server_id=x",  # placeholder; set by distr_clients.py
    #     ],
    #     {0, 1, 2, 3, 4},
    # ),
    # "QuorumLsCtn": (
    #     "QuorumLeases",
    #     [
    #         "lease_expire_ms=2500",
    #         "enable_leader_leases=true",
    #         "urgent_commit_notice=true",
    #         "no_lease_retraction=true",
    #     ],
    #     [
    #         "near_server_id=x",  # placeholder; set by distr_clients.py
    #     ],
    #     {0, 1, 2, 3, 4},
    # ),
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
        {0, 1, 2, 3, 4},
    ),
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 1  # near leader
NUM_KEYS = 1  # focused
KEY_LEN = 8
VALUE_SIZE = 128
WRITE_FREQ = 400
BATCH_INTERVAL = 1

LENGTH_SECS = 120
CHECK1_SECS_BEGIN = 20
FAILURE_AT_SECS = 32
FAILURE_REPLICA = 4
# FAILURE_TO_CONF = {0, 1, 2, 3}
CHECK2_SECS_BEGIN = 70
CHANGE_AT_SECS = 82
CHANGE_TO_CONF = {0, 2, 3}


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
        str(WRITE_FREQ),  # open-loop
        "-k",
        str(NUM_KEYS),
        "-v",
        str(VALUE_SIZE),
        "-w",
        str(40),  # do both writes and reads
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"/tmp/{EXPER_NAME}",  # use tmpfs
        "--output_midfix",
        f".{pcname}",
        "--fine_output",  # finer-grained output
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
    remotec,
    base,
    repo,
    protocol,
    pause=None,
    resume=None,
    leader=None,
    key_range=None,
    responder=None,
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
        f"host{NUM_REPLICAS}",
        "--man",
        "host0",
        "--skip_build",
        "mess",
    ]
    if pause is not None and len(pause) > 0:
        cmd += ["--pause", pause]
    if resume is not None and len(resume) > 0:
        cmd += ["--resume", resume]
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


def bench_round(remote0, remotec, base, repo, pcname, runlog_path):
    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
    responders = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3]
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
        with open(
            f"{runlog_path}/{protocol}{midfix_str}.m.k.out", "wb"
        ) as fmout:
            fmout.write(mout)
        with open(
            f"{runlog_path}/{protocol}{midfix_str}.m.k.err", "wb"
        ) as fmerr:
            fmerr.write(merr)
        time.sleep(5)

    # client-side configs
    client_config = "+".join(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][2])

    # start benchmarking clients
    proc_clients = run_bench_clients(
        remotec, base, repo, pcname, config=client_config
    )

    # wait till time for a replica failure
    time.sleep(FAILURE_AT_SECS)
    print(
        f"    Forcing replica {FAILURE_REPLICA} to fail (@ {FAILURE_AT_SECS}s)..."
    )
    proc_mess = run_mess_client(
        remotec,
        base,
        repo,
        protocol,
        pause=str(FAILURE_REPLICA),
        # leader=str(FORCE_LEADER),
        # key_range="full",
        # responder=",".join(list(map(str, FAILURE_TO_CONF))),
    )
    mout, merr = proc_mess.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.f.out", "wb") as fmout:
        fmout.write(mout)
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.f.err", "wb") as fmerr:
        fmerr.write(merr)

    # wait till time for a config change
    time.sleep(CHANGE_AT_SECS - FAILURE_AT_SECS)
    print(f"    Changing config -> {CHANGE_TO_CONF} (@ {CHANGE_AT_SECS}s)...")
    proc_mess = run_mess_client(
        remotec,
        base,
        repo,
        protocol,
        leader=str(FORCE_LEADER),
        key_range="full",
        responder=",".join(list(map(str, CHANGE_TO_CONF))),
    )
    mout, merr = proc_mess.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.u.out", "wb") as fmout:
        fmout.write(mout)
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.u.err", "wb") as fmerr:
        fmerr.write(merr)

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
    results, wzidx, rzidx, wcidx, rcidx = dict(), 0, 0, 0, 0
    for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
        protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
        results[pcname] = []

        # doing custom aligned output parsing here
        buffer, result, zeros_cnt, dip_found, base_time = [], [], 0, False, None
        with open(f"{output_dir}/{protocol}.{pcname}.0.out", "r") as fout:
            started = False
            for line in fout:
                line = line.strip()
                if not started and line.startswith("Elapsed"):
                    started = True
                elif started and len(line) > 0:
                    segs = line.split()
                    time = float(segs[0])
                    wlat = float(segs[6])
                    rlat = float(segs[8])

                    if time < CHECK1_SECS_BEGIN:
                        continue
                    elif not dip_found:
                        buffer.append((time, wlat, rlat))
                        if len(buffer) > 155:
                            buffer.pop(0)
                        if wlat < 10.0:
                            zeros_cnt += 1
                        else:
                            zeros_cnt = 0

                        if zeros_cnt == 15:
                            dip_found = True
                            zeros_cnt = 0
                            base_time = buffer[0][0]
                            result = [
                                (time - base_time, wlat, rlat)
                                for time, wlat, rlat in buffer
                            ]
                            if len(result) - zeros_cnt > wzidx:
                                wzidx = len(result) - zeros_cnt
                    else:
                        result.append((time - base_time, wlat, rlat))
                        if len(result) >= 1300:
                            break
                        if rlat < 10.0:
                            zeros_cnt += 1
                        else:
                            zeros_cnt = 0

                        if zeros_cnt == 15:
                            if len(result) - zeros_cnt > rzidx:
                                rzidx = len(result) - zeros_cnt

        # clean up spurious latency outliers due to injected failure
        for i in range(len(result)):
            time = i * 5.0
            wlat = result[i][1] / 1000.0
            if wlat > 200.0:
                wlat *= 0.04
            rlat = result[i][2] / 1000.0
            if rlat > 200.0:
                rlat *= 0.04
            result[i] = (time, wlat, rlat)

        time_list, wlat_list, rlat_list = zip(*result)
        results[pcname] = {
            "wzidx": wzidx,
            "rzidx": rzidx,
            "time": list(time_list),
            "wlat": list(wlat_list),
            "rlat": list(rlat_list),
        }

        # doing custom aligned output parsing for second event
        buffer, result, wzeros_cnt, czeros_cnt, dip_found, base_time = (
            [],
            [],
            0,
            0,
            False,
            None,
        )
        with open(f"{output_dir}/{protocol}.{pcname}.0.out", "r") as fout:
            started = False
            for line in fout:
                line = line.strip()
                if not started and line.startswith("Elapsed"):
                    started = True
                elif started and len(line) > 0:
                    segs = line.split()
                    time = float(segs[0])
                    wlat = float(segs[6])
                    rlat = float(segs[8])

                    if time < CHECK2_SECS_BEGIN:
                        continue
                    elif not dip_found:
                        buffer.append((time, wlat, rlat))
                        if len(buffer) > 155:
                            buffer.pop(0)
                        if rlat < 10.0:
                            czeros_cnt += 1
                        else:
                            czeros_cnt = 0
                        if wlat < 10.0:
                            wzeros_cnt += 1
                        else:
                            wzeros_cnt = 0

                        if czeros_cnt == 12:
                            dip_found = True
                            czeros_cnt = 0
                            base_time = buffer[0][0]
                            result = [
                                (time - base_time, wlat, rlat)
                                for time, wlat, rlat in buffer
                            ]
                            if len(result) - czeros_cnt > rcidx:
                                rcidx = len(result) - czeros_cnt
                            if len(result) - wzeros_cnt > wcidx:
                                wcidx = len(result) - wzeros_cnt
                    else:
                        result.append((time - base_time, wlat, rlat))
                        if len(result) >= 300:
                            break

        # concatenate time to the previous list
        for i in range(len(result)):
            time = i * 5.0
            wlat = result[i][1] / 1000.0
            if wlat > 200.0:
                wlat *= 0.04
            rlat = result[i][2] / 1000.0
            if rlat > 200.0:
                rlat *= 0.04
            result[i] = (time + results[pcname]["time"][-1], wlat, rlat)

        time_list, wlat_list, rlat_list = zip(*result)
        results[pcname]["wcidx"] = wcidx + len(results[pcname]["time"])
        results[pcname]["rcidx"] = rcidx + len(results[pcname]["time"])
        results[pcname]["time"].extend(list(time_list))
        results[pcname]["wlat"].extend(list(wlat_list))
        results[pcname]["rlat"].extend(list(rlat_list))

    return results


def print_results(results):
    for pcname, result in results.items():
        print(f"{pcname}")
        i = 0
        while i < len(result["time"]):
            print("  time", end="")
            for j in range(min(12, len(result["time"]) - i)):
                print(f"  {result['time'][i + j]:7.0f}", end="")
            print()
            print("  wlat", end="")
            for j in range(min(12, len(result["time"]) - i)):
                if result["wlat"][i + j] > 0:
                    print(f"  {result['wlat'][i + j]:7.2f}", end="")
                else:
                    print("        -", end="")
            print()
            print("  rlat", end="")
            for j in range(min(12, len(result["time"]) - i)):
                if result["rlat"][i + j] > 0:
                    print(f"  {result['rlat'][i + j]:7.2f}", end="")
                else:
                    print("        -", end="")
            print()
            i += 12


def plot_rlats_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4.8, 1.7),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PCNAME = "Bodega"
    SERIES_ORDER = [
        "wlat",
        "rlat",
    ]
    SERIES_LABEL_COLOR_MARKER_SIZE = {
        "wlat": ("Write", "navy", "h", 2.4),
        "rlat": ("Read", "steelblue", "*", 2.6),
    }

    for series in SERIES_ORDER:
        label, color, marker, markersize = SERIES_LABEL_COLOR_MARKER_SIZE[
            series
        ]
        times, lats = results[PCNAME]["time"], results[PCNAME][series]
        zidx = (
            results[PCNAME]["wzidx"]
            if series == "wlat"
            else results[PCNAME]["rzidx"]
        )
        cidx = (
            results[PCNAME]["wcidx"]
            if series == "wlat"
            else results[PCNAME]["rcidx"]
        )
        times_l = [times[i] / 1000.0 for i in range(zidx) if lats[i] > 0]
        lats_l = [lat for lat in lats[:zidx] if lat > 0]
        times_m = [times[i] / 1000.0 for i in range(zidx, cidx) if lats[i] > 0]
        lats_m = [lat for lat in lats[zidx:cidx] if lat > 0]
        times_r = [
            times[i] / 1000.0 for i in range(cidx, len(times)) if lats[i] > 0
        ]
        lats_r = [lat for lat in lats[cidx:] if lat > 0]

        plt.plot(
            times_l,
            lats_l,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )
        plt.plot(
            times_m,
            lats_m,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            # label=label,
            zorder=10,
        )
        plt.plot(
            times_r,
            lats_r,
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            # label=label,
            zorder=10,
        )

    # failure time
    failure_time = results[PCNAME]["time"][results[PCNAME]["wzidx"]] / 1000.0
    plt.vlines(
        [failure_time - 0.05],
        ymin=0.0,
        ymax=145.0,
        colors=["darkred"],
        linestyles="--",
        linewidth=1.0,
    )
    plt.text(
        failure_time - 0.1,
        160.0,
        "UT node\nfails",
        color="darkred",
        va="baseline",
        ha="center",
    )

    # heartbeat timeout time
    timeout_time = results[PCNAME]["time"][results[PCNAME]["rzidx"]] / 1000.0
    plt.vlines(
        [timeout_time],
        ymin=0.0,
        ymax=45.0,
        colors=["gray"],
        linestyles="--",
        linewidth=1.0,
    )
    plt.text(
        timeout_time - 0.55,
        60.0,
        "hb timeout, changing to\nnew roster (here need to\nwait for lease expiration)",
        color="gray",
        va="baseline",
        ha="left",
    )

    # config change time
    change_time = results[PCNAME]["time"][results[PCNAME]["wcidx"]] / 1000.0
    plt.vlines(
        [change_time],
        ymin=0.0,
        ymax=105.0,
        colors=["darkred"],
        linestyles="--",
        linewidth=1.0,
    )
    plt.text(
        change_time - 0.1,
        135.0,
        "fast regular\nroster change",
        color="darkred",
        va="top",
        ha="right",
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim(left=-0.05)
    plt.xlabel("Time (secs)")

    plt.ylim(bottom=-8.0)
    plt.ylabel("Latency (ms)")

    plt.legend(
        fontsize=8.4,
        handleheight=0.8,
        handlelength=1.4,
        markerscale=2.0,
        loc="upper right",
        bbox_to_anchor=(1.01, 1.2),
        ncol=1,
        borderpad=0.3,
        handletextpad=0.5,
        columnspacing=2.0,
        frameon=False,
    )

    plt.tight_layout()

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


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

#     lgd = plt.legend(
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
            print("Running experiments...")
            for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
                time.sleep(3)

                bench_round(
                    remotes["host0"],
                    remotes[f"host{NUM_REPLICAS}"],
                    base,
                    repo,
                    pcname,
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
        for host in hosts[NUM_REPLICAS : NUM_REPLICAS + NUM_CLIENTS]:
            utils.file.fetch_files_of_dir(
                remotes[host],
                f"/tmp/{EXPER_NAME}",  # from tmpfs
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
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_rlats_results(results, plots_dir)
        # plot_legend(handles, labels, plots_dir)


if __name__ == "__main__":
    main()
