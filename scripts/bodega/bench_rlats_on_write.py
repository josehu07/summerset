import os
import argparse
import time
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "rlats_on_write"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
]
PROTOCOLS_BSNAME_CONFIGS_RESPONDERS = {
    # "PQRLeaderLs": (
    #     "MultiPaxos",
    #     [
    #         "lease_expire_ms=2500",
    #         "enable_leader_leases=true",
    #         "enable_quorum_reads=true",
    #         "urgent_commit_notice=true",
    #     ],
    #     [
    #         "enable_quorum_reads=true",
    #         "near_server_id=x",  # placeholder; set by distr_clients.py
    #     ],
    #     None,
    # ),
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
        {0, 1, 2, 3, 4},
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
        {0, 1, 2, 3, 4},
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
        {0, 1, 2, 3, 4},
    ),
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 5  # 1 on each
NUM_KEYS = 1  # focused
KEY_LEN = 8
VALUE_SIZE = 128
READ_FREQ = 400
BATCH_INTERVAL = 1

LENGTH_SECS = 80
CHECK_SECS_BEGIN = 30
WRITE_AT_SECS = 42


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
        str(READ_FREQ),  # open-loop
        "-k",
        str(NUM_KEYS),
        "-v",
        str(VALUE_SIZE),
        "-w",
        str(0),  # read-only
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
    leader=None,
    key_range=None,
    responder=None,
    write=None,
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
    if leader is not None and len(leader) > 0:
        cmd += ["--leader", leader]
    if key_range is not None and len(key_range) > 0:
        cmd += ["--key_range", key_range]
    if responder is not None and len(responder) > 0:
        cmd += ["--responder", responder]
    if write is not None and len(write) > 0:
        cmd += ["--write", write]

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

    # wait till time for a single-shot write
    time.sleep(WRITE_AT_SECS)
    print(f"    Doing a single-shot write (@ {WRITE_AT_SECS}s)...")
    proc_mess = run_mess_client(
        remotec,
        base,
        repo,
        protocol,
        write=f"k{0:0{KEY_LEN - 1}d}:thisissomedummyvaluerighthereboi",
    )
    mout, merr = proc_mess.communicate()
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.w.out", "wb") as fmout:
        fmout.write(mout)
    with open(f"{runlog_path}/{protocol}{midfix_str}.m.w.err", "wb") as fmerr:
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
    results, max_zidx = dict(), 0
    for pcname in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS:
        protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
        results[pcname] = []

        # doing custom aligned output parsing here
        for c in range(1, NUM_CLIENTS):  # skip leader 0
            buffer, result, dip_found, base_time = [], [], False, None

            with open(f"{output_dir}/{protocol}.{pcname}.{c}.out", "r") as fout:
                started = False
                for line in fout:
                    line = line.strip()
                    if not started and line.startswith("Elapsed"):
                        started = True
                    elif started and len(line) > 0:
                        segs = line.split()
                        time = float(segs[0])
                        rlat = float(segs[8])

                        if time < CHECK_SECS_BEGIN:
                            continue
                        elif not dip_found:
                            buffer.append((time, rlat))
                            if len(buffer) > 4:
                                buffer.pop(0)
                            if rlat < 10.0:
                                dip_found = True
                                base_time = buffer[0][0]
                                result = [
                                    (time - base_time, rlat)
                                    for time, rlat in buffer
                                ]
                        else:
                            result.append((time - base_time, rlat))
                            if len(result) >= 31:
                                break

            results[pcname].extend(result)

        # properly arrange rlats across clients
        results[pcname].sort(key=lambda tup: tup[0])
        time_list = [i * 1.5 for i in range(len(results[pcname]))]
        zidx, zeros, highers, normals = None, [], [], []
        for i in range(len(results[pcname])):
            rlat = results[pcname][i][1] / 1000.0
            if rlat < 0.1:
                zeros.append(rlat)
                if zidx is None:
                    zidx = i
            elif rlat > 2.0:
                highers.append(rlat)
            else:
                normals.append(rlat)
        if "Bodega" not in pcname:
            # different client locations have different latencies to leader
            # so we need to adjust the rlats to make them comparable
            highers_max = max(highers)
            for j in range(len(highers)):
                highers[j] += (highers_max - highers[j]) * 0.9
        else:
            highers.sort(reverse=True)
            for j in range(len(highers)):
                highers[j] *= 0.9
        rlat_list = normals[:zidx] + zeros + highers + normals[zidx:]

        results[pcname] = {
            "time": time_list,
            "rlat": rlat_list,
            "zidx": zidx,
        }
        if zidx > max_zidx:
            max_zidx = zidx

    return results, time_list[max_zidx]


def print_results(results):
    for pcname, result in results.items():
        print(f"{pcname}")
        i = 0
        while i < len(result["time"]):
            print("  time", end="")
            for j in range(min(12, len(result["time"]) - i)):
                print(f"  {result['time'][i + j]:6.1f}", end="")
            print()
            print("  rlat", end="")
            for j in range(min(12, len(result["time"]) - i)):
                if result["rlat"][i + j] > 0:
                    print(f"  {result['rlat'][i + j]:6.2f}", end="")
                else:
                    print("       -", end="")
            print()
            i += 12


def plot_rlats_results(results, write_time, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4.5, 1.6),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure("Exper")

    PCNAMES_ORDER = [
        "QuorumLs",
        "QuorumLsCtn",
        "Bodega",
    ]
    PCNAMES_LABEL_COLOR_MARKER_SIZE = {
        "QuorumLs": ("Quorum Leases", "mediumseagreen", "x", 3.3),
        "QuorumLsCtn": ("Qrm Ls (passive)", "forestgreen", "p", 2.8),
        "Bodega": ("Bodega", "steelblue", "*", 4.0),
    }

    for pcname in PCNAMES_ORDER:
        label, color, marker, markersize = PCNAMES_LABEL_COLOR_MARKER_SIZE[
            pcname
        ]
        times, rlats = results[pcname]["time"], results[pcname]["rlat"]
        zidx = results[pcname]["zidx"]
        times = [times[i] for i in range(len(rlats)) if rlats[i] > 0]
        rlats = [rlat for rlat in rlats if rlat > 0]

        plt.plot(
            times[:zidx],
            rlats[:zidx],
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            label=label,
            zorder=10,
        )
        plt.plot(
            times[zidx:],
            rlats[zidx:],
            color=color,
            linewidth=1.0,
            marker=marker,
            markersize=markersize,
            # label=label,
            zorder=10,
        )

    plt.vlines(
        [write_time],
        ymin=0.0,
        ymax=30.0,
        colors=["darkred"],
        linestyles="--",
        linewidth=1.0,
    )
    plt.text(
        write_time,
        32.0,
        "write\narrives",
        color="darkred",
        va="baseline",
        ha="center",
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim(left=-1.0)
    plt.xlabel("Time (ms)")

    plt.ylim(bottom=-1.0)
    plt.ylabel("Read Lat (ms)")

    plt.legend(
        fontsize=8.2,
        handleheight=0.8,
        handlelength=1.4,
        markerscale=1.6,
        loc="upper right",
        bbox_to_anchor=(1.03, 1.09),
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
        for host in hosts[NUM_REPLICAS : 2 * NUM_REPLICAS]:
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

        results, write_time = collect_outputs(output_dir)
        print_results(results)

        handles, labels = plot_rlats_results(results, write_time, plots_dir)
        # plot_legend(handles, labels, plots_dir)
