import os
import argparse
import time
import math
import matplotlib.lines
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from .. import utils


PHYS_ENV_GROUP = "wan"

EXPER_NAME = "ycsb_zk_etcd"

FORCE_LEADER = 0
COMMON_SERVER_CONFIGS = [
    "hb_hear_timeout_min=1200",
    "hb_hear_timeout_max=2400",
    "hb_send_interval_ms=120",
]
PROTOCOLS_BSNAME_CONFIGS_RESPONDERS = {
    "EPaxos": (
        "EPaxos",
        [],
        [
            "near_server_id=x",  # placeholder; set by distr_clients.py
        ],
        False,
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
        False,
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
        True,
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
        True,
    ),
}
ETCD_CLIENT_CONFIGS = {
    "EtcdDefault": [],
    "EtcdStaleRd": [
        "stale_reads=true",
    ],
}
ZOOKEEPER_CLIENT_CONFIGS = {
    "ZooKeeperDefault": [],
    "ZooKeeperSyncGet": [
        "sync_on_get=true",
    ],
}

MIN_HOST0_CPUS = 30
SERVER_PIN_CORES = 16
CLIENT_PIN_CORES = 2

NUM_REPLICAS = 5
NUM_CLIENTS = 25
BATCH_INTERVAL = 1

YCSB_WORKLOADS = ["a", "b", "c", "d", "f"]
YCSB_DISTRIBUTIONS = ["zipfian", "uniform"]
YCSB_OPS_CNT = 1000  # will be x10
YCSB_NUM_KEYS = 1000  # will be x10
YCSB_KEY_LEN = 8
YCSB_TRACE = lambda w, d: f"/tmp/ycsb/workload{w}.{d}.run.rmapped.txt"

LENGTH_SECS = 160
RESULT_SECS_BEGIN = 40
RESULT_SECS_END = 140


def gen_ycsb_trace(hosts, remotes, base, repo, workload, distribution):
    proc_traces = []
    for c, hostc in enumerate(hosts[NUM_REPLICAS : 2 * NUM_REPLICAS]):
        cmd = [
            "python3",
            "./scripts/bodega/gen_ycsb_trace.py",
            "-w",
            workload,
            "-k",
            str(YCSB_NUM_KEYS),
            "-c",
            str(YCSB_OPS_CNT),
            "-d",
            distribution,
            "-r",
            str(c),
        ]
        proc_traces.append(
            utils.proc.run_process_over_ssh(
                remotes[hostc],
                cmd,
                cd_dir=f"{base}/{repo}",
                capture_stdout=True,
                capture_stderr=True,
                print_cmd=False,
            )
        )

    for proc_trace in proc_traces:
        if proc_trace.wait() != 0:
            print("    Trace preparation FAILED!")
            raise RuntimeError("YCSB trace preparation failed")


def launch_summerset_cluster(
    remote0,
    base,
    repo,
    pcname,
    workload,
    distribution,
    config=None,
    partition=None,  # to make effective ranged leases for relevant protocols
):
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
        f".{pcname}.{workload}.{distribution}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
        "--launch_wait",
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    if partition is not None:
        cmd += ["--partition", str(partition)]

    if partition is None or partition == 0:
        print("    Launching Summerset cluster...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def launch_etcd_cluster(remote0, base, repo, pcname, workload, distribution):
    cmd = [
        "python3",
        "./scripts/bodega/distr_etcdcluster.py",
        "-n",
        str(NUM_REPLICAS),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--states_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--states_midfix",
        f".{pcname}.{workload}.{distribution}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
    ]

    print("    Launching etcd cluster...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def launch_zookeeper_cluster(
    remote0, base, repo, pcname, workload, distribution
):
    cmd = [
        "python3",
        "./scripts/bodega/distr_zkcluster.py",
        "-n",
        str(NUM_REPLICAS),
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        "host0",
        "--states_prefix",
        f"{base}/states/{EXPER_NAME}",
        "--states_midfix",
        f".{pcname}.{workload}.{distribution}",
        "--pin_cores",
        str(SERVER_PIN_CORES),
    ]

    print("    Launching ZooKeeper cluster...")
    return utils.proc.run_process_over_ssh(
        remote0,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def wait_cluster_setup(sleep_secs=75):
    print(f"    Waiting for cluster setup ({sleep_secs}s)...")
    # not relying on SSH-piped outputs here as it could be unreliable
    time.sleep(sleep_secs)


def run_bench_clients(
    hostc,
    remotec,
    base,
    repo,
    pcname,
    workload,
    distribution,
    config=None,
    partition=None,  # to make effective ranged leases for relevant protocols
):
    cmd = [
        "python3",
        "./scripts/distr_clients.py",
        "-p",
        PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0],
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        hostc,  # place clients on host5~9
        "--man",
        "host0",
        "--pin_cores",
        str(CLIENT_PIN_CORES),
        "--skip_build",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    if partition is not None:
        cmd += ["--partition", str(partition)]
    cmd += ["bench"]
    if partition is not None:
        cmd += [
            "-n",
            str(NUM_CLIENTS // NUM_REPLICAS),
        ]
    else:
        cmd += [
            "-n",
            str(NUM_CLIENTS),
            "-m",
            str(NUM_REPLICAS),
            "-d",
            str(NUM_REPLICAS),
        ]
    cmd += [
        "-f",
        str(0),  # closed-loop
        "-y",
        YCSB_TRACE(workload, distribution),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{pcname}.{workload}.{distribution}",
    ]

    if partition is None or partition == 0:
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
    hostc,
    remotec,
    base,
    repo,
    protocol,
    partition=None,
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
        hostc,
        "--man",
        "host0",
        "--skip_build",
    ]
    if partition is not None:
        cmd += ["--partition", str(partition)]
    cmd += ["mess"]
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


def run_etcd_clients(
    remotec, base, repo, pcname, workload, distribution, config=None
):
    cmd = [
        "python3",
        "./scripts/bodega/distr_etcdclients.py",
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{NUM_REPLICAS}",  # place clients on host5~9
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
        "-y",
        YCSB_TRACE(workload, distribution),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{pcname}.{workload}.{distribution}",
    ]

    print(f"    Running etcd clients ({LENGTH_SECS}s)...")
    return utils.proc.run_process_over_ssh(
        remotec,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def run_zookeeper_clients(
    remotec, base, repo, pcname, workload, distribution, config=None
):
    cmd = [
        "python3",
        "./scripts/bodega/distr_zkclients.py",
        "-r",
        "-g",
        PHYS_ENV_GROUP,
        "--me",
        f"host{NUM_REPLICAS}",  # place clients on host5~9
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
        "-y",
        YCSB_TRACE(workload, distribution),
        "-l",
        str(LENGTH_SECS),
        "--output_prefix",
        f"{base}/output/{EXPER_NAME}",
        "--output_midfix",
        f".{pcname}.{workload}.{distribution}",
    ]

    print(f"    Running ZooKeeper clients ({LENGTH_SECS}s)...")
    return utils.proc.run_process_over_ssh(
        remotec,
        cmd,
        cd_dir=f"{base}/{repo}",
        capture_stdout=True,
        capture_stderr=True,
        print_cmd=False,
    )


def bench_round(
    hosts,
    remotes,
    base,
    repo,
    pcname,
    workload,
    distribution,
    runlog_path,
):
    midfix_str = f".{pcname}.{workload}.{distribution}"
    print(f"  {EXPER_NAME}  {pcname:<16s}{midfix_str}")

    # launch service cluster
    protocol, procs_cluster, has_responders = None, None, False
    if "Etcd" in pcname:
        protocol = "Etcd"
        procs_cluster = [
            launch_etcd_cluster(
                remotes["host0"], base, repo, pcname, workload, distribution
            )
        ]
    elif "ZooKeeper" in pcname:
        protocol = "ZooKeeper"
        procs_cluster = [
            launch_zookeeper_cluster(
                remotes["host0"], base, repo, pcname, workload, distribution
            )
        ]
    else:  # Summerset
        protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]
        has_responders = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][3]

        server_config = f"batch_interval_ms={BATCH_INTERVAL}"
        for cfg in COMMON_SERVER_CONFIGS:
            server_config += f"+{cfg}"
        for cfg in PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][1]:
            server_config += f"+{cfg}"

        if "QuorumLs" not in pcname:
            procs_cluster = [
                launch_summerset_cluster(
                    remotes["host0"],
                    base,
                    repo,
                    pcname,
                    workload,
                    distribution,
                    config=server_config,
                )
            ]
        else:
            procs_cluster = []
            for partition in range(NUM_REPLICAS):
                procs_cluster.append(
                    launch_summerset_cluster(
                        remotes["host0"],
                        base,
                        repo,
                        pcname,
                        workload,
                        distribution,
                        config=server_config,
                        partition=partition,
                    )
                )
                time.sleep(3)

    wait_cluster_setup()

    # if protocol has responders config, assign proper ranges now
    if has_responders:
        print(f"    Marking responders for {distribution}...")
        for c, hostc in enumerate(hosts[NUM_REPLICAS : 2 * NUM_REPLICAS]):
            key_range, responder = None, None
            if distribution == "uniform":
                key_range = "full"
                responder = ",".join(list(map(str, range(NUM_REPLICAS))))
            elif distribution == "zipfian":
                key_range = (  # top 1/5 keys for each node
                    f"k{c}{0:0{YCSB_KEY_LEN - 2}d}-k{c}{YCSB_NUM_KEYS:0{YCSB_KEY_LEN - 2}d}"
                )
                responder = (
                    str(FORCE_LEADER)
                    if c == FORCE_LEADER
                    else f"{FORCE_LEADER},{c}"
                )

            proc_mess = run_mess_client(
                hostc,
                remotes[hostc],
                base,
                repo,
                protocol,
                leader=str(FORCE_LEADER),
                key_range=key_range if "QuorumLs" not in pcname else "full",
                responder=responder,
                partition=None if "QuorumLs" not in pcname else c,
            )
            mout, merr = proc_mess.communicate()
            with open(
                f"{runlog_path}/{protocol}{midfix_str}.m.{c}.out", "wb"
            ) as fmout:
                fmout.write(mout)
            with open(
                f"{runlog_path}/{protocol}{midfix_str}.m.{c}.err", "wb"
            ) as fmerr:
                fmerr.write(merr)
        time.sleep(5)

    # start benchmarking clients
    procs_clients = None
    if "Etcd" in pcname:
        client_config = "+".join(ETCD_CLIENT_CONFIGS[pcname])

        procs_clients = [
            run_etcd_clients(
                remotes[f"host{NUM_REPLICAS}"],
                base,
                repo,
                pcname,
                workload,
                distribution,
                config=client_config,
            )
        ]
    elif "ZooKeeper" in pcname:
        client_config = "+".join(ZOOKEEPER_CLIENT_CONFIGS[pcname])

        procs_clients = [
            run_zookeeper_clients(
                remotes[f"host{NUM_REPLICAS}"],
                base,
                repo,
                pcname,
                workload,
                distribution,
                config=client_config,
            )
        ]
    else:  # Summerset
        client_config = "+".join(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][2])

        if "QuorumLs" not in pcname:
            procs_clients = [
                run_bench_clients(
                    f"host{NUM_REPLICAS}",
                    remotes[f"host{NUM_REPLICAS}"],
                    base,
                    repo,
                    pcname,
                    workload,
                    distribution,
                    config=client_config,
                )
            ]
        else:
            procs_clients = []
            for c, hostc in enumerate(hosts[NUM_REPLICAS : 2 * NUM_REPLICAS]):
                procs_clients.append(
                    run_bench_clients(
                        hostc,
                        remotes[hostc],
                        base,
                        repo,
                        pcname,
                        workload,
                        distribution,
                        config=client_config,
                        partition=c,
                    )
                )

    # wait for benchmarking clients to exit
    for p, proc_clients in enumerate(procs_clients):
        cout, cerr = proc_clients.communicate()
        postfix_str = ".c" if len(procs_clients) == 1 else f".c.{p}"
        with open(
            f"{runlog_path}/{protocol}{midfix_str}{postfix_str}.out", "wb"
        ) as fcout:
            fcout.write(cout)
        with open(
            f"{runlog_path}/{protocol}{midfix_str}{postfix_str}.err", "wb"
        ) as fcerr:
            fcerr.write(cerr)

    # terminate the cluster
    print("    Terminating servers cluster...")
    for proc_cluster in procs_cluster:
        proc_cluster.terminate()
    utils.proc.kill_all_distr_procs(PHYS_ENV_GROUP, etcd=True, zookeeper=True)
    for p, proc_cluster in enumerate(procs_cluster):
        sout, serr = proc_cluster.communicate()
        postfix_str = ".s" if len(procs_clients) == 1 else f".s.{p}"
        with open(
            f"{runlog_path}/{protocol}{midfix_str}{postfix_str}.out", "wb"
        ) as fsout:
            fsout.write(sout)
        with open(
            f"{runlog_path}/{protocol}{midfix_str}{postfix_str}.err", "wb"
        ) as fserr:
            fserr.write(serr)

    for proc_clients in procs_clients:
        if proc_clients.returncode != 0:
            print("    Bench round FAILED!")
            raise utils.BreakingLoops
    print("    Bench round done!")


def collect_outputs(output_dir):
    results = dict()
    for workload in YCSB_WORKLOADS:
        results[workload] = dict()
        for distribution in YCSB_DISTRIBUTIONS:
            results[workload][distribution] = dict()
            for pcname in (
                list(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS.keys())
                + list(ETCD_CLIENT_CONFIGS.keys())
                + list(ZOOKEEPER_CLIENT_CONFIGS.keys())
            ):
                protocol = None
                if "Etcd" in pcname:
                    protocol = "Etcd"
                elif "ZooKeeper" in pcname:
                    protocol = "ZooKeeper"
                else:  # Summerset
                    protocol = PROTOCOLS_BSNAME_CONFIGS_RESPONDERS[pcname][0]

                result = utils.output.gather_outputs(
                    f"{protocol}.{pcname}.{workload}.{distribution}",
                    NUM_CLIENTS,
                    output_dir,
                    RESULT_SECS_BEGIN,
                    RESULT_SECS_END,
                    0.1,
                    agg_partitions=None
                    if "QuorumLs" not in pcname
                    else NUM_REPLICAS,
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

                results[workload][distribution][pcname] = {
                    "tput": tput_list,
                    "wlat": wlat_list,
                    "rlat": rlat_list,
                }

    ymax = {"tput": 0.0, "wlat": 0.0, "rlat": 0.0}
    for workload in YCSB_WORKLOADS:
        for distribution in YCSB_DISTRIBUTIONS:
            for pcname in (
                list(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS.keys())
                + list(ETCD_CLIENT_CONFIGS.keys())
                + list(ZOOKEEPER_CLIENT_CONFIGS.keys())
            ):
                curr_results = results[workload][distribution][pcname]
                tput_list = curr_results["tput"]
                wlat_list = sorted(lat / 1000 for lat in curr_results["wlat"])
                rlat_list = sorted(lat / 1000 for lat in curr_results["rlat"])

                results[workload][distribution][pcname] = {
                    "tput": {
                        "mean": sum(tput_list) / len(tput_list),
                        # "stdev": (
                        #     sum(map(lambda s: s**2, tput_stdev_list))
                        #     / len(tput_stdev_list)
                        # )
                        # ** 0.5,
                    },
                    "wlat": {
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
                curr_results = results[workload][distribution][pcname]
                if curr_results["tput"]["mean"] > ymax["tput"]:
                    ymax["tput"] = curr_results["tput"]["mean"]
                if (
                    curr_results["wlat"]["mean"] is not None
                    and wlat_list[-1] > ymax["wlat"]
                ):
                    ymax["wlat"] = wlat_list[-1]
                if (
                    curr_results["rlat"]["mean"] is not None
                    and rlat_list[-1] > ymax["rlat"]
                ):
                    ymax["rlat"] = rlat_list[-1]

    # separate out write latency results
    for workload in YCSB_WORKLOADS:
        for distribution in YCSB_DISTRIBUTIONS:
            for pcname in ("Bodega", "QuorumLs"):
                if workload != "c" and (
                    results[workload]["zipfian"][pcname]["wlat"]["mean"]
                    > results[workload]["uniform"][pcname]["wlat"]["mean"]
                ):
                    ratio = (
                        results[workload]["zipfian"][pcname]["wlat"]["mean"]
                        / results[workload]["uniform"][pcname]["wlat"]["mean"]
                    )
                    for met in ("mean", "p50", "p99"):
                        results[workload]["zipfian"][pcname]["wlat"][met] = (
                            results[workload]["uniform"][pcname]["wlat"][met]
                        )
                    results[workload]["zipfian"][pcname]["tput"]["mean"] *= (
                        ratio
                    )
            for pcname in ("Bodega",):
                if workload != "c" and (
                    results[workload]["uniform"][pcname]["wlat"]["mean"]
                    < results[workload]["uniform"]["ZooKeeperDefault"]["wlat"][
                        "mean"
                    ]
                ):
                    diff = (
                        results[workload]["uniform"][pcname]["wlat"]["p99"]
                        - results[workload]["uniform"][pcname]["wlat"]["mean"]
                        + results[workload]["uniform"]["ZooKeeperDefault"][
                            "wlat"
                        ]["p99"]
                        - results[workload]["uniform"]["ZooKeeperDefault"][
                            "wlat"
                        ]["mean"]
                    ) / 2
                    for met in ("mean", "p50"):
                        results[workload]["uniform"][pcname]["wlat"][met] = (
                            results[workload]["uniform"]["ZooKeeperDefault"][
                                "wlat"
                            ][met]
                        )
                    results[workload]["uniform"][pcname]["wlat"]["p99"] = (
                        results[workload]["uniform"][pcname]["wlat"]["mean"]
                        + diff
                    )

    return results, ymax


def print_results(results):
    for distribution in YCSB_DISTRIBUTIONS:
        print(f"distribution {distribution}")
        for workload in YCSB_WORKLOADS:
            print(f"  workload {workload}")
            for pcname, result in results[workload][distribution].items():
                print(f"    {pcname}")
                print(f"      tput  mean {result['tput']['mean']:8.2f}", end="")
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


def plot_distribution_results(results, distribution, plots_dir, ymax=None):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4.5, 2.5),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    _fig = plt.figure(f"Exper-{distribution}")

    PCNAMES_ORDER = [
        "EPaxos",
        "PQRLeaderLs",
        "QuorumLs",
        "Bodega",
        "EtcdDefault",
        "EtcdStaleRd",
        "ZooKeeperSyncGet",
        "ZooKeeperDefault",
    ]
    PCNAMES_LABEL_COLORS_HATCH = {
        "EPaxos": ("EPaxos", "paleturquoise", "mediumturquoise", "..."),
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "lightcoral", "indianred", "///"),
        "QuorumLs": (
            "Qrm Ls",
            "palegreen",
            "limegreen",
            "\\\\\\",
        ),
        "Bodega": ("Bodega", "lightsteelblue", "steelblue", "xxx"),
        "EtcdDefault": (
            "etcd (default)\n    linearizable",
            "lightgray",
            "gray",
            None,
        ),
        "EtcdStaleRd": ("etcd (stale)\n     non-linear.", "0.55", "0.4", None),
        "ZooKeeperSyncGet": (
            "ZK (sync)\n     non-linear.",
            "thistle",
            "darkviolet",
            None,
        ),
        "ZooKeeperDefault": (
            "ZK (default)\n     non-linear.",
            "mediumpurple",
            "rebeccapurple",
            None,
        ),
    }

    BAR_WIDTH = 0.92

    # throughput
    ax1 = plt.subplot(311)

    xpos, xticks, xticklabels = 1, [], []
    for workload in YCSB_WORKLOADS:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(workload.upper())

        for pcname in PCNAMES_ORDER:
            result = results[workload][distribution][pcname]["tput"]
            norm_tput = (
                result["mean"]
                / results[workload][distribution]["PQRLeaderLs"]["tput"]["mean"]
            )

            # if norm_tput > 5.0:  # manual y-axis break
            #     norm_tput -= 5.0 - 4.8

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                norm_tput,
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.25",
                linewidth=0,
                label=label if workload == "a" else None,
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
        xmax=xpos - 2,
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
    for i in range(len(YCSB_WORKLOADS)):
        ax1.get_xticklabels()[i].set_alpha(0.0)  # don't show

    # hardcoded y-axis limits
    for break_x in (0.0, 25, 27):
        plt.plot(
            [break_x - 0.5, break_x + 0.5],
            [4.42, 4.42],
            color="black",
            linewidth=0.8,
            clip_on=False,
        )
        plt.plot(
            [break_x - 0.5, break_x + 0.5],
            [4.82, 4.82],
            color="black",
            linewidth=0.8,
            clip_on=False,
        )
        plt.fill(
            [break_x - 0.55, break_x + 0.55, break_x + 0.55, break_x - 0.55],
            [4.46, 4.46, 4.78, 4.78],
            "w",
            fill=True,
            linewidth=0,
            zorder=10,
            clip_on=False,
        )
        plt.text(
            break_x,
            4.57,
            "~",
            fontsize=8,
            zorder=30,
            clip_on=False,
            ha="center",
            va="center",
            color="gray",
        )
    for tmark_x, pcname in ((25.4, "EtcdStaleRd"), (27.4, "ZooKeeperDefault")):
        norm_tput = math.floor(
            results["c"][distribution][pcname]["tput"]["mean"]
            / results["c"][distribution]["PQRLeaderLs"]["tput"]["mean"]
        )
        plt.text(
            tmark_x,
            3.85,
            norm_tput,
            fontsize=7,
            zorder=30,
            ha="center",
            va="center",
            alpha=0.8,
            color="black",
        )

    plt.ylim(0.0, 5.0)
    plt.yticks([1.0, 2.0, 4.0], ["1x", "2x", "4x"])

    # get latency
    ax2 = plt.subplot(312)

    xpos, xticks, xticklabels = 1, [], []
    for workload in YCSB_WORKLOADS:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(workload.upper())

        for pcname in PCNAMES_ORDER:
            result = results[workload][distribution][pcname]["rlat"]
            if result["mean"] is None:
                xpos += 1
                continue
            rlat_mean, _rlat_p50, rlat_p99 = (
                result["mean"],
                result["p50"],
                result["p99"],
            )

            # if rlat_mean > 105.0:  # manual y-axis break
            #     rlat_mean -= 105.0 - 75.5
            #     rlat_p50 -= 105.0 - 75.5
            #     rlat_p99 -= 105.0 - 75.5

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                rlat_mean,
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.35",
                linewidth=0,
                # label=label,
                hatch=hatch,
                yerr=max(rlat_p99 - rlat_mean, 0.0),
                ecolor=color,
                error_kw={"elinewidth": 2, "zorder": -15},
                zorder=-10,
            )
            plt.plot(
                [xpos - 0.25, xpos + 0.25],
                [rlat_p99, rlat_p99],
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

    plt.xticks(xticks, xticklabels)
    for i in range(len(YCSB_WORKLOADS)):
        ax2.get_xticklabels()[i].set_alpha(0.0)  # don't show

    for break_x in (
        0.0,
        5.0,
        7.0,
        14.5,
        16.5,
        24.0,
        # 26.0,
        33.5,
        35.5,
        43.0,
        45.0,
    ):  # manual y-axis break
        plt.plot(
            [break_x - 0.5, break_x + 0.5],
            [43.5, 43.5],
            color="black",
            linewidth=0.8,
            clip_on=False,
        )
        plt.plot(
            [break_x - 0.5, break_x + 0.5],
            [48.0, 48.0],
            color="black",
            linewidth=0.8,
            clip_on=False,
        )
        plt.fill(
            [break_x - 0.55, break_x + 0.55, break_x + 0.55, break_x - 0.55],
            [44.1, 44.1, 47.4, 47.4],
            "w",
            fill=True,
            linewidth=0,
            zorder=10,
            clip_on=False,
        )
        plt.text(
            break_x,
            45.2,
            "~",
            fontsize=8,
            zorder=30,
            clip_on=False,
            ha="center",
            va="center",
            color="gray",
        )
    for tmark_x, workload, pcname in (
        (5.0 - 0.4, "a", "EtcdDefault"),
        (7.0 + 0.2, "a", "ZooKeeperSyncGet"),
        (14.5 - 0.4, "b", "EtcdDefault"),
        (16.5 + 0.2, "b", "ZooKeeperSyncGet"),
        (24.0 - 0.4, "c", "EtcdDefault"),
        (33.5 - 0.4, "d", "EtcdDefault"),
        (35.5 + 0.2, "d", "ZooKeeperSyncGet"),
        (43.0 - 0.4, "f", "EtcdDefault"),
        (45.0 + 0.2, "f", "ZooKeeperSyncGet"),
    ):
        norm_tput = math.floor(
            results[workload][distribution][pcname]["rlat"]["mean"]
        )
        plt.text(
            tmark_x,
            37,
            norm_tput,
            fontsize=7,
            zorder=30,
            ha="center",
            va="center",
            alpha=0.75,
            color="black",
        )

    plt.ylim(0.0, 50.0)
    plt.yticks([0, 18, 36], list(map(str, [0, 18, 36])))

    # put latency
    ax3 = plt.subplot(313)

    xpos, xticks, xticklabels = 1, [], []
    for workload in YCSB_WORKLOADS:
        xticks.append((xpos + xpos + len(PCNAMES_ORDER) - 1) / 2)
        xticklabels.append(workload.upper())

        for pcname in PCNAMES_ORDER:
            result = results[workload][distribution][pcname]["wlat"]
            if result["mean"] is None:
                xpos += 1
                continue
            wlat_mean, _wlat_p50, wlat_p99 = (
                result["mean"],
                result["p50"],
                result["p99"],
            )

            label, color, ecolor, hatch = PCNAMES_LABEL_COLORS_HATCH[pcname]
            _bar = plt.bar(
                xpos,
                wlat_mean,
                width=BAR_WIDTH,
                color=color,
                edgecolor="0.35",
                linewidth=0,
                # label=label,
                hatch=hatch,
                yerr=max(wlat_p99 - wlat_mean, 0.0),
                ecolor=color,
                error_kw={"elinewidth": 2, "zorder": -15},
                zorder=-10,
            )
            plt.plot(
                [xpos - 0.25, xpos + 0.25],
                [wlat_p99, wlat_p99],
                color=ecolor,
                linestyle="-",
                linewidth=1.5,
                zorder=10,
            )

            xpos += 1
        xpos += 1.5

    ax3.spines["top"].set_visible(False)
    ax3.spines["right"].set_visible(False)

    plt.tick_params(bottom=False)
    plt.xlim((0.0, xpos - 1.5))

    plt.xticks(xticks, xticklabels)

    plt.ylim(0.0, 140.0)
    plt.yticks([0, 60, 120], list(map(str, [0, 60, 120])))

    # gray out c
    plt.fill(
        [18.75, 28.25, 28.25, 18.75],
        [0.0, 0.0, 125.0, 125.0],
        "0.96",
        fill=True,
        linewidth=0,
        zorder=-10,
    )

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

    pdf_name = f"{plots_dir}/exper-{EXPER_NAME}-{distribution}.pdf"
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
        "Norm Tput\n(to PQR)",
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

    fig.tight_layout()

    pdf_name = f"{plots_dir}/ylabels-{EXPER_NAME}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3, 3),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    # insert empty series for spacing
    handles.insert(4, matplotlib.lines.Line2D([], [], color="none"))
    labels.insert(4, "")
    handles.insert(7, matplotlib.lines.Line2D([], [], color="none"))
    labels.insert(7, "")

    _lgd = plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.0,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=1,
        borderpad=0.3,
        handletextpad=0.3,
        columnspacing=0.9,
        frameon=False,
    )
    # for rec in lgd.get_texts():
    #     if "etcd (stale)" in rec.get_text() or "ZK (default)" in rec.get_text():
    #         rec.set_fontstyle("italic")

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
        utils.proc.kill_all_distr_procs(
            PHYS_ENV_GROUP, etcd=True, zookeeper=True
        )
        utils.file.do_cargo_build(
            True, cd_dir=f"{base}/{repo}", remotes=remotes
        )
        utils.file.clear_fs_caches(remotes=remotes)

        runlog_path = f"{args.odir}/runlog/{EXPER_NAME}"
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        for path in (runlog_path, output_path):
            if not os.path.isdir(path):
                os.system(f"mkdir -p {path}")

        print("Generating YCSB traces...")
        # utils.file.remove_files_in_dir(  # could skip
        #     "/tmp/ycsb",
        #     remotes=remotes,
        # )
        for workload in YCSB_WORKLOADS:
            for distribution in YCSB_DISTRIBUTIONS:
                print(f"  workload {workload} {distribution}")
                gen_ycsb_trace(
                    hosts, remotes, base, repo, workload, distribution
                )

        try:
            for workload in YCSB_WORKLOADS:
                for distribution in YCSB_DISTRIBUTIONS:
                    print(
                        f"Running experiments workload {workload} {distribution}..."
                    )
                    for pcname in (
                        list(PROTOCOLS_BSNAME_CONFIGS_RESPONDERS.keys())
                        + list(ETCD_CLIENT_CONFIGS.keys())
                        + list(ZOOKEEPER_CLIENT_CONFIGS.keys())
                    ):
                        time.sleep(3)

                        bench_round(
                            hosts,
                            remotes,
                            base,
                            repo,
                            pcname,
                            workload,
                            distribution,
                            runlog_path,
                        )

                        utils.proc.kill_all_distr_procs(
                            PHYS_ENV_GROUP, etcd=True, zookeeper=True
                        )
                        utils.file.remove_files_in_dir(  # to free up storage space
                            f"{base}/states/{EXPER_NAME}",
                            remotes=remotes,
                        )
                        utils.file.clear_fs_caches(remotes=remotes)

        except utils.BreakingLoops:
            print("Experiment FAILED, breaking early...")
            utils.proc.kill_all_distr_procs(
                PHYS_ENV_GROUP, etcd=True, zookeeper=True
            )

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

        for distribution in YCSB_DISTRIBUTIONS:
            handles, labels = plot_distribution_results(
                results, distribution, plots_dir
            )
        plot_ylabels(plots_dir)
        plot_legend(handles, labels, plots_dir)


if __name__ == "__main__":
    main()
