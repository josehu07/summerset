import sys
import os
import signal
import argparse
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


COCK_REPO_NAME = "cockroach"


SERVER_SQL_PORT = 26157
SERVER_LISTEN_PORT = 26257
SERVER_HTTP_PORT = 28080


PROTOCOL_STORE_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}"
)

PROTOCOLS = {"Raft", "Crossword", "CRaft"}


def run_process_pinned(
    cmd,
    capture_stderr=False,
    cores_per_proc=0,
    remote=None,
    cd_dir=None,
    extra_env=None,
):
    cpu_list = None
    if cores_per_proc > 0:
        # get number of processors
        num_cpus = utils.proc.get_cpu_count(remote=remote)
        # pin servers at CPUs [0, cores_per_proc)
        core_start = 0
        core_end = core_start + cores_per_proc - 1
        assert core_end <= num_cpus - 1
        cpu_list = f"{core_start}-{core_end}"
    if remote is None or len(remote) == 0:
        return utils.proc.run_process(
            cmd,
            capture_stderr=capture_stderr,
            cd_dir=cd_dir,
            cpu_list=cpu_list,
            extra_env=extra_env,
        )
    else:
        return utils.proc.run_process_over_ssh(
            remote,
            cmd,
            capture_stderr=capture_stderr,
            cd_dir=cd_dir,
            cpu_list=cpu_list,
            extra_env=extra_env,
        )


def compose_server_cmd(
    protocol,
    this_ip,
    sql_port,
    listen_port,
    http_port,
    join_list,
    replica_id,
    remote,
    states_prefix,
    states_midfix,
    fresh_files,
):
    backer_dir = PROTOCOL_STORE_PATH(
        protocol, states_prefix, states_midfix, replica_id
    )
    if fresh_files:
        utils.proc.run_process_over_ssh(
            remote,
            ["sudo", "rm", "-rf", backer_dir],
            print_cmd=False,
        ).wait()

    cmd = [
        "./cockroach",
        "start",
        "--insecure",
        f"--store={backer_dir}",
        f"--listen-addr=0.0.0.0:{listen_port}",
        f"--advertise-addr={this_ip}:{listen_port}",
        f"--sql-addr=0.0.0.0:{sql_port}",
        f"--advertise-sql-addr={this_ip}:{sql_port}",
        f"--http-addr=0.0.0.0:{http_port}",
        f"--advertise-http-addr={this_ip}:{http_port}",
        "--cache=.25",
        "--max-sql-memory=.25",
        f"--locality=node=n{replica_id}",
        f"--join={','.join(join_list)}",
    ]
    return cmd


def launch_servers(
    remotes,
    ipaddrs,
    hosts,
    me,
    cd_dir,
    protocol,
    cluster_size,
    value_size,
    states_prefix,
    states_midfix,
    fresh_files,
    pin_cores,
    size_profiling,
    rscoding_timing,
    try_force_leader,
    min_range_id,
    min_payload,
    fixed_num_voters,
):
    if cluster_size != len(remotes):
        raise ValueError(f"invalid cluster_size: {cluster_size}")

    join_list = [
        f"{ipaddrs[hosts[r]]}:{SERVER_LISTEN_PORT}" for r in range(cluster_size)
    ]

    extra_env = {"COCKROACH_RAFT_ENABLE_CHECKQUORUM": "false"}
    if value_size < 1 or value_size > 4096:
        raise ValueError(
            f"textScale {value_size} too large: expect in range [1, 4096]"
        )
    extra_env["COCKROACH_TPCC_TEXT_SCALE"] = str(value_size)
    if size_profiling:
        extra_env["COCKROACH_RAFT_MSG_SIZE_PROFILING"] = "true"
    if rscoding_timing:
        extra_env["COCKROACH_RAFT_RSCODING_TIMING"] = "true"
    if try_force_leader:
        extra_env["COCKROACH_RAFT_TRY_FORCE_LEADER"] = "true"
    if protocol == "Crossword":
        extra_env["COCKROACH_RAFT_CW_NUM_VOTERS"] = str(fixed_num_voters)
        extra_env["COCKROACH_RAFT_ENABLE_CROSSWORD"] = "true"
        extra_env["COCKROACH_RAFT_CW_MIN_RANGE_ID"] = str(min_range_id)
        extra_env["COCKROACH_RAFT_CW_MIN_PAYLOAD"] = str(min_payload)
    elif protocol == "CRaft":
        extra_env["COCKROACH_RAFT_CW_NUM_VOTERS"] = str(fixed_num_voters)
        extra_env["COCKROACH_RAFT_ENABLE_CROSSWORD"] = "true"
        extra_env["COCKROACH_RAFT_CW_MIN_RANGE_ID"] = str(min_range_id)
        extra_env["COCKROACH_RAFT_CW_MIN_PAYLOAD"] = str(0)
    elif protocol != "Raft":
        raise ValueError(f"invalid protocol name: {protocol}")

    server_procs = []
    for replica in range(cluster_size):
        host = hosts[replica]

        cmd = compose_server_cmd(
            protocol,
            ipaddrs[host],
            SERVER_SQL_PORT,
            SERVER_LISTEN_PORT,
            SERVER_HTTP_PORT,
            join_list,
            replica,
            remotes[host],
            states_prefix,
            states_midfix,
            fresh_files,
        )

        proc = None
        if host == me:
            # run my responsible server locally
            proc = run_process_pinned(
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
                cd_dir=cd_dir,
                extra_env=extra_env,
            )
        else:
            # spawn server process on remote server through ssh
            proc = run_process_pinned(
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
                remote=remotes[host],
                cd_dir=cd_dir,
                extra_env=extra_env,
            )
        server_procs.append(proc)

    return server_procs


def wait_servers_setup():
    # print("Waiting for servers setup...")
    # wait for 25 seconds to safely allow all nodes up
    # not relying on SSH-piped outputs here
    time.sleep(25)


def compose_init_cmd(init_listen_addr):
    cmd = [
        "./cockroach",
        "init",
        "--insecure",
        f"--host={init_listen_addr}",
    ]
    return cmd


def do_init_action(ipaddrs, hosts, cd_dir):
    init_ip = ipaddrs[hosts[0]]
    init_listen_addr = f"{init_ip}:{SERVER_LISTEN_PORT}"

    cmd = compose_init_cmd(init_listen_addr)

    proc = run_process_pinned(cmd, capture_stderr=False, cd_dir=cd_dir)
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"failed to init CockroachDB cluster: rc {rc}")


def wait_init_finish():
    # print("Waiting for init finish...")
    # wait for 20 seconds to safely allow init to fully finish
    # not relying on SSH-piped outputs here
    time.sleep(20)


def compose_setting_cmds(init_sql_addr, try_force_leader):
    settings = [
        # "SET CLUSTER SETTING kv.transaction.write_pipelining.enabled=false;",
        "SET CLUSTER SETTING admission.kv.enabled=false;",
        "SET CLUSTER SETTING admission.sql_kv_response.enabled=false;",
        "SET CLUSTER SETTING admission.sql_sql_response.enabled=false;",
        "SET CLUSTER SETTING server.consistency_check.interval='0';",
    ]
    if try_force_leader:
        settings += [
            "SET CLUSTER SETTING kv.allocator.lease_rebalance_threshold=1.0;",
            "SET CLUSTER SETTING kv.allocator.load_based_lease_rebalancing.enabled=false;",
            "SET CLUSTER SETTING kv.allocator.load_based_rebalancing=0;",
        ]

    cmds = []
    for setting in settings:
        cmds.append(
            [
                "./cockroach",
                "sql",
                "--insecure",
                f"--host={init_sql_addr}",
                f"--execute={setting}",
            ]
        )
    return cmds


def compose_alter_cmd(init_sql_addr, num_replicas, try_force_leader):
    alter_sql = (
        f"ALTER RANGE default CONFIGURE ZONE USING num_replicas={num_replicas}"
    )
    if try_force_leader:
        alter_sql += ",lease_preferences ='[[+node=n0]]'"
    alter_sql += ";"
    cmd = [
        "./cockroach",
        "sql",
        "--insecure",
        f"--host={init_sql_addr}",
        f"--execute={alter_sql}",
    ]
    return cmd


def set_proper_settings(ipaddrs, hosts, cd_dir, num_replicas, try_force_leader):
    init_ip = ipaddrs[hosts[0]]
    init_sql_addr = f"{init_ip}:{SERVER_SQL_PORT}"

    for cmd in compose_setting_cmds(init_sql_addr, try_force_leader):
        proc = run_process_pinned(cmd, capture_stderr=False, cd_dir=cd_dir)
        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(
                f"failed to set proper cluster settings: rc {rc}"
            )

    cmd = compose_alter_cmd(init_sql_addr, num_replicas, try_force_leader)
    proc = run_process_pinned(cmd, capture_stderr=False, cd_dir=cd_dir)
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(
            f"failed to set num_replicas to {num_replicas}: rc {rc}"
        )


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-c",
        "--cluster_size",
        type=int,
        required=True,
        help="total number of nodes",
    )
    parser.add_argument(
        "-n",
        "--num_replicas",
        type=int,
        required=True,
        help="number of replicas",
    )
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    parser.add_argument(
        "--me",
        type=str,
        default="host0",
        help="main script runner's host nickname",
    )
    parser.add_argument(
        "-v",
        "--value_size",
        type=int,
        default=256,
        help="payload size scale (should to be in sync with workloads client)",
    )
    parser.add_argument(
        "--states_prefix",
        type=str,
        default="/tmp/cockroach",
        help="states file prefix folder path",
    )
    parser.add_argument(
        "--states_midfix",
        type=str,
        default="",
        help="states file extra identifier after protocol name",
    )
    parser.add_argument(
        "--keep_files",
        action="store_true",
        help="if set, keep any old durable files",
    )
    parser.add_argument(
        "--pin_cores",
        type=int,
        default=0,
        help="if > 0, set CPU cores affinity",
    )
    parser.add_argument(
        "--size_profiling",
        action="store_true",
        help="if set, turn on Raft msg size profiling",
    )
    parser.add_argument(
        "--rscoding_timing",
        action="store_true",
        help="if set, turn on RS coding timing logging",
    )
    parser.add_argument(
        "--no_force_leader",
        action="store_true",
        help="if set, don't desire leader-leaseholder colocation and don't prevent transfers",
    )
    parser.add_argument(
        "--min_range_id",
        type=int,
        default=70,
        help="when using Crossword, minimum range ID to enable on (to avoid system db ranges)",
    )
    parser.add_argument(
        "--min_payload",
        type=int,
        default=4096,
        help="when using Crossword, minimum payload size in bytes to enable on",
    )
    parser.add_argument(
        "--fixed_num_voters",
        type=int,
        default=5,
        help="when using Crossword, fixed voters cardinality as a predicate to enable on",
    )
    args = parser.parse_args()

    # parse hosts config file
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir_summerset = f"{base}/{repo}"
    cd_dir_cockroach = f"{base}/{COCK_REPO_NAME}"

    # check that cluster size and number of replicas are valid
    if args.cluster_size <= 0:
        raise ValueError(f"invalid cluster size {args.cluster_size}")
    if args.cluster_size > len(remotes):
        raise ValueError(f"#nodes {args.cluster_size} > #hosts in config file")
    if args.num_replicas <= 0:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")
    if args.num_replicas > len(remotes):
        raise ValueError(
            f"#replicas {args.num_replicas} > #hosts in config file"
        )
    if args.num_replicas > args.cluster_size:
        raise ValueError(
            f"#replicas {args.num_replicas} > cluster size {args.cluster_size}"
        )
    hosts = hosts[: args.cluster_size]
    remotes = {h: remotes[h] for h in hosts}
    ipaddrs = {h: ipaddrs[h] for h in hosts}

    # check protocol name
    if args.protocol not in PROTOCOLS:
        raise ValueError(f"unrecognized protocol name '{args.protocol}'")
    if args.protocol == "Crossword":
        args.no_force_leader = False  # current implementation assumes this

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # kill all existing server processes
    print("Killing related processes...")
    kill_procs = []
    for host in hosts:
        kill_procs.append(
            utils.proc.run_process_over_ssh(
                remotes[host],
                ["./scripts/crossword/kill_cockroach_procs.sh"],
                cd_dir=cd_dir_summerset,
                print_cmd=False,
            )
        )
    utils.proc.wait_parallel_procs(kill_procs, names=hosts)

    # check that the prefix folder path exists, or create it if not
    print("Preparing states folder...")
    prepare_procs = []
    for host in hosts:
        prepare_procs.append(
            utils.proc.run_process_over_ssh(
                remotes[host],
                ["mkdir", "-p", args.states_prefix],
                cd_dir=cd_dir_cockroach,
                print_cmd=False,
            )
        )
    utils.proc.wait_parallel_procs(prepare_procs, names=hosts)

    # msg size profiling assumes '/tmp/cockroach/size-profiles/' exists
    if args.size_profiling:
        if "/tmp/cockroach" not in args.states_prefix:
            raise ValueError(
                "msg size profiling requires '/tmp/cockroach' in `states_prefix`"
            )
        print("Preparing size-profiles folder...")
        prepare_procs.clear()
        for host in hosts:
            prepare_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["rm", "-rf", "/tmp/cockroach/size-profiles"],
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(prepare_procs, names=hosts)
        prepare_procs.clear()
        for host in hosts:
            prepare_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["mkdir", "-p", "/tmp/cockroach/size-profiles"],
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(prepare_procs, names=hosts)

    # launch server replicas
    print("Launching server processes...")
    server_procs = launch_servers(
        remotes,
        ipaddrs,
        hosts,
        args.me,
        cd_dir_cockroach,
        args.protocol,
        args.cluster_size,
        args.value_size,
        args.states_prefix,
        args.states_midfix,
        not args.keep_files,
        args.pin_cores,
        args.size_profiling,
        args.rscoding_timing,
        not args.no_force_leader,
        args.min_range_id,
        args.min_payload,
        args.fixed_num_voters,
    )

    # register termination signals handler
    def kill_spawned_procs(*args):
        print("Killing related processes...")
        kill_procs = []
        for host in hosts:
            kill_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["./scripts/crossword/kill_cockroach_procs.sh"],
                    cd_dir=cd_dir_summerset,
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(kill_procs, names=hosts)

        for proc in server_procs:
            proc.terminate()

    signal.signal(signal.SIGINT, kill_spawned_procs)
    signal.signal(signal.SIGTERM, kill_spawned_procs)
    signal.signal(signal.SIGHUP, kill_spawned_procs)

    # do the cockroach init action
    wait_servers_setup()
    do_init_action(ipaddrs, hosts, cd_dir_cockroach)

    # set default replication factor & other cluster settings
    wait_init_finish()
    set_proper_settings(
        ipaddrs,
        hosts,
        cd_dir_cockroach,
        args.num_replicas,
        not args.no_force_leader,
    )

    for proc in server_procs:
        proc.wait()
