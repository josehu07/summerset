import sys
import os
import time
import signal
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


SERVER_LOOP_IP = "0.0.0.0"
SERVER_API_PORT = lambda p, r: 40000 + (p * 20 + r)
SERVER_P2P_PORT = lambda p, r: 40010 + (p * 20 + r)

MANAGER_LOOP_IP = "0.0.0.0"
MANAGER_CLI_PORT = lambda p: 40009 + p * 20  # NOTE: assuming at most 9 servers
MANAGER_SRV_PORT = lambda p: 40019 + p * 20


PROTOCOL_BACKER_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}.wal"
)
PROTOCOL_SNAPSHOT_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}.snap"
)


class ProtoFeats:
    def __init__(self, could_snapshot, has_leadership, extra_defaults):
        self.could_snapshot = could_snapshot
        self.has_leadership = has_leadership
        self.extra_defaults = extra_defaults


PROTOCOL_FEATURES = {
    "RepNothing": ProtoFeats(False, False, None),
    "SimplePush": ProtoFeats(False, False, None),
    "ChainRep": ProtoFeats(False, False, None),
    "MultiPaxos": ProtoFeats(True, True, None),
    "EPaxos": ProtoFeats(True, False, lambda n, _: "optimized_quorum=true"),
    "RSPaxos": ProtoFeats(
        True, True, lambda n, _: f"fault_tolerance={(n // 2) // 2}"
    ),
    "Raft": ProtoFeats(True, True, None),
    "CRaft": ProtoFeats(
        True, True, lambda n, _: f"fault_tolerance={(n // 2) // 2}"
    ),
    "Crossword": ProtoFeats(
        True, True, lambda n, _: f"fault_tolerance={n // 2}"
    ),
    "QuorumLeases": ProtoFeats(True, True, lambda n, _: "sim_read_lease=false"),
    "Bodega": ProtoFeats(True, True, lambda n, _: "sim_read_lease=false"),
}


def run_process_pinned(
    cmd, capture_stderr=False, cores_per_proc=0, remote=None, cd_dir=None
):
    cpu_list = None
    if cores_per_proc > 0:
        # get number of processors
        num_cpus = utils.proc.get_cpu_count(remote=remote)
        # pin servers at CPUs [0, cores_per_proc); not pinning manager
        core_start = 0
        core_end = core_start + cores_per_proc - 1
        assert core_end <= num_cpus - 1
        cpu_list = f"{core_start}-{core_end}"
    if remote is None or len(remote) == 0:
        return utils.proc.run_process(
            cmd, capture_stderr=capture_stderr, cd_dir=cd_dir, cpu_list=cpu_list
        )
    else:
        return utils.proc.run_process_over_ssh(
            remote,
            cmd,
            capture_stderr=capture_stderr,
            cd_dir=cd_dir,
            cpu_list=cpu_list,
        )


def config_with_defaults(
    protocol,
    config,
    num_replicas,
    replica_id,
    remote,
    no_step_up,
    states_prefix,
    states_midfix,
    fresh_files,
):
    def config_str_to_dict(s):
        l = s.strip().split("+")
        l = [c.strip().split("=") for c in l]
        for c in l:
            assert len(c) == 2
        return {c[0]: c[1] for c in l}

    def config_dict_to_str(d):
        l = ["=".join([k, v]) for k, v in d.items()]
        return "+".join(l)

    backer_path = PROTOCOL_BACKER_PATH(
        protocol, states_prefix, states_midfix, replica_id
    )
    config_dict = {"backer_path": f"'{backer_path}'"}
    if fresh_files:
        utils.proc.run_process_over_ssh(
            remote,
            ["sudo", "rm", "-f", backer_path],
            print_cmd=False,
        ).wait()

    if PROTOCOL_FEATURES[protocol].could_snapshot:
        snapshot_path = PROTOCOL_SNAPSHOT_PATH(
            protocol, states_prefix, states_midfix, replica_id
        )
        config_dict["snapshot_path"] = f"'{snapshot_path}'"
        if fresh_files:
            utils.proc.run_process_over_ssh(
                remote,
                ["sudo", "rm", "-f", snapshot_path],
                print_cmd=False,
            ).wait()

    if PROTOCOL_FEATURES[protocol].extra_defaults is not None:
        config_dict.update(
            config_str_to_dict(
                PROTOCOL_FEATURES[protocol].extra_defaults(
                    num_replicas, replica_id
                )
            )
        )

    if config is not None and len(config) > 0:
        config_dict.update(config_str_to_dict(config))

    if PROTOCOL_FEATURES[protocol].has_leadership and no_step_up:
        config_dict["disallow_step_up"] = "true"

    return config_dict_to_str(config_dict)


def compose_manager_cmd(protocol, srv_port, cli_port, num_replicas, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_manager"]
    cmd += [
        "-p",
        protocol,
        "-b",
        MANAGER_LOOP_IP,
        "-c",
        str(cli_port),
        "-s",
        str(srv_port),
        "-n",
        str(num_replicas),
    ]
    return cmd


def launch_manager(protocol, partition, num_replicas, release):
    cmd = compose_manager_cmd(
        protocol,
        MANAGER_SRV_PORT(partition),
        MANAGER_CLI_PORT(partition),
        num_replicas,
        release,
    )
    return run_process_pinned(cmd, capture_stderr=True)


def wait_manager_setup(proc):
    # print("Waiting for manager setup...")
    accepting_servers, accepting_clients = False, False

    for line in iter(proc.stderr.readline, b""):
        l = line.decode()
        print(l, end="", file=sys.stderr)

        if "(m) accepting servers" in l:
            assert not accepting_servers
            accepting_servers = True
        if "(m) accepting clients" in l:
            assert not accepting_clients
            accepting_clients = True

        if accepting_servers and accepting_clients:
            break


def compose_server_cmd(protocol, api_port, p2p_port, manager, config, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_server"]
    cmd += [
        "-p",
        protocol,
        "-b",
        SERVER_LOOP_IP,
        "-a",
        str(api_port),
        "-i",
        str(p2p_port),
        "-m",
        manager,
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return cmd


def launch_servers(
    remotes,
    ipaddrs,
    hosts,
    me,
    cd_dir,
    protocol,
    partition,
    num_replicas,
    release,
    config,
    force_leader,
    states_prefix,
    states_midfix,
    fresh_files,
    pin_cores,
    launch_wait,
):
    if num_replicas != len(remotes):
        raise ValueError(f"invalid num_replicas: {num_replicas}")

    # assuming I am the machine to run manager
    manager_pub_ip = ipaddrs[me]

    server_procs = []
    for replica in range(num_replicas):
        host = hosts[replica]
        manager_addr = f"{manager_pub_ip}:{MANAGER_SRV_PORT(partition)}"

        cmd = compose_server_cmd(
            protocol,
            SERVER_API_PORT(partition, replica),
            SERVER_P2P_PORT(partition, replica),
            manager_addr,
            config_with_defaults(
                protocol,
                config,
                num_replicas,
                replica,
                remotes[host],
                force_leader >= 0 and force_leader != replica,
                states_prefix,
                states_midfix,
                fresh_files,
            ),
            release,
        )

        proc = None
        if host == me:
            # run my responsible server locally
            proc = run_process_pinned(
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
            )
        else:
            # spawn server process on remote server through ssh
            proc = run_process_pinned(
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
                remote=remotes[host],
                cd_dir=cd_dir,
            )
        server_procs.append(proc)

        if launch_wait:
            time.sleep(3)  # NOTE: helps enforce server ID assignment

    return server_procs


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-a",
        "--partition",
        type=int,
        default=argparse.SUPPRESS,
        help="if doing keyspace partitioning, the partition idx",
    )
    parser.add_argument(
        "-n",
        "--num_replicas",
        type=int,
        required=True,
        help="number of replicas",
    )
    parser.add_argument(
        "-r", "--release", action="store_true", help="if set, run release mode"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
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
        "--force_leader",
        type=int,
        default=-1,
        help="force this server to be leader",
    )
    parser.add_argument(
        "--states_prefix",
        type=str,
        default="/tmp/summerset",
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
        "--launch_wait",
        action="store_true",
        help="if set, wait 3s between launches",
    )
    parser.add_argument(
        "--skip_build", action="store_true", help="if set, skip cargo build"
    )
    args = parser.parse_args()

    # parse hosts config file
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir = f"{base}/{repo}"

    # check that the partition index is valid
    partition_in_args = "partition" in args
    if partition_in_args and (args.partition < 0 or args.partition >= 5):
        raise ValueError("currently only supports <= 5 partitions")
    partition = 0 if not partition_in_args else args.partition
    states_midfix = (
        args.states_midfix
        if not partition_in_args
        else f"{args.states_midfix}.{partition}"
    )

    # check that number of replicas is valid
    if args.num_replicas <= 0:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")
    if args.num_replicas > len(remotes):
        raise ValueError(
            f"#replicas {args.num_replicas} > #hosts in config file"
        )
    hosts = hosts[: args.num_replicas]
    remotes = {h: remotes[h] for h in hosts}
    ipaddrs = {h: ipaddrs[h] for h in hosts}

    # check protocol name
    if args.protocol not in PROTOCOL_FEATURES:
        raise ValueError(f"unrecognized protocol name '{args.protocol}'")

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # kill all existing server and manager processes
    if not partition_in_args:
        print("Killing related processes...")
        kill_procs = []
        for host in hosts:
            kill_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["./scripts/kill_all_procs.sh"],
                    cd_dir=cd_dir,
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
                cd_dir=cd_dir,
                print_cmd=False,
            )
        )
    utils.proc.wait_parallel_procs(prepare_procs, names=hosts)

    # build everything
    if not partition_in_args and not args.skip_build:
        print("Building everything...")
        utils.file.do_cargo_build(args.release, cd_dir=cd_dir, remotes=remotes)

    # launch cluster manager oracle first
    manager_proc = launch_manager(
        args.protocol, partition, args.num_replicas, args.release
    )
    wait_manager_setup(manager_proc)

    # create a thread that prints out captured manager stderrs
    # def print_manager_stderr():
    #     for line in iter(manager_proc.stderr.readline, b""):
    #         l = line.decode()
    #         print(l, end="", file=sys.stderr)

    # print_manager_t = threading.Thread(target=print_manager_stderr)
    # print_manager_t.start()

    # then launch server replicas
    print("Launching server processes...")
    server_procs = launch_servers(
        remotes,
        ipaddrs,
        hosts,
        args.me,
        cd_dir,
        args.protocol,
        partition,
        args.num_replicas,
        args.release,
        args.config,
        args.force_leader,
        args.states_prefix,
        states_midfix,
        not args.keep_files,
        args.pin_cores,
        args.launch_wait,
    )

    # register termination signals handler
    # NOTE: this also terminates other partitions' processes if doing
    #       keyspace partitioning
    def kill_spawned_procs(*args):
        print("Killing related processes...")
        kill_procs = []
        for host in hosts:
            kill_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["./scripts/kill_all_procs.sh"],
                    cd_dir=cd_dir,
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(kill_procs, names=hosts)

        for proc in server_procs:
            proc.terminate()
        for proc in server_procs:
            proc.wait()
        manager_proc.terminate()

    signal.signal(signal.SIGINT, kill_spawned_procs)
    signal.signal(signal.SIGTERM, kill_spawned_procs)
    signal.signal(signal.SIGHUP, kill_spawned_procs)

    # reaches here after manager proc has terminated
    rc = manager_proc.wait()
    sys.exit(rc)
