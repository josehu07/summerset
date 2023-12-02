import sys
import os
import signal
import argparse
import multiprocessing

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"


SERVER_LOOP_IP = "0.0.0.0"
SERVER_API_PORT = lambda r: 52700 + r
SERVER_P2P_PORT = lambda r: 52800 + r
SERVER_BIND_BASE_PORT = lambda r: 50000 + r * 100

MANAGER_LOOP_IP = "0.0.0.0"
MANAGER_SRV_PORT = 52600
MANAGER_CLI_PORT = 52601


PROTOCOL_BACKER_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}.wal"
)
PROTOCOL_SNAPSHOT_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}.snap"
)

PROTOCOL_MAY_SNAPSHOT = {
    "RepNothing": False,
    "SimplePush": False,
    "MultiPaxos": True,
    "Raft": True,
    "RSPaxos": True,
    "CRaft": True,
    "Crossword": True,
}

PROTOCOL_EXTRA_DEFAULTS = {
    "RSPaxos": lambda n, _: f"fault_tolerance={(n//2)//2}",
    "CRaft": lambda n, _: f"fault_tolerance={(n//2)//2}",
    "Crossword": lambda n, _: f"fault_tolerance={n//2}",
}


def run_process_pinned(
    i, cmd, capture_stderr=False, cores_per_proc=0, remote=None, cd_dir=None
):
    cpu_list = None
    if cores_per_proc > 0:
        # pin servers from CPU 0 up
        num_cpus = multiprocessing.cpu_count()
        core_start = i * cores_per_proc
        core_end = core_start + cores_per_proc - 1
        assert core_end <= num_cpus - 1
        cpu_list = f"{core_start}-{core_end}"
    if remote is None or len(remote) == 0:
        return utils.run_process(cmd, capture_stderr=capture_stderr, cpu_list=cpu_list)
    else:
        return utils.run_process_over_ssh(
            remote, cmd, capture_stderr=capture_stderr, cd_dir=cd_dir, cpu_list=cpu_list
        )


def config_with_defaults(
    protocol,
    config,
    num_replicas,
    replica_id,
    hb_timer_off,
    file_prefix,
    file_midfix,
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

    backer_path = PROTOCOL_BACKER_PATH(protocol, file_prefix, file_midfix, replica_id)
    config_dict = {"backer_path": f"'{backer_path}'"}
    if fresh_files and os.path.isfile(backer_path):
        print(f"Delete: {backer_path}")
        os.remove(backer_path)

    if PROTOCOL_MAY_SNAPSHOT[protocol]:
        snapshot_path = PROTOCOL_SNAPSHOT_PATH(
            protocol, file_prefix, file_midfix, replica_id
        )
        config_dict["snapshot_path"] = f"'{snapshot_path}'"
        if fresh_files and os.path.isfile(snapshot_path):
            print(f"Delete: {snapshot_path}")
            os.remove(snapshot_path)

    if protocol in PROTOCOL_EXTRA_DEFAULTS:
        config_dict.update(
            config_str_to_dict(
                PROTOCOL_EXTRA_DEFAULTS[protocol](num_replicas, replica_id)
            )
        )

    if config is not None and len(config) > 0:
        config_dict.update(config_str_to_dict(config))

    if hb_timer_off:
        config_dict["disable_hb_timer"] = "true"

    return config_dict_to_str(config_dict)


def compose_manager_cmd(protocol, bind_ip, srv_port, cli_port, num_replicas, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_manager"]
    cmd += [
        "-p",
        protocol,
        "-b",
        bind_ip,
        "-s",
        str(srv_port),
        "-c",
        str(cli_port),
        "-n",
        str(num_replicas),
    ]
    return cmd


def launch_manager(protocol, num_replicas, release, pin_cores):
    bind_ip = MANAGER_LOOP_IP

    cmd = compose_manager_cmd(
        protocol,
        bind_ip,
        MANAGER_SRV_PORT,
        MANAGER_CLI_PORT,
        num_replicas,
        release,
    )
    return run_process_pinned(0, cmd, capture_stderr=True, cores_per_proc=pin_cores)


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


def compose_server_cmd(
    protocol, bind_base, api_port, p2p_port, manager, config, release
):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_server"]
    cmd += [
        "-p",
        protocol,
        "-b",
        bind_base,
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
    num_replicas,
    release,
    config,
    file_prefix,
    file_midfix,
    fresh_files,
    pin_cores,
):
    if num_replicas != len(remotes):
        raise ValueError(f"invalid num_replicas: {num_replicas}")

    # assuming I am the machine to run manager
    manager_pub_ip = ipaddrs[me]

    server_procs = []
    for replica in range(num_replicas):
        host = hosts[replica]

        bind_base = f"{SERVER_LOOP_IP}:{SERVER_BIND_BASE_PORT(replica)}"
        manager_addr = f"{manager_pub_ip}:{MANAGER_SRV_PORT}"

        cmd = compose_server_cmd(
            protocol,
            bind_base,
            SERVER_API_PORT(replica),
            SERVER_P2P_PORT(replica),
            manager_addr,
            config_with_defaults(
                protocol,
                config,
                num_replicas,
                replica,
                host != me,
                file_prefix,
                file_midfix,
                fresh_files,
            ),
            release,
        )
        if host == me:
            # run my responsible server locally
            proc = run_process_pinned(
                replica + 1,
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
            )
        else:
            # spawn server process on remote server through ssh
            proc = run_process_pinned(
                replica + 1,
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
                remote=remotes[host],
                cd_dir=cd_dir,
            )

        server_procs.append(proc)

    return server_procs


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
    )
    parser.add_argument(
        "-r", "--release", action="store_true", help="if set, run release mode"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )
    parser.add_argument(
        "--me", type=str, default="host0", help="main script runner's host nickname"
    )
    parser.add_argument(
        "--base_dir",
        type=str,
        default="/mnt/eval",
        help="base cd dir after ssh",
    )
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="/tmp/summerset",
        help="states file prefix folder path",
    )
    parser.add_argument(
        "--file_midfix",
        type=str,
        default="",
        help="states file extra identifier after protocol name",
    )
    parser.add_argument(
        "--keep_files", action="store_true", help="if set, keep any old durable files"
    )
    parser.add_argument(
        "--pin_cores", type=int, default=0, help="if > 0, set CPU cores affinity"
    )
    args = parser.parse_args()

    # parse hosts config file
    hosts_config = utils.read_toml_file(TOML_FILENAME)
    remotes = hosts_config["hosts"]
    hosts = sorted(list(remotes.keys()))
    domains = {
        name: utils.split_remote_string(remote)[1] for name, remote in remotes.items()
    }
    ipaddrs = {name: utils.lookup_dns_to_ip(domain) for name, domain in domains.items()}

    repo = hosts_config["repo_name"]
    cd_dir = f"{args.base_dir}/{repo}"

    # check that number of replicas equals 3
    if args.num_replicas != len(remotes):
        print("ERROR: #replicas does not match #hosts in config file")
        sys.exit(1)

    # check that I am indeed the "me" host
    utils.check_remote_is_me(remotes[args.me])

    # kill all existing server and manager processes
    print("Killing related processes...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.run_process_over_ssh(
            remote,
            ["./scripts/kill_local_procs.sh"],
            cd_dir=cd_dir,
            print_cmd=False,
        ).wait()

    # check that the prefix folder path exists, or create it if not
    print("Preparing states folder...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.run_process_over_ssh(
            remote,
            ["mkdir", "-p", args.file_prefix],
            cd_dir=cd_dir,
            print_cmd=False,
        ).wait()

    # build everything
    print("Building everything...")
    cargo_cmd = ["source", "$HOME/.cargo/env;", "cargo", "build", "--workspace"]
    if args.release:
        cargo_cmd.append("-r")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.run_process_over_ssh(
            remote,
            cargo_cmd,
            cd_dir=cd_dir,
            print_cmd=False,
        ).wait()

    # launch cluster manager oracle first
    manager_proc = launch_manager(
        args.protocol, args.num_replicas, args.release, args.pin_cores
    )
    wait_manager_setup(manager_proc)

    # create a thread that prints out captured manager outputs
    # def print_manager_stderr():
    #     for line in iter(manager_proc.stderr.readline, b""):
    #         l = line.decode()
    #         print(l, end="", file=sys.stderr)

    # print_manager_t = threading.Thread(target=print_manager_stderr)
    # print_manager_t.start()

    # then launch server replicas
    server_procs = launch_servers(
        remotes,
        ipaddrs,
        hosts,
        args.me,
        cd_dir,
        args.protocol,
        args.num_replicas,
        args.release,
        args.config,
        args.file_prefix,
        args.file_midfix,
        not args.keep_files,
        args.pin_cores,
    )

    # register termination signals handler
    def kill_spawned_procs(*args):
        print("Killing related processes...")
        for host, remote in remotes.items():
            print(f"  {host}")
            utils.run_process_over_ssh(
                remote,
                ["./scripts/kill_local_procs.sh"],
                cd_dir=cd_dir,
                print_cmd=False,
            ).wait()

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
