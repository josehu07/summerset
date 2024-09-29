import sys
import os
import signal
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"

ZK_REPO_NAME = "zookeeper"


SERVER_PEER_PORT = 20888
SERVER_ELECT_PORT = 20988
SERVER_CLI_PORT = 20181


PROTOCOL_STORE_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}"
)

PROTOCOLS = {"ZooKeeper"}  # not accepting other strings yet...


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


def dump_server_configs(
    protocol,
    peer_port,
    elect_port,
    cli_port,
    node_ips,
    replica_id,
    zk_repo,
    file_prefix,
    file_midfix,
    fresh_files,
):
    backer_dir = PROTOCOL_STORE_PATH(protocol, file_prefix, file_midfix, replica_id)
    if fresh_files:
        utils.proc.run_process(
            ["sudo", "rm", "-rf", backer_dir],
            print_cmd=False,
        ).wait()

    # write myid file under dataDir
    utils.proc.run_process(
        ["mkdir", "-p", f"{backer_dir}/data"],
        print_cmd=False,
    ).wait()
    with open(f"{backer_dir}/data/myid", "w") as f:
        f.write(f"{replica_id}")

    # populate conf/zoo.cfg in zookeeper source root
    for cfg_r in range(len(node_ips)):
        configs = [
            f"tickTime=100",
            f"dataDir={PROTOCOL_STORE_PATH(protocol, file_prefix, file_midfix, cfg_r)}/data",
            f"dataLogDir={PROTOCOL_STORE_PATH(protocol, file_prefix, file_midfix, cfg_r)}/dlog",
            f"clientPort={cli_port}",
            f"initLimit=100",
            f"syncLimit=40",
            f"snapCount=100000",
        ]
        for r, ip in enumerate(node_ips):
            configs.append(f"server.{r}={ip}:{peer_port}:{elect_port}")

        cfg_path = f"{zk_repo}/conf/zoo.{cfg_r}.cfg"
        if cfg_r == replica_id:
            cfg_path = f"{zk_repo}/conf/zoo.cfg"
        with open(cfg_path, "w") as fcfg:
            for cfg in configs:
                fcfg.write(cfg + "\n")


def copy_server_config(
    protocol,
    replica_id,
    remote,
    zk_repo,
    file_prefix,
    file_midfix,
    fresh_files,
):
    backer_dir = PROTOCOL_STORE_PATH(protocol, file_prefix, file_midfix, replica_id)
    if fresh_files:
        utils.proc.run_process_over_ssh(
            remote,
            ["sudo", "rm", "-rf", backer_dir],
            print_cmd=False,
        ).wait()

    # write myid file under dataDir over ssh
    utils.proc.run_process_over_ssh(
        remote,
        [
            "mkdir",
            "-p",
            f"{backer_dir}/data",
            ";",
            "echo",
            str(replica_id),
            ">",
            f"{backer_dir}/data/myid",
        ],
        print_cmd=False,
    ).wait()

    # copy conf/zoo.cfg to zookeeper source root over ssh
    utils.file.copy_file_to_remote(
        remote,
        f"{zk_repo}/conf/zoo.{replica_id}.cfg",
        f"{zk_repo}/conf/zoo.cfg",
    )


def launch_servers(
    remotes,
    ipaddrs,
    hosts,
    me,
    cd_dir,
    protocol,
    num_replicas,
    file_prefix,
    file_midfix,
    fresh_files,
    pin_cores,
):
    if num_replicas != len(remotes):
        raise ValueError(f"invalid num_replicas: {num_replicas}")

    node_ips = [ipaddrs[hosts[r]] for r in range(num_replicas)]
    dump_server_configs(
        protocol,
        SERVER_PEER_PORT,
        SERVER_ELECT_PORT,
        SERVER_CLI_PORT,
        node_ips,
        hosts.index(me),
        cd_dir,
        file_prefix,
        file_midfix,
        fresh_files,
    )
    for replica in range(num_replicas):
        host = hosts[replica]
        if host != me:
            copy_server_config(
                protocol,
                replica,
                remotes[host],
                cd_dir,
                file_prefix,
                file_midfix,
                fresh_files,
            )

    server_procs = []
    for replica in range(num_replicas):
        host = hosts[replica]

        extra_env = {
            "ZOO_LOG_DIR": f"{PROTOCOL_STORE_PATH(protocol, file_prefix, file_midfix, replica)}/logs",
        }
        cmd = ["./bin/zkServer.sh", "start-foreground"]

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


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p",
        "--protocol",
        type=str,
        default="ZooKeeper",
        help="protocol name (unused yet)",
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
    )
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    parser.add_argument(
        "--me", type=str, default="host0", help="main script runner's host nickname"
    )
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="/tmp/zookeeper",
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
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        TOML_FILENAME, args.group
    )
    cd_dir_summerset = f"{base}/{repo}"
    cd_dir_zookeeper = f"{base}/{ZK_REPO_NAME}"

    # check that number of replicas is valid
    if args.num_replicas <= 0:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")
    if args.num_replicas > len(remotes):
        raise ValueError(f"#replicas {args.num_replicas} > #hosts in config file")
    hosts = hosts[: args.num_replicas]
    remotes = {h: remotes[h] for h in hosts}
    ipaddrs = {h: ipaddrs[h] for h in hosts}

    # check protocol name
    if args.protocol not in PROTOCOLS:
        raise ValueError(f"unrecognized protocol name '{args.protocol}'")

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # kill all existing server processes
    print("Killing related processes...")
    kill_procs = []
    for host in hosts:
        kill_procs.append(
            utils.proc.run_process_over_ssh(
                remotes[host],
                ["./scripts/bodega/kill_zookeeper_procs.sh"],
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
                ["mkdir", "-p", args.file_prefix],
                cd_dir=cd_dir_zookeeper,
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
        cd_dir_zookeeper,
        args.protocol,
        args.num_replicas,
        args.file_prefix,
        args.file_midfix,
        not args.keep_files,
        args.pin_cores,
    )

    # register termination signals handler
    def kill_spawned_procs(*args):
        print("Killing related processes...")
        kill_procs = []
        for host in hosts:
            kill_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["./scripts/bodega/kill_zookeeper_procs.sh"],
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

    for proc in server_procs:
        proc.wait()
