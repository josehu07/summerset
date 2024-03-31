import sys
import os
import signal
import argparse
import multiprocessing

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"

CHAIN_REPO_NAME = "chain"
CHAIN_JAR_FOLDER = "deploy/server"


SERVER_CONSENSUS_PORT = 50300
SERVER_FRONTEND_PEER_PORT = 50400
SERVER_APP_PORT = 50500

SERVER_LEADER_TIMEOUT = 5000
SERVER_NOOP_INTERVAL = 100


PROTOCOLS = {"chain_delayed", "chain_mixed", "chainrep", "epaxos"}

PROTOCOL_BACKER_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}.wal"
)


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
        return utils.proc.run_process(
            cmd, capture_stderr=capture_stderr, cd_dir=cd_dir, cpu_list=cpu_list
        )
    else:
        return utils.proc.run_process_over_ssh(
            remote, cmd, capture_stderr=capture_stderr, cd_dir=cd_dir, cpu_list=cpu_list
        )


def compose_server_cmd(
    protocol,
    ipaddrs,
    quorum_size,
    replica_id,
    remote,
    interface,
    file_prefix,
    file_midfix,
    fresh_files,
):
    backer_file = PROTOCOL_BACKER_PATH(protocol, file_prefix, file_midfix, replica_id)
    if fresh_files:
        utils.proc.run_process_over_ssh(
            remote,
            ["rm", "-f", backer_file],
            print_cmd=False,
        ).wait()

    cmd = [
        "java",
        "-Dlog4j.configurationFile=log4j2.xml",
        "-Djava.net.preferIPv4Stack=true",
        f"-DlogFilename={backer_file}",
        "-cp",
        "chain.jar:.",
        "app.HashMapApp",
        f"interface={interface}",
        f"algorithm={protocol}",
        f"initial_membership={','.join(ipaddrs.values())}",
        "initial_state=ACTIVE",
        f"quorum_size={quorum_size}",
        f"consensus_port={SERVER_CONSENSUS_PORT}",
        f"frontend_peer_port={SERVER_FRONTEND_PEER_PORT}",
        f"app_port={SERVER_APP_PORT}",
        f"leader_timeout={SERVER_LEADER_TIMEOUT}",
        f"noop_interval={SERVER_NOOP_INTERVAL}",
    ]
    return cmd


def launch_servers(
    remotes,
    ipaddrs,
    interfaces,
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

    server_procs = []
    for replica in range(num_replicas):
        host = hosts[replica]

        cmd = compose_server_cmd(
            protocol,
            ipaddrs,
            (num_replicas // 2) + 1,
            replica,
            remotes[host],
            interfaces[host],
            file_prefix,
            file_midfix,
            fresh_files,
        )
        if host == me:
            # run my responsible server locally
            proc = run_process_pinned(
                replica + 1,
                cmd,
                capture_stderr=False,
                cores_per_proc=pin_cores,
                cd_dir=cd_dir,
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
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
    )
    parser.add_argument(
        "-g", "--group", type=str, default="1dc", help="hosts group to run on"
    )
    parser.add_argument(
        "--me", type=str, default="host0", help="main script runner's host nickname"
    )
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="/tmp/chain",
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
    cd_dir_chain = f"{base}/{CHAIN_REPO_NAME}/{CHAIN_JAR_FOLDER}"

    # check that number of replicas is valid
    if args.num_replicas > len(remotes):
        raise ValueError("#replicas exceeds #hosts in the config file")
    hosts = hosts[: args.num_replicas]
    remotes = {h: remotes[h] for h in hosts}
    ipaddrs = {h: ipaddrs[h] for h in hosts}

    # check protocol name
    if args.protocol not in PROTOCOLS:
        raise ValueError(f"unrecognized protocol name '{args.protocol}'")

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # kill all existing server and manager processes
    print("Killing related processes...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.proc.run_process_over_ssh(
            remote,
            ["./scripts/crossword/kill_chain_procs.sh"],
            cd_dir=cd_dir_summerset,
            print_cmd=False,
        ).wait()

    # check that the prefix folder path exists, or create it if not
    print("Preparing states folder...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.proc.run_process_over_ssh(
            remote,
            ["mkdir", "-p", args.file_prefix],
            cd_dir=cd_dir_chain,
            print_cmd=False,
        ).wait()

    # get the main Ethernet interface name on each host
    print("Getting main interface name...")
    interfaces = dict()
    for host in hosts:
        print(f"  {host} ", end="")
        interface = utils.net.get_interface_name(
            ipaddrs[host], remote=None if host == args.me else remotes[host]
        )
        print(interface)
        interfaces[host] = interface

    # then launch server replicas
    server_procs = launch_servers(
        remotes,
        ipaddrs,
        interfaces,
        hosts,
        args.me,
        cd_dir_chain,
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
        for host, remote in remotes.items():
            print(f"  {host}")
            utils.proc.run_process_over_ssh(
                remote,
                ["./scripts/crossword/kill_chain_procs.sh"],
                cd_dir=cd_dir_summerset,
                print_cmd=False,
            ).wait()

        for proc in server_procs:
            proc.terminate()

    signal.signal(signal.SIGINT, kill_spawned_procs)
    signal.signal(signal.SIGTERM, kill_spawned_procs)
    signal.signal(signal.SIGHUP, kill_spawned_procs)

    for proc in server_procs:
        proc.wait()
