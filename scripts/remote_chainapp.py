import sys
import os
import signal
import argparse
import multiprocessing

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


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
        return utils.run_process(
            cmd, capture_stderr=capture_stderr, cd_dir=cd_dir, cpu_list=cpu_list
        )
    else:
        return utils.run_process_over_ssh(
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
        utils.run_process_over_ssh(
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
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
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
    hosts_config = utils.read_toml_file(TOML_FILENAME)
    remotes = hosts_config["hosts"]
    hosts = sorted(list(remotes.keys()))
    domains = {
        name: utils.split_remote_string(remote)[1] for name, remote in remotes.items()
    }
    ipaddrs = {name: utils.lookup_dns_to_ip(domain) for name, domain in domains.items()}

    repo = hosts_config["repo_name"]
    cd_dir_summerset = f"{args.base_dir}/{repo}"
    cd_dir_chain = f"{args.base_dir}/{CHAIN_REPO_NAME}/{CHAIN_JAR_FOLDER}"

    interfaces = hosts_config["intfs"]

    # check that number of replicas equals 3
    if args.num_replicas != len(remotes):
        print("ERROR: #replicas does not match #hosts in config file")
        sys.exit(1)

    # check protocol name
    if args.protocol not in PROTOCOLS:
        print(f"ERROR: unrecognized protocol name '{args.protocol}'")

    # check that I am indeed the "me" host
    utils.check_remote_is_me(remotes[args.me])

    # kill all existing server and manager processes
    print("Killing related processes...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.run_process_over_ssh(
            remote,
            ["./scripts/kill_chain_procs.sh"],
            cd_dir=cd_dir_summerset,
            print_cmd=False,
        ).wait()

    # check that the prefix folder path exists, or create it if not
    print("Preparing states folder...")
    for host, remote in remotes.items():
        print(f"  {host}")
        utils.run_process_over_ssh(
            remote,
            ["mkdir", "-p", args.file_prefix],
            cd_dir=cd_dir_chain,
            print_cmd=False,
        ).wait()

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
            utils.run_process_over_ssh(
                remote,
                ["./scripts/kill_chain_procs.sh"],
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
