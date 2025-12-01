import sys
import os
import signal
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


ETCD_REPO_NAME = "etcd"


SERVER_LISTEN_PORT = 21380
SERVER_CLIENT_PORT = 21379


PROTOCOL_STORE_PATH = (
    lambda protocol, prefix, midfix, r: f"{prefix}/{protocol}{midfix}.{r}"
)

PROTOCOLS = {"Raft"}  # not accepting other strings yet...


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
    listen_port,
    client_port,
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

    # make states directory
    utils.proc.run_process_over_ssh(
        remote,
        ["mkdir", "-p", f"{backer_dir}"],
        print_cmd=False,
    ).wait()

    cmd = [
        "./bin/etcd",
        "--name",
        f"infra{replica_id}",
        "--initial-advertise-peer-urls",
        f"http://{this_ip}:{listen_port}",
        "--listen-peer-urls",
        f"http://0.0.0.0:{listen_port}",
        "--advertise-client-urls",
        f"http://{this_ip}:{client_port}",
        "--listen-client-urls",
        f"http://0.0.0.0:{client_port}",
        "--initial-cluster",
        f"{','.join([f'infra{r}={addr}' for r, addr in enumerate(join_list)])}",
        "--initial-cluster-state",
        "new",
        "--snapshot-count",
        "10000000",  # don't trigger during bench
        "--heartbeat-interval",
        "120",
        "--data-dir",
        f"{backer_dir}/data",
        "--wal-dir",
        f"{backer_dir}/wal",
        "--log-level",
        "warn",
        "--log-outputs",
        f"stderr,{backer_dir}/etcd.log",
        "--experimental-warning-apply-duration",
        "300ms",
        "--unsafe-no-fsync",  # to accurately compare with ZK on crappy disks
    ]
    return cmd


def launch_servers(
    remotes,
    ipaddrs,
    hosts,
    me,
    cd_dir,
    protocol,
    num_replicas,
    states_prefix,
    states_midfix,
    fresh_files,
    pin_cores,
):
    if num_replicas != len(remotes):
        raise ValueError(f"invalid num_replicas: {num_replicas}")

    join_list = [
        f"http://{ipaddrs[hosts[r]]}:{SERVER_LISTEN_PORT}"
        for r in range(num_replicas)
    ]

    server_procs = []
    for replica in range(num_replicas):
        host = hosts[replica]

        cmd = compose_server_cmd(
            protocol,
            ipaddrs[host],
            SERVER_LISTEN_PORT,
            SERVER_CLIENT_PORT,
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

    return server_procs


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p",
        "--protocol",
        type=str,
        default="Raft",
        help="protocol name (unused yet)",
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
        "--states_prefix",
        type=str,
        default="/tmp/etcd",
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
    args = parser.parse_args()

    # parse hosts config file
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir_summerset = f"{base}/{repo}"
    cd_dir_etcd = f"{base}/{ETCD_REPO_NAME}"

    # check that cluster size and number of replicas are valid
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
                ["./scripts/bodega/kill_etcd_procs.sh"],
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
                cd_dir=cd_dir_etcd,
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
        cd_dir_etcd,
        args.protocol,
        args.num_replicas,
        args.states_prefix,
        args.states_midfix,
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
                    ["./scripts/bodega/kill_etcd_procs.sh"],
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
