import sys
import os
import argparse
import subprocess
import time
from pathlib import Path


def do_cargo_build(release):
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd):
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd)
    return proc


def kill_all_matching(name):
    print("Kill all:", name)
    assert name.count(" ") == 0
    os.system(f"sudo pkill -9 -f {name}")


MANAGER_SRV_PORT = 52600
MANAGER_CLI_PORT = 52601

SERVER_API_PORT = lambda r: 52700 + r
SERVER_P2P_PORT = lambda r: 52800 + r


PROTOCOL_CONFIGS = {
    "RepNothing": lambda r, n: f"backer_path='/tmp/summerset.rep_nothing.{r}.wal'",
    "SimplePush": lambda r, n: f"backer_path='/tmp/summerset.simple_push.{r}.wal'+rep_degree={n-1}",
    "MultiPaxos": lambda r, n: f"backer_path='/tmp/summerset.multipaxos.{r}.wal'",
    "RSPaxos": lambda r, n: f"backer_path='/tmp/summerset.rs_paxos.{r}.wal'+fault_tolerance={n-(n//2+1)}",
}


def compose_manager_cmd(protocol, srv_port, cli_port, num_replicas, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_manager"]
    cmd += [
        "-p",
        protocol,
        "-s",
        str(srv_port),
        "-c",
        str(cli_port),
        "-n",
        str(num_replicas),
    ]
    return cmd


def launch_manager(protocol, num_replicas, release):
    cmd = compose_manager_cmd(
        protocol,
        MANAGER_SRV_PORT,
        MANAGER_CLI_PORT,
        num_replicas,
        release,
    )
    return run_process(cmd)


def compose_server_cmd(protocol, api_port, p2p_port, manager, config, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_server"]
    cmd += [
        "-p",
        protocol,
        "-a",
        str(api_port),
        "-i",
        str(p2p_port),
        "-m",
        manager,
    ]
    if len(config) > 0:
        cmd += ["--config", config]
    return cmd


def launch_servers(protocol, num_replicas, release):
    server_procs = []
    for replica in range(num_replicas):
        cmd = compose_server_cmd(
            protocol,
            SERVER_API_PORT(replica),
            SERVER_P2P_PORT(replica),
            f"127.0.0.1:{MANAGER_SRV_PORT}",
            PROTOCOL_CONFIGS[protocol](replica, num_replicas),
            release,
        )
        proc = run_process(cmd)
        server_procs.append(proc)

    return server_procs


if __name__ == "__main__":
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
    args = parser.parse_args()

    if args.protocol not in PROTOCOL_CONFIGS:
        raise ValueError(f"unknown protocol name '{args.protocol}'")
    if args.num_replicas <= 0 or args.num_replicas > 9:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")

    # kill all existing server and manager processes
    kill_all_matching("summerset_server")
    kill_all_matching("summerset_manager")

    # remove all existing wal files
    for path in Path("/tmp").glob("summerset.*.wal"):
        path.unlink()

    # build everything
    do_cargo_build(args.release)

    # launch cluster manager oracle first
    manager_proc = launch_manager(args.protocol, args.num_replicas, args.release)
    time.sleep(1)

    # then launch server replicas
    launch_servers(args.protocol, args.num_replicas, args.release)

    rc = manager_proc.wait()
    sys.exit(rc)
