import os
import argparse
import subprocess
from pathlib import Path


def run_process(cmd):
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd)
    return proc


PROTOCOL_CONFIGS = {
    "RepNothing": lambda r, _: f"backer_path='/tmp/summerset.rep_nothing.{r}.wal'",
    "SimplePush": lambda r, n: f"backer_path='/tmp/summerset.simple_push.{r}.wal'+rep_degree={n-1}",
    "MultiPaxos": lambda r, _: f"backer_path='/tmp/summerset.multipaxos.{r}.wal'",
}


def launch_replica(
    protocol, api_port, base_conn_port, replica_id, replica_list, config, release
):
    cmd = [
        "cargo",
        "run",
        "-p",
        "summerset_server",
    ]
    if release:
        cmd.append("-r")

    cmd += [
        "--",
        "-p",
        protocol,
        "-a",
        str(api_port),
        "-b",
        str(base_conn_port),
        "-i",
        str(replica_id),
    ]
    cmd += replica_list
    cmd += ["--config", config]

    return run_process(cmd)


def launch_servers(protocol, num_replicas, release):
    api_ports = list(range(52700, 52700 + num_replicas * 10, 10))
    base_conn_ports = list(range(52800, 52800 + num_replicas * 10, 10))
    replica_lists = [[] for _ in range(num_replicas)]
    for replica in range(num_replicas):
        for peer in range(num_replicas):
            replica_lists[replica] += [
                "-r",
                f"127.0.0.1:{base_conn_ports[peer] + replica}",
            ]

    server_procs = []
    for replica in range(num_replicas):
        proc = launch_replica(
            protocol,
            api_ports[replica],
            base_conn_ports[replica],
            replica,
            replica_lists[replica],
            PROTOCOL_CONFIGS[protocol](replica, num_replicas),
            release,
        )
        server_procs.append(proc)

    return server_procs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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

    # kill all existing server processes
    os.system("pkill summerset_server")

    # remove all existing wal files
    for path in Path("/tmp").glob("summerset.*.wal"):
        path.unlink()

    server_procs = launch_servers(args.protocol, args.num_replicas, args.release)

    for proc in server_procs:
        proc.wait()
