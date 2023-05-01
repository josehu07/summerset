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
}


def launch_servers(protocol, num_replicas):
    api_ports = list(range(52700, 52700 + num_replicas))
    smr_ports = list(range(52800, 52800 + num_replicas))
    replica_list = []
    for replica in range(num_replicas):
        replica_list += ["-r", f"127.0.0.1:{smr_ports[replica]}"]

    server_procs = []
    for replica in range(num_replicas):
        api_port = api_ports[replica]
        smr_port = smr_ports[replica]

        cmd = [
            "cargo",
            "run",
            "-p",
            "summerset_server",
            "--",
            "-p",
            protocol,
            "-a",
            str(api_port),
            "-s",
            str(smr_port),
            "-i",
            str(replica),
        ]
        cmd += replica_list
        cmd += ["--config", PROTOCOL_CONFIGS[protocol](replica, num_replicas)]

        server_procs.append(run_process(cmd))

    return server_procs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-r", "--num_replicas", type=int, required=True, help="number of replicas"
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

    server_procs = launch_servers(args.protocol, args.num_replicas)

    for proc in server_procs:
        proc.wait()
