import argparse
import os
import subprocess
import sys
import threading
from typing import IO

# number of replicas to launch for each protocol; erasure-coded ones need 5
# for a meaningful fault tolerance setting
PROTOCOLS_NUM_REPLICAS = {
    "RepNothing": 3,
    "SimplePush": 3,
    "ChainRep": 3,
    "MultiPaxos": 3,
    "EPaxos": 3,
    "RSPaxos": 5,
    "Raft": 3,
    "CRaft": 5,
    "Crossword": 5,
    "QuorumLeases": 3,
    "Bodega": 3,
}


def do_cargo_build() -> None:
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd: list[str]) -> subprocess.Popen[bytes]:
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def kill_all_matching(name: str, force: bool = False) -> None:
    # print("Kill all:", name)
    assert name.count(" ") == 0
    try:
        pgrep_cmd = f"sudo pgrep -f {name}"
        pids = subprocess.check_output(pgrep_cmd, shell=True).decode()
        pids = pids.strip().split("\n")
        for pid in pids:
            pid = pid.strip()
            if len(pid) > 0:
                kill_cmd = "sudo kill -9" if force else "sudo kill"
                kill_cmd += f" {int(pid)} > /dev/null 2>&1"
                os.system(kill_cmd)
    except subprocess.CalledProcessError:
        pass


def launch_cluster(protocol: str, num_replicas: int, config: str | None) -> subprocess.Popen[bytes]:
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.local_cluster",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return run_process(cmd)


def wait_cluster_setup(proc: subprocess.Popen[bytes], num_replicas: int) -> None:
    accepting_clients = [False for _ in range(num_replicas)]

    for line in iter(proc.stderr.readline, b""):
        l = line.decode()
        print(l, end="", file=sys.stderr)
        if "accepting clients" in l:
            replica = l[l.find("(") + 1 : l.find(")")]
            if replica == "m":
                continue
            replica = int(replica)
            assert not accepting_clients[replica]
            accepting_clients[replica] = True

        if accepting_clients.count(True) == num_replicas:
            break


def drain_output(stream: IO[bytes]) -> None:
    for line in iter(stream.readline, b""):
        print(line.decode(), end="", file=sys.stderr)


def run_tester_client(protocol: str, test_name: str) -> subprocess.Popen[bytes]:
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.local_clients",
        "-p",
        protocol,
        "tester",
        "-t",
        test_name,
    ]
    return run_process(cmd)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--protocol", type=str, required=True, help="protocol name")
    args = parser.parse_args()

    do_cargo_build()

    kill_all_matching("local_clients.py", force=True)
    kill_all_matching("local_cluster.py", force=True)
    kill_all_matching("summerset_client", force=True)
    kill_all_matching("summerset_server", force=True)
    kill_all_matching("summerset_manager", force=True)

    if args.protocol not in PROTOCOLS_NUM_REPLICAS:
        raise ValueError(f"unrecognized protocol {args.protocol} to run workflow test")
    PROTOCOL = args.protocol
    NUM_REPLICAS = PROTOCOLS_NUM_REPLICAS[PROTOCOL]
    TEST_NAME = "primitive_ops"
    TIMEOUT = 300

    proc_cluster = launch_cluster(PROTOCOL, NUM_REPLICAS, config=None)
    wait_cluster_setup(proc_cluster, NUM_REPLICAS)
    # keep draining cluster output, otherwise servers block once the pipe fills
    assert proc_cluster.stderr is not None
    threading.Thread(target=drain_output, args=(proc_cluster.stderr,), daemon=True).start()

    proc_client = run_tester_client(PROTOCOL, TEST_NAME)

    try:
        out, err = proc_client.communicate(timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"Client tester did not finish in {TIMEOUT} secs")
        sys.exit(1)
    print(out.decode(), end="")
    print(err.decode(), end="", file=sys.stderr)
    client_rc = proc_client.returncode

    proc_cluster.terminate()

    if client_rc != 0:
        print(f"Client tester exitted with {client_rc}")
        sys.exit(client_rc)
    else:
        print("Client tester finished successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
