import sys
import os
import argparse
import subprocess


def do_cargo_build():
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd):
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def kill_all_matching(name, force=False):
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


def launch_cluster(protocol, num_replicas, config):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return run_process(cmd)


def wait_cluster_setup(proc, num_replicas):
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


def run_tester_client(protocol, test_name):
    cmd = [
        "python3",
        "./scripts/local_clients.py",
        "-p",
        protocol,
        "tester",
        "-t",
        test_name,
    ]
    return run_process(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    args = parser.parse_args()

    do_cargo_build()

    kill_all_matching("local_clients.py", force=True)
    kill_all_matching("local_cluster.py", force=True)
    kill_all_matching("summerset_client", force=True)
    kill_all_matching("summerset_server", force=True)
    kill_all_matching("summerset_manager", force=True)

    PROTOCOL = "MultiPaxos"
    if args.protocol == "MultiPaxos":
        pass
    elif args.protocol == "Raft":
        PROTOCOL = "Raft"
    else:
        raise ValueError(
            f"unrecognized protocol {args.protocol} to run workflow test"
        )

    NUM_REPLICAS = 3
    TEST_NAME = "primitive_ops"
    TIMEOUT = 300

    proc_cluster = launch_cluster(PROTOCOL, NUM_REPLICAS, config=None)
    wait_cluster_setup(proc_cluster, NUM_REPLICAS)

    proc_client = run_tester_client(PROTOCOL, TEST_NAME)

    try:
        client_rc = proc_client.wait(timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"Client tester did not finish in {TIMEOUT} secs")
        exit(1)

    proc_cluster.terminate()

    if client_rc != 0:
        print(f"Client tester exitted with {client_rc}")
        exit(client_rc)
    else:
        exit(0)
