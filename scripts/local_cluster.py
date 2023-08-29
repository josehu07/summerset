import sys
import argparse
import subprocess
from pathlib import Path


def do_cargo_build(release):
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd, capture_stderr=False):
    print("Run:", " ".join(cmd))
    proc = None
    if capture_stderr:
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen(cmd)
    return proc


def kill_all_matching(name):
    print("Kill all:", name)
    assert name.count(" ") == 0
    cmd = ["pkill", "-9", "-f", name]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()


MANAGER_SRV_PORT = 52600
MANAGER_CLI_PORT = 52601

SERVER_API_PORT = lambda r: 52700 + r
SERVER_P2P_PORT = lambda r: 52800 + r


PROTOCOL_BACKER_PATH = {
    "RepNothing": lambda r: f"backer_path='/tmp/summerset.rep_nothing.{r}.wal'",
    "SimplePush": lambda r: f"backer_path='/tmp/summerset.simple_push.{r}.wal'",
    "MultiPaxos": lambda r: f"backer_path='/tmp/summerset.multipaxos.{r}.wal'",
    "RSPaxos": lambda r: f"backer_path='/tmp/summerset.rs_paxos.{r}.wal'",
    "Crossword": lambda r: f"backer_path='/tmp/summerset.crossword.{r}.wal'",
}


def config_with_backer_path(protocol, config, replica):
    result_config = PROTOCOL_BACKER_PATH[protocol](replica)

    if config is not None and len(config) > 0:
        if "backer_path" in config:
            result_config = config  # use user-supplied path
        else:
            result_config += "+"
            result_config += config

    return result_config


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
    return run_process(cmd, capture_stderr=True)


def wait_manager_setup(proc):
    accepting_servers, accepting_clients = False, False

    for line in iter(proc.stderr.readline, b""):
        sys.stderr.buffer.write(line)
        sys.stderr.flush()

        l = line.decode()
        if "(m) accepting servers" in l:
            assert not accepting_servers
            accepting_servers = True
        if "(m) accepting clients" in l:
            assert not accepting_clients
            accepting_clients = True

        if accepting_servers and accepting_clients:
            break


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
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return cmd


def launch_servers(protocol, num_replicas, release, config):
    server_procs = []
    for replica in range(num_replicas):
        cmd = compose_server_cmd(
            protocol,
            SERVER_API_PORT(replica),
            SERVER_P2P_PORT(replica),
            f"127.0.0.1:{MANAGER_SRV_PORT}",
            config_with_backer_path(protocol, config, replica),
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
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )
    args = parser.parse_args()

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
    wait_manager_setup(manager_proc)

    # then launch server replicas
    launch_servers(args.protocol, args.num_replicas, args.release, args.config)

    for line in iter(manager_proc.stderr.readline, b""):
        sys.stderr.buffer.write(line)
        sys.stderr.flush()

    rc = manager_proc.wait()
    sys.exit(rc)
