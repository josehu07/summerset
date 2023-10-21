import sys
import os
import signal
import argparse
import subprocess
import multiprocessing

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


MANAGER_SRV_PORT = 52600
MANAGER_CLI_PORT = 52601

SERVER_API_PORT = lambda r: 52700 + r
SERVER_P2P_PORT = lambda r: 52800 + r


PROTOCOL_BACKER_PATH = lambda protocol, prefix, r: f"{prefix}/{protocol}.{r}.wal"
PROTOCOL_SNAPSHOT_PATH = lambda protocol, prefix, r: f"{prefix}/{protocol}.{r}.snap"

PROTOCOL_MAY_SNAPSHOT = {
    "RepNothing": False,
    "SimplePush": False,
    "MultiPaxos": True,
    "Raft": True,
    "RSPaxos": True,
    "CRaft": True,
    "Crossword": True,
}

PROTOCOL_EXTRA_DEFAULTS = {
    "RSPaxos": lambda n, _: f"fault_tolerance={(n//2)//2}",
    "CRaft": lambda n, _: f"fault_tolerance={(n//2)//2}",
    "Crossword": lambda n, _: f"fault_tolerance={n//2}",
}


def run_process_pinned(i, cmd, capture_stderr=False, cores_per_proc=0):
    if cores_per_proc > 0:
        # pin servers from CPU 0 up
        num_cpus = multiprocessing.cpu_count()
        core_start = i * cores_per_proc
        core_end = core_start + cores_per_proc - 1
        assert core_end <= num_cpus - 1
        cmd = ["sudo", "taskset", "-c", f"{core_start}-{core_end}"] + cmd
    return utils.run_process(cmd, capture_stderr=capture_stderr)


def config_with_defaults(
    protocol, config, num_replicas, replica_id, file_prefix, fresh_files
):
    def config_str_to_dict(s):
        l = s.strip().split("+")
        l = [c.strip().split("=") for c in l]
        for c in l:
            assert len(c) == 2
        return {c[0]: c[1] for c in l}

    def config_dict_to_str(d):
        l = ["=".join([k, v]) for k, v in d.items()]
        return "+".join(l)

    backer_path = PROTOCOL_BACKER_PATH(protocol, file_prefix, replica_id)
    config_dict = {"backer_path": f"'{backer_path}'"}
    if fresh_files and os.path.isfile(backer_path):
        print(f"Delete: {backer_path}")
        os.remove(backer_path)

    if PROTOCOL_MAY_SNAPSHOT[protocol]:
        snapshot_path = PROTOCOL_SNAPSHOT_PATH(protocol, file_prefix, replica_id)
        config_dict["snapshot_path"] = f"'{snapshot_path}'"
        if fresh_files and os.path.isfile(snapshot_path):
            print(f"Delete: {snapshot_path}")
            os.remove(snapshot_path)

    if protocol in PROTOCOL_EXTRA_DEFAULTS:
        config_dict.update(
            config_str_to_dict(
                PROTOCOL_EXTRA_DEFAULTS[protocol](num_replicas, replica_id)
            )
        )

    if config is not None and len(config) > 0:
        config_dict.update(config_str_to_dict(config))

    return config_dict_to_str(config_dict)


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


def launch_manager(protocol, num_replicas, release, pin_cores):
    cmd = compose_manager_cmd(
        protocol,
        MANAGER_SRV_PORT,
        MANAGER_CLI_PORT,
        num_replicas,
        release,
    )
    return run_process_pinned(0, cmd, capture_stderr=True, cores_per_proc=pin_cores)


def wait_manager_setup(proc, print_stderr=True):
    # print("Waiting for manager setup...")
    accepting_servers, accepting_clients = False, False

    for line in iter(proc.stderr.readline, b""):
        if print_stderr:
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


def launch_servers(
    protocol, num_replicas, release, config, file_prefix, fresh_files, pin_cores
):
    if num_replicas not in (3, 5, 7, 9):
        raise ValueError(f"invalid num_replicas: {num_replicas}")

    server_procs = []
    for replica in range(num_replicas):
        cmd = compose_server_cmd(
            protocol,
            SERVER_API_PORT(replica),
            SERVER_P2P_PORT(replica),
            f"127.0.0.1:{MANAGER_SRV_PORT}",
            config_with_defaults(
                protocol, config, num_replicas, replica, file_prefix, fresh_files
            ),
            release,
        )
        proc = run_process_pinned(
            replica + 1, cmd, capture_stderr=False, cores_per_proc=pin_cores
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
        "-r", "--release", action="store_true", help="if set, run release mode"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="/tmp/summerset",
        help="states file prefix folder path",
    )
    parser.add_argument(
        "--keep_files", action="store_true", help="if set, keep any old durable files"
    )
    parser.add_argument(
        "--pin_cores", type=int, default=0, help="if > 0, set CPU cores affinity"
    )
    args = parser.parse_args()

    # kill all existing server and manager processes
    if args.pin_cores == 0:
        utils.kill_all_matching("summerset_server")
        utils.kill_all_matching("summerset_manager")

    # check that the prefix folder path exists, or create it if not
    if not os.path.isdir(args.file_prefix):
        os.system(f"mkdir -p {args.file_prefix}")

    # build everything
    rc = utils.do_cargo_build(args.release)
    if rc != 0:
        print("ERROR: cargo build failed")
        sys.exit(rc)

    # launch cluster manager oracle first
    manager_proc = launch_manager(
        args.protocol, args.num_replicas, args.release, args.pin_cores
    )
    wait_manager_setup(manager_proc, (args.pin_cores == 0))

    # then launch server replicas
    server_procs = launch_servers(
        args.protocol,
        args.num_replicas,
        args.release,
        args.config,
        args.file_prefix,
        not args.keep_files,
        args.pin_cores,
    )

    # register termination signals handler
    def kill_spawned_procs(*args):
        for proc in server_procs:
            proc.terminate()
        for proc in server_procs:
            proc.wait()
        manager_proc.terminate()

    signal.signal(signal.SIGINT, kill_spawned_procs)
    signal.signal(signal.SIGTERM, kill_spawned_procs)
    signal.signal(signal.SIGHUP, kill_spawned_procs)

    # since we piped manager proc's output, re-print it out
    for line in iter(manager_proc.stderr.readline, b""):
        if args.pin_cores == 0:
            sys.stderr.buffer.write(line)
            sys.stderr.flush()

    # reaches here after manager proc has terminated
    rc = manager_proc.wait()
    sys.exit(rc)
