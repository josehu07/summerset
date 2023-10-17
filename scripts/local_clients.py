import os
import sys
import argparse
import subprocess
import multiprocessing


MANAGER_CLI_PORT = 52601


CLIENT_OUTPUT_PATH = lambda protocol, prefix, i: f"{prefix}/{protocol}.{i}.out"

UTILITY_PARAM_NAMES = {
    "repl": [],
    "bench": ["freq_target", "value_size", "put_ratio", "length_s"],
    "tester": ["test_name", "keep_going", "logger_on"],
}


def path_get_last_segment(path):
    if "/" not in path:
        return None
    eidx = len(path) - 1
    while eidx > 0 and path[eidx] == "/":
        eidx -= 1
    bidx = path[:eidx].rfind("/")
    bidx += 1
    return path[bidx : eidx + 1]


def check_proper_cwd():
    cwd = os.getcwd()
    if "summerset" not in path_get_last_segment(cwd) or not os.path.isdir("scripts/"):
        print(
            "ERROR: script must be run under top-level repo with `python3 scripts/<script>.py ...`"
        )
        sys.exit(1)


def do_cargo_build(release):
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    proc = subprocess.Popen(cmd)
    return proc.wait()


def run_process(cmd, capture_stdout=False):
    print("Run:", " ".join(cmd))
    proc = None
    if capture_stdout:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    else:
        proc = subprocess.Popen(cmd)
    return proc


def pin_cores_for(i, pid, cores_per_proc):
    # pin client cores from last CPU down
    num_cpus = multiprocessing.cpu_count()
    core_end = num_cpus - 1 - i * cores_per_proc
    core_start = core_end - cores_per_proc + 1
    assert core_start >= 0
    print(f"Pinning cores: {i} ({pid}) -> {core_start}-{core_end}")
    cmd = [
        "sudo",
        "taskset",
        "-p",
        "-c",
        f"{core_start}-{core_end}",
        f"{pid}",
    ]
    proc = subprocess.Popen(cmd)
    return proc.wait()


def glue_params_str(cli_args, params_list):
    params_strs = []

    for param in params_list:
        value = getattr(cli_args, param)
        if value is None:
            continue

        if isinstance(value, str):
            params_strs.append(f"{param}='{value}'")
        elif isinstance(value, bool):
            params_strs.append(f"{param}={'true' if value else 'false'}")
        else:
            params_strs.append(f"{param}={value}")

    return "+".join(params_strs)


def compose_client_cmd(protocol, manager, config, utility, params, release):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_client"]
    cmd += [
        "-p",
        protocol,
        "-m",
        manager,
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]

    cmd += ["-u", utility]
    if len(params) > 0:
        cmd += ["--params", params]

    # if in benchmarking mode, lower the client's CPU scheduling priority
    if utility == "bench":
        cmd = ["nice", "-n", "19"] + cmd

    return cmd


def run_clients(
    protocol, utility, num_clients, params, release, config, capture_stdout, pin_cores
):
    if num_clients < 1:
        raise ValueError(f"invalid num_clients: {num_clients}")

    client_procs = []
    for i in range(num_clients):
        cmd = compose_client_cmd(
            protocol,
            f"127.0.0.1:{MANAGER_CLI_PORT}",
            config,
            utility,
            params,
            release,
        )
        proc = run_process(cmd, capture_stdout)

        if pin_cores > 0:
            pin_cores_for(i, proc.pid, pin_cores)

        client_procs.append()

    return client_procs


if __name__ == "__main__":
    check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument("-r", "--release", action="store_true", help="run release mode")
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )
    parser.add_argument(
        "--pin_cores", type=int, default=0, help="if > 0, set CPU cores affinity"
    )

    subparsers = parser.add_subparsers(
        required=True,
        dest="utility",
        description="client utility mode: repl|bench|tester",
    )

    parser_repl = subparsers.add_parser("repl", help="REPL mode")

    parser_bench = subparsers.add_parser("bench", help="benchmark mode")
    parser_bench.add_argument(
        "-n",
        "--num_clients",
        type=int,
        required=True,
        help="number of client processes",
    )
    parser_bench.add_argument(
        "-f", "--freq_target", type=int, help="frequency target reqs per sec"
    )
    parser_bench.add_argument(
        "-v", "--value_size", type=int, help="value size in bytes"
    )
    parser_bench.add_argument("-w", "--put_ratio", type=int, help="percentage of puts")
    parser_bench.add_argument("-l", "--length_s", type=int, help="run length in secs")
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="",
        help="output file prefix folder path",
    )

    parser_tester = subparsers.add_parser("tester", help="testing mode")
    parser_tester.add_argument(
        "-t", "--test_name", type=str, required=True, help="<test_name>|basic|all"
    )
    parser_tester.add_argument(
        "-k", "--keep_going", action="store_true", help="continue upon failed test"
    )
    parser_tester.add_argument(
        "--logger_on", action="store_true", help="do not suppress logger output"
    )

    args = parser.parse_args()

    # check that the prefix folder path exists, or create it if not
    if len(args.file_prefix) > 0 and not os.path.isdir(args.file_prefix):
        os.system(f"mkdir -p {args.file_prefix}")

    # build everything
    rc = do_cargo_build(args.release)
    if rc != 0:
        print("ERROR: cargo build failed")
        sys.exit(rc)

    # run client executable(s)
    capture_stdout = len(args.file_prefix) > 0
    client_procs = run_clients(
        args.protocol,
        args.utility,
        args.num_clients if args.utility == "bench" else 1,
        glue_params_str(args, UTILITY_PARAM_NAMES[args.utility]),
        args.release,
        args.config,
        capture_stdout,
        args.pin_cores,
    )

    rcs = []
    for i, client_proc in enumerate(client_procs):
        if not capture_stdout:
            rcs.append(client_proc.wait())
        else:
            out, _ = client_proc.communicate()
            with open(
                CLIENT_OUTPUT_PATH(args.protocol, args.file_prefix, i), "w+"
            ) as fout:
                fout.write(out.decode())
            rcs.append(client_proc.returncode)

    if any(map(lambda rc: rc != 0, rcs)):
        sys.exit(1)
    else:
        sys.exit(0)
