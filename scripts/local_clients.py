import os
import sys
import argparse
import subprocess
import multiprocessing

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


MANAGER_CLI_PORT = 52601


CLIENT_OUTPUT_PATH = lambda protocol, prefix, i: f"{prefix}/{protocol}.{i}.out"

UTILITY_PARAM_NAMES = {
    "repl": [],
    "bench": ["freq_target", "value_size", "put_ratio", "length_s"],
    "tester": ["test_name", "keep_going", "logger_on"],
    "mess": ["pause", "resume"],
}


def run_process_pinned(i, cmd, capture_stdout=False, cores_per_proc=0):
    if cores_per_proc > 0:
        # pin client cores from last CPU down
        num_cpus = multiprocessing.cpu_count()
        core_end = num_cpus - 1 - i * cores_per_proc
        core_start = core_end - cores_per_proc + 1
        assert core_start >= 0
        cmd = ["sudo", "taskset", "-c", f"{core_start}-{core_end}"] + cmd
    return utils.run_process(cmd, capture_stdout=capture_stdout)


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

    # if in benchmarking mode, lower the client's CPU scheduling priority?
    # if utility == "bench":
    #     cmd = ["nice", "-n", "19"] + cmd

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
        proc = run_process_pinned(
            i, cmd, capture_stdout=capture_stdout, cores_per_proc=pin_cores
        )

        client_procs.append(proc)

    return client_procs


if __name__ == "__main__":
    utils.check_proper_cwd()

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
        description="client utility mode: repl|bench|tester|mess",
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
    parser_bench.add_argument(
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

    parser_mess = subparsers.add_parser("mess", help="one-shot control mode")
    parser_mess.add_argument(
        "--pause", type=str, help="comma-separated list of servers to pause"
    )
    parser_mess.add_argument(
        "--resume", type=str, help="comma-separated list of servers to resume"
    )

    args = parser.parse_args()

    # check that the prefix folder path exists, or create it if not
    if (
        args.utility == "bench"
        and len(args.file_prefix) > 0
        and not os.path.isdir(args.file_prefix)
    ):
        os.system(f"mkdir -p {args.file_prefix}")

    # build everything
    rc = utils.do_cargo_build(args.release)
    if rc != 0:
        print("ERROR: cargo build failed")
        sys.exit(rc)

    capture_stdout = args.utility == "bench" and len(args.file_prefix) > 0
    num_clients = args.num_clients if args.utility == "bench" else 1

    # run client executable(s)
    client_procs = run_clients(
        args.protocol,
        args.utility,
        num_clients,
        glue_params_str(args, UTILITY_PARAM_NAMES[args.utility]),
        args.release,
        args.config,
        capture_stdout,
        args.pin_cores,
    )

    # if running bench client, add proper timeout on wait
    timeout = None
    if args.utility == "bench":
        if args.length_s is None:
            timeout = 75
        else:
            timeout = args.length_s + 15
    try:
        rcs = []
        for i, client_proc in enumerate(client_procs):
            if not capture_stdout:
                rcs.append(client_proc.wait(timeout=timeout))
            else:
                # doing automated experiments, so capture output
                out, _ = client_proc.communicate(timeout=timeout)
                with open(
                    CLIENT_OUTPUT_PATH(args.protocol, args.file_prefix, i), "w+"
                ) as fout:
                    fout.write(out.decode())
                rcs.append(client_proc.returncode)
    except subprocess.TimeoutExpired:
        print(f"ERROR: some client(s) timed-out {timeout} secs")
        sys.exit(1)

    if any(map(lambda rc: rc != 0, rcs)):
        sys.exit(1)
    else:
        sys.exit(0)
