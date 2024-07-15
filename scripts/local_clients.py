import os
import sys
import argparse
import subprocess
import math

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


CLIENT_LOOP_IP = "127.0.0.1"
CLIENT_VETH_IP = lambda c: f"10.0.2.{c}"
CLIENT_BIND_BASE_PORT = lambda c: 32000 + c * 10

MANAGER_LOOP_IP = "127.0.0.1"
MANAGER_VETH_IP = "10.0.0.0"
MANAGER_CLI_PORT = 30001


CLIENT_OUTPUT_PATH = (
    lambda protocol, prefix, midfix, i: f"{prefix}/{protocol}{midfix}.{i}.out"
)

UTILITY_PARAM_NAMES = {
    "repl": [],
    "bench": [
        "freq_target",
        "value_size",
        "num_keys",
        "put_ratio",
        "ycsb_trace",
        "length_s",
        "norm_stdev_ratio",
        "unif_interval_ms",
        "unif_upper_bound",
    ],
    "tester": ["test_name", "keep_going", "logger_on"],
    "mess": ["pause", "resume"],
}


def run_process_pinned(i, cmd, capture_stdout=False, cores_per_proc=0):
    cpu_list = None
    if cores_per_proc != 0:
        # get number of processors
        num_cpus = utils.proc.get_cpu_count()
        # parse cores_per_proc setting
        if cores_per_proc != int(cores_per_proc) and (
            cores_per_proc > 1 or cores_per_proc < -1
        ):
            raise ValueError(f"invalid cores_per_proc {cores_per_proc}")
        if cores_per_proc < 0:
            # negative means starting from CPU 0 (instead from last)
            cores_per_proc *= -1
            core_start = math.floor(i * cores_per_proc)
            core_end = math.ceil(core_start + cores_per_proc - 1)
            assert core_end < num_cpus
        else:
            # else pin client cores from last CPU down
            core_end = math.ceil(num_cpus - 1 - i * cores_per_proc)
            core_start = math.floor(core_end - cores_per_proc + 1)
            assert core_start >= 0
        cpu_list = f"{core_start}-{core_end}"
    return utils.proc.run_process(cmd, capture_stdout=capture_stdout, cpu_list=cpu_list)


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


def compose_client_cmd(
    protocol, bind_base, manager, config, utility, timeout_ms, params, release
):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_client"]
    cmd += [
        "-p",
        protocol,
        "-b",
        bind_base,
        "-m",
        manager,
        "--timeout-ms",
        str(timeout_ms),
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
    protocol,
    utility,
    num_clients,
    params,
    release,
    config,
    capture_stdout,
    pin_cores,
    use_veth,
    base_idx,
    timeout_ms,
):
    if num_clients < 1:
        raise ValueError(f"invalid num_clients: {num_clients}")

    client_procs = []
    for i in range(num_clients):
        client = base_idx + i
        bind_base = f"{CLIENT_LOOP_IP}:{CLIENT_BIND_BASE_PORT(client)}"
        manager_addr = f"{MANAGER_LOOP_IP}:{MANAGER_CLI_PORT}"
        if use_veth:
            bind_base = f"{CLIENT_VETH_IP(client)}:{CLIENT_BIND_BASE_PORT(client)}"
            manager_addr = f"{MANAGER_VETH_IP}:{MANAGER_CLI_PORT}"

        cmd = compose_client_cmd(
            protocol,
            bind_base,
            manager_addr,
            config,
            utility,
            timeout_ms,
            params,
            release,
        )

        proc = run_process_pinned(
            i, cmd, capture_stdout=capture_stdout, cores_per_proc=pin_cores
        )
        client_procs.append(proc)

    return client_procs


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument("-r", "--release", action="store_true", help="run release mode")
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )
    parser.add_argument(
        "--pin_cores", type=float, default=0, help="if not 0, set CPU cores affinity"
    )
    parser.add_argument(
        "--use_veth", action="store_true", help="if set, use netns and veth setting"
    )
    parser.add_argument(
        "--base_idx",
        type=int,
        default=0,
        help="idx of the first client for calculating ports",
    )
    parser.add_argument(
        "--timeout_ms", type=int, default=5000, help="client-side request timeout"
    )
    parser.add_argument(
        "--skip_build", action="store_true", help="if set, skip cargo build"
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
        "-v", "--value_size", type=str, help="value sizes over time"
    )
    parser_bench.add_argument(
        "-k", "--num_keys", type=int, help="number of keys to choose from"
    )
    parser_bench.add_argument("-w", "--put_ratio", type=int, help="percentage of puts")
    parser_bench.add_argument("-y", "--ycsb_trace", type=str, help="YCSB trace file")
    parser_bench.add_argument("-l", "--length_s", type=int, help="run length in secs")
    parser_bench.add_argument(
        "--expect_halt",
        action="store_true",
        help="if set, expect there'll be a service halt",
    )
    parser_bench.add_argument(
        "--norm_stdev_ratio", type=float, help="normal dist stdev ratio"
    )
    parser_bench.add_argument(
        "--unif_interval_ms", type=int, help="uniform dist usage interval"
    )
    parser_bench.add_argument(
        "--unif_upper_bound", type=int, help="uniform dist upper bound"
    )
    parser_bench.add_argument(
        "--file_prefix",
        type=str,
        default="",
        help="output file prefix folder path",
    )
    parser_bench.add_argument(
        "--file_midfix",
        type=str,
        default="",
        help="output file extra identifier after protocol name",
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

    # check that number of clients does not exceed 99
    if args.utility == "bench":
        if args.num_clients <= 0:
            raise ValueError(f"invalid number of clients {args.num_clients}")
        elif args.num_clients > 99:
            raise ValueError(f"#clients {args.num_clients} > 99 not supported")

    # check that the prefix folder path exists, or create it if not
    if (
        args.utility == "bench"
        and len(args.file_prefix) > 0
        and not os.path.isdir(args.file_prefix)
    ):
        os.system(f"mkdir -p {args.file_prefix}")

    # build everything
    if not args.skip_build:
        print("Building everything...")
        utils.file.do_cargo_build(args.release)

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
        args.use_veth,
        args.base_idx,
        args.timeout_ms,
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
                    CLIENT_OUTPUT_PATH(
                        args.protocol, args.file_prefix, args.file_midfix, i
                    ),
                    "w+",
                ) as fout:
                    fout.write(out.decode())
                rcs.append(client_proc.returncode)
    except subprocess.TimeoutExpired:
        if args.expect_halt:  # mainly for failover experiments
            print("WARN: getting expected halt, exitting...")
            sys.exit(0)
        raise RuntimeError(f"some client(s) timed-out {timeout} secs")

    if any(map(lambda rc: rc != 0, rcs)):
        sys.exit(1)
    else:
        sys.exit(0)
