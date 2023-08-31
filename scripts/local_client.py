import sys
import argparse
import subprocess


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


MANAGER_CLI_PORT = 52601


UTILITY_PARAM_NAMES = {
    "repl": [],
    "bench": ["freq_target", "value_size", "put_ratio", "length_s"],
    "tester": ["test_name", "keep_going", "logger_on"],
}


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


def run_client(protocol, utility, params, release, config):
    cmd = compose_client_cmd(
        protocol,
        f"127.0.0.1:{MANAGER_CLI_PORT}",
        config,
        utility,
        params,
        release,
    )
    proc = run_process(cmd)

    return proc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument("-r", "--release", action="store_true", help="run release mode")
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
    )

    subparsers = parser.add_subparsers(
        required=True,
        dest="utility",
        description="client utility mode: repl|bench|tester",
    )

    parser_repl = subparsers.add_parser("repl", help="REPL mode")

    parser_bench = subparsers.add_parser("bench", help="benchmark mode")
    parser_bench.add_argument(
        "-f", "--freq_target", type=int, help="frequency target reqs per sec"
    )
    parser_bench.add_argument(
        "-v", "--value_size", type=int, help="value size in bytes"
    )
    parser_bench.add_argument("-w", "--put_ratio", type=int, help="percentage of puts")
    parser_bench.add_argument("-l", "--length_s", type=int, help="run length in secs")

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

    # build everything
    do_cargo_build(args.release)

    # run client executable
    client_proc = run_client(
        args.protocol,
        args.utility,
        glue_params_str(args, UTILITY_PARAM_NAMES[args.utility]),
        args.release,
        args.config,
    )

    rc = client_proc.wait()
    sys.exit(rc)
