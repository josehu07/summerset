import os
import argparse
import subprocess
from pathlib import Path


def run_process(cmd):
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd)
    return proc


PROTOCOL_CONFIGS = {
    "RepNothing": lambda n: "",
    "SimplePush": lambda n: "",
    "MultiPaxos": lambda n: "",
}

MODE_PARAMS = {
    "repl": [],
    "bench": ["init_batch_size", "value_size", "put_ratio", "length_s", "adaptive"],
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


def compose_client_cmd(protocol, replica_list, config, mode, params, release):
    cmd = [
        "cargo",
        "run",
        "-p",
        "summerset_client",
    ]
    if release:
        cmd.append("-r")

    cmd += [
        "--",
        "-p",
        protocol,
    ]
    cmd += replica_list
    if len(config) > 0:
        cmd += ["--config", config]

    cmd += ["-m", mode]
    if len(params) > 0:
        cmd += ["--params", params]

    return cmd


def run_client(protocol, num_replicas, mode, params, release):
    api_ports = list(range(52700, 52700 + num_replicas * 10, 10))
    replica_list = []
    for replica in range(num_replicas):
        replica_list += ["-r", f"127.0.0.1:{api_ports[replica]}"]

    cmd = compose_client_cmd(
        protocol,
        replica_list,
        PROTOCOL_CONFIGS[protocol](num_replicas),
        mode,
        params,
        release,
    )
    proc = run_process(cmd)

    return proc


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
    )
    parser.add_argument("-r", "--release", action="store_true", help="run release mode")

    subparsers = parser.add_subparsers(
        required=True, dest="mode", description="client utility mode"
    )

    parser_repl = subparsers.add_parser("repl", help="REPL mode")

    parser_bench = subparsers.add_parser("bench", help="benchmark mode")
    parser_bench.add_argument(
        "-b", "--init_batch_size", type=int, help="initial batch size"
    )
    parser_bench.add_argument(
        "-v", "--value_size", type=int, help="value size in bytes"
    )
    parser_bench.add_argument("-w", "--put_ratio", type=int, help="percentage of puts")
    parser_bench.add_argument("-l", "--length_s", type=int, help="run length in secs")
    parser_bench.add_argument(
        "-a", "--adaptive", action="store_true", help="adaptive batch size"
    )

    args = parser.parse_args()

    if args.protocol not in PROTOCOL_CONFIGS:
        raise ValueError(f"unknown protocol name '{args.protocol}'")
    if args.num_replicas <= 0 or args.num_replicas > 9:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")

    client_proc = run_client(
        args.protocol,
        args.num_replicas,
        args.mode,
        glue_params_str(args, MODE_PARAMS[args.mode]),
        args.release,
    )
    client_proc.wait()
