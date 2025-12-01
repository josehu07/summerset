import os
import sys
import argparse
import subprocess
import math

from .. import utils


COCK_REPO_NAME = "cockroach"


SERVER_SQL_PORT = 26157


WORKLOAD_OUTPUT_PATH = (
    lambda protocol,
    prefix,
    midfix,
    workload,
    phase: f"{prefix}/{protocol}{midfix}.{workload}.{phase}"
)

WORKLOAD_SETTINGS = {
    "kv": {
        "init": lambda c, v, h: [
            "--drop",
        ],
        "run": lambda c, v, h: [
            f"--min-block-bytes={v}",
            f"--max-block-bytes={v}",
            "--read-percent=0",
        ],
    },
    "bank": {
        "init": lambda c, v, h: [
            "--drop",
            f"--payload-bytes={v}",
        ],
        "run": lambda c, v, h: [
            f"--payload-bytes={v}",
        ],
    },
    "tpcc": {
        "init": lambda c, v, h: [
            "--drop",
            f"--warehouses={h}",
        ],
        "run": lambda c, v, h: [
            f"--warehouses={h}",
            f"--workers={c}",
            "--wait=false",
            "--replicate-static-columns=true",
            "--mix=newOrder=10,payment=10,orderStatus=1,delivery=1,stockLevel=1",
            # below was used for more accurate newOrder msg size profiling
            # "--mix=newOrder=100,payment=1,orderStatus=1,delivery=1,stockLevel=1",
        ],
    },
}


def run_process_pinned(
    cmd,
    capture_stdout=False,
    cores_per_proc=0,
    cd_dir=None,
    extra_env=None,
):
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
            core_start = 0
            core_end = math.ceil(core_start + cores_per_proc - 1)
            assert core_end < num_cpus
        else:
            # else pin client cores from last CPU down
            core_end = math.ceil(num_cpus - 1)
            core_start = math.floor(core_end - cores_per_proc + 1)
            assert core_start >= 0
        cpu_list = f"{core_start}-{core_end}"
    return utils.proc.run_process(
        cmd,
        capture_stdout=capture_stdout,
        cd_dir=cd_dir,
        cpu_list=cpu_list,
        extra_env=extra_env,
    )


def compose_init_cmd(
    workload,
    concurrency,
    value_size,
    warehouses,
    sql_addr,
):
    cmd = [
        "./cockroach",
        "workload",
        "init",
        workload,
        f"--concurrency={concurrency}",
    ]
    cmd += WORKLOAD_SETTINGS[workload]["init"](
        concurrency, value_size, warehouses
    )
    cmd.append(f"postgresql://root@{sql_addr}?sslmode=disable")
    return cmd


def init_workload(
    workload,
    ipaddrs,
    hosts,
    concurrency,
    value_size,
    warehouses,
    cd_dir,
    capture_stdout,
    pin_cores,
):
    extra_env = None
    if workload == "tpcc" and value_size > 0:
        if value_size < 1 or value_size > 4096:
            raise ValueError(
                f"textScale {value_size} too large: expect in range [1, 4096]"
            )
        extra_env = {"COCKROACH_TPCC_TEXT_SCALE": str(value_size)}
        warehouses //= max(value_size // 8, 1)

    sql_addr = f"{ipaddrs[hosts[0]]}:{SERVER_SQL_PORT}"
    cmd = compose_init_cmd(
        workload, concurrency, value_size, warehouses, sql_addr
    )

    client_proc = run_process_pinned(
        cmd,
        capture_stdout=capture_stdout,
        cores_per_proc=pin_cores,
        cd_dir=cd_dir,
        extra_env=extra_env,
    )
    return client_proc


def compose_run_cmd(
    workload,
    concurrency,
    value_size,
    warehouses,
    length_s,
    sql_addr,
):
    cmd = [
        "./cockroach",
        "workload",
        "run",
        workload,
        f"--concurrency={concurrency}",
        f"--duration={length_s}s",
    ]
    cmd += WORKLOAD_SETTINGS[workload]["run"](
        concurrency, value_size, warehouses
    )
    cmd.append(f"postgresql://root@{sql_addr}?sslmode=disable")
    return cmd


def run_workload(
    workload,
    ipaddrs,
    hosts,
    concurrency,
    value_size,
    warehouses,
    length_s,
    cd_dir,
    capture_stdout,
    pin_cores,
):
    extra_env = None
    if workload == "tpcc" and value_size > 0:
        if value_size < 1 or value_size > 4096:
            raise ValueError(
                f"textScale {value_size} too large: expect in range [1, 4096]"
            )
        extra_env = {"COCKROACH_TPCC_TEXT_SCALE": str(value_size)}
        warehouses //= max(value_size // 8, 1)

    sql_addr = f"{ipaddrs[hosts[0]]}:{SERVER_SQL_PORT}"
    cmd = compose_run_cmd(
        workload, concurrency, value_size, warehouses, length_s, sql_addr
    )

    client_proc = run_process_pinned(
        cmd,
        capture_stdout=capture_stdout,
        cores_per_proc=pin_cores,
        cd_dir=cd_dir,
        extra_env=extra_env,
    )
    return client_proc


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    parser.add_argument(
        "--me",
        type=str,
        default="host0",
        help="main script runner's host nickname",
    )
    parser.add_argument(
        "-w", "--workload", type=str, required=True, help="name of workload"
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=16,
        help="number of concurrent workers",
    )
    parser.add_argument(
        "-v",
        "--value_size",
        type=int,
        default=256,
        help="payload size (meaning differs per workload)",
    )
    parser.add_argument(
        "-s",
        "--warehouses",
        type=int,
        default=200,
        help="number of warehouses (tpcc only)",
    )
    parser.add_argument(
        "-i",
        "--init",
        action="store_true",
        help="if set, do the init phase; else do the run phase",
    )
    parser.add_argument(
        "-l", "--length_s", type=int, default=60, help="run length in secs"
    )
    parser.add_argument(
        "--pin_cores",
        type=float,
        default=0,
        help="if not 0, set CPU cores affinity",
    )
    parser.add_argument(
        "--output_prefix",
        type=str,
        default="",
        help="output file prefix folder path",
    )
    parser.add_argument(
        "--output_midfix",
        type=str,
        default="",
        help="output file extra identifier after protocol name",
    )
    args = parser.parse_args()

    # parse hosts config file
    base, _, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir_cockroach = f"{base}/{COCK_REPO_NAME}"

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # check workload name
    if args.workload not in WORKLOAD_SETTINGS:
        raise ValueError(f"unrecognized workload name '{args.workload}'")

    # check that number of workers is valid
    if args.concurrency <= 0:
        raise ValueError(f"invalid number of workers {args.concurrency}")

    # check that the prefix folder path exists, or create it if not
    if len(args.output_prefix) > 0 and not os.path.isdir(args.output_prefix):
        os.system(f"mkdir -p {args.output_prefix}")

    capture_stdout = len(args.output_prefix) > 0

    client_proc = None
    if args.init:
        # invoke the init phase
        client_proc = init_workload(
            args.workload,
            ipaddrs,
            hosts,
            args.concurrency,
            args.value_size,
            args.warehouses,
            cd_dir_cockroach,
            capture_stdout,
            args.pin_cores,
        )

        if not capture_stdout:
            client_proc.wait()
        else:
            # doing automated experiments, so capture output
            out, _ = client_proc.communicate()
            with open(
                WORKLOAD_OUTPUT_PATH(
                    args.protocol,
                    args.output_prefix,
                    args.output_midfix,
                    args.workload,
                    "load",
                ),
                "w+",
            ) as fout:
                fout.write(out.decode())
    else:
        # invoke the run phase
        client_proc = run_workload(
            args.workload,
            ipaddrs,
            hosts,
            args.concurrency,
            args.value_size,
            args.warehouses,
            args.length_s,
            cd_dir_cockroach,
            capture_stdout,
            args.pin_cores,
        )

        # if running bench client, add proper timeout on wait
        timeout = args.length_s + 15
        try:
            if not capture_stdout:
                client_proc.wait(timeout=timeout)
            else:
                # doing automated experiments, so capture output
                out, _ = client_proc.communicate(timeout=timeout)
                with open(
                    WORKLOAD_OUTPUT_PATH(
                        args.protocol,
                        args.output_prefix,
                        args.output_midfix,
                        args.workload,
                        "run",
                    ),
                    "w+",
                ) as fout:
                    fout.write(out.decode())
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"client timed-out {timeout} secs")

    sys.exit(client_proc.returncode)
