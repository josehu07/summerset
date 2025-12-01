import sys
import argparse
import subprocess
import math

from . import utils


MANAGER_CLI_PORT = lambda p: 40009 + p * 20  # NOTE: assuming at most 9 servers


CLIENT_OUTPUT_PATH = (
    lambda protocol, prefix, midfix, i: f"{prefix}/{protocol}{midfix}.{i}.out"
)

UTILITY_PARAM_NAMES = {
    "repl": [],
    "bench": [
        "fine_output",
        "freq_target",
        "value_size",
        "num_keys",
        "put_ratio",
        "ycsb_trace",
        "length_s",
        "use_random_keys",
        "skip_preloading",
        "norm_stdev_ratio",
        "unif_interval_ms",
        "unif_upper_bound",
    ],
    "tester": [
        "test_name",
        "keep_going",
        "logger_on",
    ],
    "mess": [
        "pause",
        "resume",
        "leader",
        "key_range",
        "responder",
        "write",
    ],
}


def run_process_pinned(i, cmd, cores_per_proc=0, remote=None, cd_dir=None):
    cpu_list = None
    if cores_per_proc != 0:
        # get number of processors
        num_cpus = utils.proc.get_cpu_count(remote=remote)
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
    if remote is None or len(remote) == 0:
        return utils.proc.run_process(cmd, cd_dir=cd_dir, cpu_list=cpu_list)
    else:
        return utils.proc.run_process_over_ssh(
            remote, cmd, cd_dir=cd_dir, cpu_list=cpu_list
        )


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
    protocol,
    manager,
    config,
    utility,
    timeout_ms,
    params,
    release,
    output_path=None,
    near_id=None,
):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_client"]
    cmd += [
        "-p",
        protocol,
        "-m",
        manager,
        "--timeout-ms",
        str(timeout_ms),
    ]

    if config is not None and len(config) > 0:
        # if dist_machs is set, near_id will be the node ID that's considered
        # the closest to this client
        # NOTE: dumb overwriting near_server_id field here for simplicity
        if near_id is not None and "near_server_id" in config:
            bi = config.index("near_server_id=") + 15
            epi = config[bi:].find("+")
            esi = config[bi:].find(" ")
            ei = epi if esi == -1 else esi if epi == -1 else min(epi, esi)
            ei = len(config) if ei == -1 else ei
            if config[bi:ei] == "x":
                config = config[:bi] + str(near_id) + config[ei:]
        cmd += ["--config", config]

    cmd += ["-u", utility]
    if output_path is not None:
        params = "+".join([f"output_path='{output_path}'", params])
    if len(params) > 0:
        cmd += ["--params", params]

    # if in benchmarking mode, lower the client's CPU scheduling priority?
    # if utility == "bench":
    #     cmd = ["nice", "-n", "19"] + cmd

    return cmd


def run_clients(
    remotes,
    ipaddrs,
    hosts,
    me,
    man,
    cd_dir,
    protocol,
    utility,
    partition,
    num_clients,
    num_replicas_for_near,
    dist_machs,
    params,
    release,
    config,
    output_prefix,
    output_midfix,
    pin_cores,
    timeout_ms,
):
    if num_clients < 1:
        raise ValueError(f"invalid num_clients: {num_clients}")

    manager_pub_ip = ipaddrs[man]

    # if dist_machs set, put clients round-robinly across this many machines
    # starting from me
    td_hosts = hosts[hosts.index(me) :] + hosts[: hosts.index(me)]
    if dist_machs > 0:
        td_hosts = td_hosts[:dist_machs]

    client_procs = []
    for i in range(num_clients):
        manager_addr = f"{manager_pub_ip}:{MANAGER_CLI_PORT(partition)}"

        cmd = compose_client_cmd(
            protocol,
            manager_addr,
            config,
            utility,
            timeout_ms,
            params,
            release,
            output_path=(
                CLIENT_OUTPUT_PATH(protocol, output_prefix, output_midfix, i)
                if len(output_prefix) > 0
                else None
            ),
            near_id=(
                hosts.index(
                    me if dist_machs <= 1 else td_hosts[i % len(td_hosts)]
                )
                % num_replicas_for_near
            ),
        )

        proc = None
        if dist_machs <= 1:
            proc = run_process_pinned(i, cmd, cores_per_proc=pin_cores)
        else:
            host = td_hosts[i % len(td_hosts)]
            local_i = i // len(td_hosts)
            if host == me:
                # run my responsible clients locally
                proc = run_process_pinned(
                    local_i,
                    cmd,
                    cores_per_proc=pin_cores,
                )
            else:
                # spawn client process on remote machine through ssh
                proc = run_process_pinned(
                    local_i,
                    cmd,
                    cores_per_proc=pin_cores,
                    remote=remotes[host],
                    cd_dir=cd_dir,
                )
        client_procs.append(proc)

    return client_procs


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-a",
        "--partition",
        type=int,
        default=argparse.SUPPRESS,
        help="if doing keyspace partitioning, the partition idx",
    )
    parser.add_argument(
        "-r", "--release", action="store_true", help="run release mode"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="protocol-specific TOML config string"
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
        "--man",
        type=str,
        default="host0",
        help="manager oracle's host nickname",
    )
    parser.add_argument(
        "--pin_cores",
        type=float,
        default=0,
        help="if not 0, set CPU cores affinity",
    )
    parser.add_argument(
        "--timeout_ms",
        type=int,
        default=5000,
        help="client-side request timeout",
    )
    parser.add_argument(
        "--skip_build", action="store_true", help="if set, skip cargo build"
    )

    subparsers = parser.add_subparsers(
        required=True,
        dest="utility",
        description="client utility mode: repl|bench|tester|mess",
    )

    _parser_repl = subparsers.add_parser("repl", help="REPL mode")

    parser_bench = subparsers.add_parser("bench", help="benchmark mode")
    parser_bench.add_argument(
        "-n",
        "--num_clients",
        type=int,
        required=True,
        help="number of client processes",
    )
    parser_bench.add_argument(
        "-m",
        "--num_replicas_for_near",
        type=int,
        default=5,
        help="number of replicas for correct near server setting",
    )
    parser_bench.add_argument(
        "-d",
        "--dist_machs",
        type=int,
        default=0,
        help="if > 0, put clients round-robinly on dist_machs machines",
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
    parser_bench.add_argument(
        "-w", "--put_ratio", type=int, help="percentage of puts"
    )
    parser_bench.add_argument(
        "-y", "--ycsb_trace", type=str, help="YCSB trace file"
    )
    parser_bench.add_argument(
        "-l", "--length_s", type=int, help="run length in secs"
    )
    parser_bench.add_argument(
        "--expect_halt",
        action="store_true",
        help="if set, expect there'll be a service halt",
    )
    parser_bench.add_argument(
        "--use_random_keys",
        action="store_true",
        help="if set, generate random keys",
    )
    parser_bench.add_argument(
        "--skip_preloading",
        action="store_true",
        help="if set, skip preloading phase",
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
        "--output_prefix",
        type=str,
        default="",
        help="output file prefix folder path",
    )
    parser_bench.add_argument(
        "--output_midfix",
        type=str,
        default="",
        help="output file extra identifier after protocol name",
    )
    parser_bench.add_argument(
        "--fine_output",
        action="store_true",
        help="if set, produce output at finer-grained time intervals",
    )

    parser_tester = subparsers.add_parser("tester", help="testing mode")
    parser_tester.add_argument(
        "-t",
        "--test_name",
        type=str,
        required=True,
        help="<test_name>|basic|all",
    )
    parser_tester.add_argument(
        "-k",
        "--keep_going",
        action="store_true",
        help="continue upon failed test",
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
    parser_mess.add_argument(
        "--leader",
        type=str,
        help="string form of configured leader ID (or empty string)",
    )
    parser_mess.add_argument(
        "--key_range",
        type=str,
        help="range of keys to apply responder set (or 'full' or 'reset')",
    )
    parser_mess.add_argument(
        "--responder",
        type=str,
        help="comma-separated list of servers as configured responders",
    )
    parser_mess.add_argument(
        "--write",
        type=str,
        help="colon-separated pair of key & value as a single-shot write",
    )

    args = parser.parse_args()

    # parse hosts config file
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir = f"{base}/{repo}"

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # check that the manager host is valid
    if args.man not in remotes:
        raise ValueError(f"invalid manager oracle's host {args.man}")

    # check that the partition index is valid
    partition_in_args = "partition" in args
    if partition_in_args and (args.partition < 0 or args.partition >= 5):
        raise ValueError("currently only supports <= 5 partitions")
    partition = 0 if not partition_in_args else args.partition
    output_midfix = args.output_midfix if args.utility == "bench" else ""
    if partition_in_args:
        output_midfix += f".{partition}"

    # check that number of clients does not exceed 99
    if args.utility == "bench":
        if args.num_clients <= 0:
            raise ValueError(f"invalid number of clients {args.num_clients}")
        elif args.num_clients > 99:
            raise ValueError(f"#clients {args.num_clients} > 99 not supported")

    # check that the prefix folder path exists, or create it if not
    if args.utility == "bench" and len(args.output_prefix) > 0:
        print("Preparing output folder...")
        prepare_procs = []
        for host in hosts:
            prepare_procs.append(
                utils.proc.run_process_over_ssh(
                    remotes[host],
                    ["mkdir", "-p", args.output_prefix],
                    cd_dir=cd_dir,
                    print_cmd=False,
                )
            )
        utils.proc.wait_parallel_procs(prepare_procs, names=hosts)

    # build everything
    if not partition_in_args and not args.skip_build:
        print("Building everything...")
        utils.file.do_cargo_build(args.release, cd_dir=cd_dir, remotes=remotes)

    # run client executable(s)
    client_procs = run_clients(
        remotes,
        ipaddrs,
        hosts,
        args.me,
        args.man,
        cd_dir,
        args.protocol,
        args.utility,
        partition,
        args.num_clients if args.utility == "bench" else 1,
        3 if args.utility != "bench" else args.num_replicas_for_near,
        0 if args.utility != "bench" else args.dist_machs,
        glue_params_str(args, UTILITY_PARAM_NAMES[args.utility]),
        args.release,
        args.config,
        "" if args.utility != "bench" else args.output_prefix,
        "" if args.utility != "bench" else output_midfix,
        args.pin_cores,
        args.timeout_ms,
    )

    # if running bench client, add proper timeout on wait
    timeout = None
    if args.utility == "bench":
        if args.length_s is None or args.length_s == 0:
            timeout = 600
        else:
            timeout = args.length_s + 30
    try:
        rcs = []
        for i, client_proc in enumerate(client_procs):
            rcs.append(client_proc.wait(timeout=timeout))
    except subprocess.TimeoutExpired:
        if args.expect_halt:  # mainly for failover experiments
            print("WARN: getting expected halt, exiting...")
            sys.exit(0)
        raise RuntimeError(f"some client(s) timed-out {timeout} secs")

    if any(map(lambda rc: rc != 0, rcs)):
        sys.exit(1)
    else:
        sys.exit(0)
