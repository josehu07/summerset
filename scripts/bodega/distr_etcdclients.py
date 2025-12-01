import sys
import argparse
import subprocess
import math

from .. import utils


SERVER_CLI_PORT = 21379


CLIENT_OUTPUT_PATH = (
    lambda protocol, prefix, midfix, i: f"{prefix}/{protocol}{midfix}.{i}.out"
)

UTILITY_PARAM_NAMES = {
    "repl": None,
    "bench": [
        "freq_target",
        "value_size",
        "num_keys",
        "put_ratio",
        "ycsb_trace",
        "length_s",
        "use_random_keys",
        # others are unused for etcd
    ],
    "tester": None,
    "mess": None,
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
    server,
    config,
    utility,
    params,
    release,
    output_path=None,
):
    cmd = [f"./target/{'release' if release else 'debug'}/summerset_client"]
    cmd += [
        "-p",
        protocol,
        "-m",
        server,
    ]
    if config is not None and len(config) > 0:
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
    server,
    cd_dir,
    protocol,
    utility,
    num_clients,
    num_replicas_for_near,
    dist_machs,
    params,
    release,
    config,
    output_prefix,
    output_midfix,
    pin_cores,
):
    if num_clients < 1:
        raise ValueError(f"invalid num_clients: {num_clients}")

    # if dist_machs set, put clients round-robinly across this many machines
    # starting from me
    td_hosts = hosts[hosts.index(me) :] + hosts[: hosts.index(me)]
    if dist_machs > 0:
        td_hosts = td_hosts[:dist_machs]

    client_procs = []
    for i in range(num_clients):
        # decide on the server to connect to. If dist_machs is set, use the near
        # server; otherwise, use the passed-in `server`
        server_pub_ip = ipaddrs[server]
        if dist_machs > 0:
            near_id = (
                hosts.index(
                    me if dist_machs <= 1 else td_hosts[i % len(td_hosts)]
                )
                % num_replicas_for_near
            )
            server_pub_ip = ipaddrs[hosts[near_id]]
        server_addr = f"{server_pub_ip}:{SERVER_CLI_PORT}"

        cmd = compose_client_cmd(
            protocol,
            server_addr,
            config,
            utility,
            params,
            release,
            output_path=(
                CLIENT_OUTPUT_PATH(protocol, output_prefix, output_midfix, i)
                if len(output_prefix) > 0
                else None
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
        "-p",
        "--protocol",
        type=str,
        default="Etcd",
        help="protocol name (unused yet)",
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
        "-s",
        "--server",
        type=str,
        default="host0",
        help="connecting server's host nickname (could be ignored with 'bench -d')",
    )
    parser.add_argument(
        "--pin_cores",
        type=float,
        default=0,
        help="if not 0, set CPU cores affinity",
    )
    parser.add_argument(
        "--skip_build", action="store_true", help="if set, skip cargo build"
    )

    subparsers = parser.add_subparsers(
        required=True,
        dest="utility",
        description="client utility mode (now only accepts 'bench')",
    )

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

    args = parser.parse_args()

    # parse hosts config file
    base, repo, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        args.group
    )
    cd_dir = f"{base}/{repo}"

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # check that the server host is valid
    if args.server not in remotes:
        raise ValueError(f"invalid connecting server's host {args.server}")

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
    if not args.skip_build:
        print("Building everything...")
        utils.file.do_cargo_build(args.release, cd_dir=cd_dir, remotes=remotes)

    # check that the utility mode is supported
    if UTILITY_PARAM_NAMES[args.utility] is None:
        raise ValueError(
            f"utility mode '{args.utility}' not supported for etcd"
        )

    # run client executable(s)
    client_procs = run_clients(
        remotes,
        ipaddrs,
        hosts,
        args.me,
        args.server,
        cd_dir,
        args.protocol,
        args.utility,
        args.num_clients if args.utility == "bench" else 1,
        3 if args.utility != "bench" else args.num_replicas_for_near,
        0 if args.utility != "bench" else args.dist_machs,
        glue_params_str(args, UTILITY_PARAM_NAMES[args.utility]),
        args.release,
        args.config,
        "" if args.utility != "bench" else args.output_prefix,
        "" if args.utility != "bench" else args.output_midfix,
        args.pin_cores,
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


if __name__ == "__main__":
    main()
