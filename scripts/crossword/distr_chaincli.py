import os
import sys
import argparse
import subprocess
import math

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"

CHAIN_REPO_NAME = "chain-client"
CHAIN_JAR_FOLDER = "deploy/client"


SERVER_APP_PORT = lambda p: 40020 + p


CLIENT_OUTPUT_PATH = lambda protocol, prefix, midfix: f"{prefix}/{protocol}{midfix}.out"


def run_process_pinned(
    num_clients, cmd, capture_stdout=False, cores_per_proc=0, cd_dir=None
):
    # their implementation used a single process client with threading only
    # so `cores_per_proc` should be multiplied by the number of threads
    cpu_list = None
    if cores_per_proc != 0:
        # get number of processors
        num_cpus = utils.proc.get_cpu_count()
        # parse cores_per_proc setting
        if cores_per_proc != int(cores_per_proc) and (
            cores_per_proc > 1 or cores_per_proc < -1
        ):
            raise ValueError(f"invalid cores_per_proc {cores_per_proc}")
        cores = cores_per_proc * num_clients
        if cores < 0:
            # negative means starting from CPU 0 (instead from last)
            cores *= -1
            core_start = 0
            core_end = math.ceil(core_start + cores - 1)
            assert core_end < num_cpus
        else:
            # else pin client cores from last CPU down
            core_end = math.ceil(num_cpus - 1)
            core_start = math.floor(core_end - cores + 1)
            assert core_start >= 0
        cpu_list = f"{core_start}-{core_end}"
    return utils.proc.run_process(
        cmd, capture_stdout=capture_stdout, cd_dir=cd_dir, cpu_list=cpu_list
    )


def compose_client_cmd(
    ipaddrs,
    app_port,
    num_threads,
    value_size,
    put_ratio,
    length_s,
):
    cmd = [
        "java",
        "-cp",
        "chain-client.jar:.",
        "site.ycsb.Client",
        "-t",
        "-s",
        "-P",
        "config.properties",
        "-threads",
        str(num_threads),
        "-p",
        f"fieldlength={value_size}",
        "-p",
        f"hosts={','.join(ipaddrs.values())}",
        "-p",
        "requestdistribution=zipfian",
        "-p",
        "fieldcount=1",
        "-p",
        f"readproportion={100 - put_ratio}",
        "-p",
        f"updateproportion={put_ratio}",
        "-p",
        f"app_server_port={app_port}",
        "-p",
        f"maxexecutiontime={length_s}",
    ]
    return cmd


def run_clients(
    ipaddrs,
    value_size,
    put_ratio,
    length_s,
    partition,
    num_threads,
    cd_dir,
    capture_stdout,
    pin_cores,
):
    if num_threads < 1:
        raise ValueError(f"invalid num_threads: {num_threads}")

    cmd = compose_client_cmd(
        ipaddrs,
        SERVER_APP_PORT(partition),
        num_threads,
        value_size,
        put_ratio,
        length_s,
    )

    client_proc = run_process_pinned(
        num_threads,
        cmd,
        capture_stdout=capture_stdout,
        cores_per_proc=pin_cores,
        cd_dir=cd_dir,
    )
    return client_proc


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n", "--num_replicas", type=int, required=True, help="number of replicas"
    )
    parser.add_argument(
        "-g", "--group", type=str, default="1dc", help="hosts group to run on"
    )
    parser.add_argument(
        "--me", type=str, default="host0", help="main script runner's host nickname"
    )
    parser.add_argument(
        "-a",
        "--partition",
        type=int,
        default=argparse.SUPPRESS,
        help="if doing keyspace partitioning, the partition idx",
    )
    parser.add_argument(
        "-t",
        "--num_threads",
        type=int,
        required=True,
        help="number of threads",
    )
    parser.add_argument(
        "-v", "--value_size", type=int, required=True, help="value size"
    )
    parser.add_argument(
        "-w", "--put_ratio", type=int, required=True, help="percentage of puts"
    )
    parser.add_argument(
        "-l", "--length_s", type=int, required=True, help="run length in secs"
    )
    parser.add_argument(
        "--pin_cores", type=float, default=0, help="if not 0, set CPU cores affinity"
    )
    parser.add_argument(
        "--file_prefix",
        type=str,
        default="",
        help="output file prefix folder path",
    )
    parser.add_argument(
        "--file_midfix",
        type=str,
        default="",
        help="output file extra identifier after protocol name",
    )
    args = parser.parse_args()

    # parse hosts config file
    base, _, hosts, remotes, _, ipaddrs = utils.config.parse_toml_file(
        TOML_FILENAME, args.group
    )
    cd_dir_chain = f"{base}/{CHAIN_REPO_NAME}/{CHAIN_JAR_FOLDER}"

    # check that number of replicas is valid
    if args.num_replicas <= 0:
        raise ValueError(f"invalid number of replicas {args.num_replicas}")
    if args.num_replicas > len(ipaddrs):
        raise ValueError("#replicas exceeds #hosts in the config file")
    hosts = hosts[: args.num_replicas]
    remotes = {h: remotes[h] for h in hosts}
    ipaddrs = {h: ipaddrs[h] for h in hosts}

    # check that I am indeed the "me" host
    utils.config.check_remote_is_me(remotes[args.me])

    # check that the partition index is valid
    partition_in_args, partition = False, 0
    partition_in_args = "partition" in args
    if partition_in_args and (args.partition < 0 or args.partition >= 5):
        raise ValueError("currently only supports <= 5 partitions")
    partition = 0 if not partition_in_args else args.partition
    file_midfix = (
        args.file_midfix if not partition_in_args else f"{args.file_midfix}.{partition}"
    )

    # check that number of clients is valid
    if args.num_threads <= 0:
        raise ValueError(f"invalid number of clients {args.num_threads}")

    # check that the prefix folder path exists, or create it if not
    if len(args.file_prefix) > 0 and not os.path.isdir(args.file_prefix):
        os.system(f"mkdir -p {args.file_prefix}")

    capture_stdout = len(args.file_prefix) > 0

    # run client executable(s)
    client_proc = run_clients(
        ipaddrs,
        args.value_size,
        args.put_ratio,
        args.length_s,
        partition,
        args.num_threads,
        cd_dir_chain,
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
                CLIENT_OUTPUT_PATH(args.protocol, args.file_prefix, file_midfix),
                "w+",
            ) as fout:
                fout.write(out.decode())
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"client timed-out {timeout} secs")

    sys.exit(client_proc.returncode)
