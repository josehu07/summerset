import os
import sys
import argparse
import subprocess
import multiprocessing
import math

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"

CHAIN_REPO_NAME = "chain-client"
CHAIN_JAR_FOLDER = "deploy/client"


SERVER_APP_PORT = 50500


CLIENT_OUTPUT_PATH = lambda protocol, prefix, midfix: f"{prefix}/{protocol}{midfix}.out"


def run_process_pinned(
    num_clients, cmd, capture_stdout=False, cores_per_proc=0, cd_dir=None
):
    # their implementation used a single process client with threading only
    # so `cores_per_proc` should be multiplied by the number of threads
    cpu_list = None
    if cores_per_proc != 0:
        # parse cores_per_proc setting
        if cores_per_proc != int(cores_per_proc) and (
            cores_per_proc > 1 or cores_per_proc < -1
        ):
            raise ValueError(f"invalid cores_per_proc {cores_per_proc}")
        num_cpus = multiprocessing.cpu_count()
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
    return utils.run_process(
        cmd, capture_stdout=capture_stdout, cd_dir=cd_dir, cpu_list=cpu_list
    )


def compose_client_cmd(
    ipaddrs,
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
        f"app_server_port={SERVER_APP_PORT}",
        "-p",
        f"maxexecutiontime={length_s}",
    ]
    return cmd


def run_clients(
    ipaddrs,
    value_size,
    put_ratio,
    length_s,
    num_threads,
    cd_dir,
    capture_output,
    pin_cores,
):
    if num_threads < 1:
        raise ValueError(f"invalid num_threads: {num_threads}")

    cmd = compose_client_cmd(
        ipaddrs,
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
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--protocol", type=str, required=True, help="protocol name"
    )
    parser.add_argument(
        "-n",
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
        "--base_dir",
        type=str,
        default="/mnt/eval",
        help="base cd dir after ssh",
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
    hosts_config = utils.read_toml_file(TOML_FILENAME)
    remotes = hosts_config["hosts"]
    hosts = sorted(list(remotes.keys()))
    domains = {
        name: utils.split_remote_string(remote)[1] for name, remote in remotes.items()
    }
    ipaddrs = {name: utils.lookup_dns_to_ip(domain) for name, domain in domains.items()}

    cd_dir_chain = f"{args.base_dir}/{CHAIN_REPO_NAME}/{CHAIN_JAR_FOLDER}"

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
                CLIENT_OUTPUT_PATH(args.protocol, args.file_prefix, args.file_midfix),
                "w+",
            ) as fout:
                fout.write(out.decode())
    except subprocess.TimeoutExpired:
        print(f"ERROR: client timed-out {timeout} secs", file=sys.stderr)
        sys.exit(1)

    sys.exit(client_proc.returncode)
