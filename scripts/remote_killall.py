import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"


def compose_kill_cmds(chain=False, cockroach=False, zookeeper=False):
    cmds = [["./scripts/kill_all_procs.sh", "incl_distr"]]
    if chain:
        cmds.append(["./scripts/crossword/kill_chain_procs.sh", "incl_distr"])
    if cockroach:
        cmds.append(["./scripts/crossword/kill_cockroach_procs.sh", "incl_distr"])
    if zookeeper:
        cmds.append(["./scripts/bodega/kill_zookeeper_procs.sh", "incl_distr"])
    return cmds


def killall_on_targets(
    destinations, cd_dir, chain=False, cockroach=False, zookeeper=False
):
    cmds = compose_kill_cmds(chain=chain, cockroach=cockroach, zookeeper=zookeeper)
    for cmd in cmds:
        print("Running kill commands in parallel...")
        procs = []
        for remote in destinations:
            procs.append(
                utils.proc.run_process_over_ssh(
                    remote,
                    cmd,
                    cd_dir=cd_dir,
                    capture_stdout=True,
                    capture_stderr=True,
                )
            )
        print("Waiting for command results...")
        utils.proc.wait_parallel_procs(procs)


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    parser.add_argument(
        "-t",
        "--targets",
        type=str,
        default="all",
        help="comma-separated remote hosts' nicknames, or 'all'",
    )
    parser.add_argument(
        "--chain", action="store_true", help="if set, kill ChainPaxos processes"
    )
    parser.add_argument(
        "--cockroach", action="store_true", help="if set, kill CockroachDB processes"
    )
    parser.add_argument(
        "--zookeeper", action="store_true", help="if set, kill ZooKeeper processes"
    )
    args = parser.parse_args()

    base, repo, _, remotes, _, _ = utils.config.parse_toml_file(
        TOML_FILENAME, args.group
    )

    targets = utils.config.parse_comma_separated(args.targets)
    destinations = []
    if args.targets == "all":
        destinations = list(remotes.values())
    else:
        for target in targets:
            if target not in remotes:
                raise ValueError(f"nickname '{target}' not found in toml file")
            destinations.append(remotes[target])
    if len(destinations) == 0:
        raise ValueError(f"targets list is empty")

    killall_on_targets(
        destinations, f"{base}/{repo}", args.chain, args.cockroach, args.zookeeper
    )
