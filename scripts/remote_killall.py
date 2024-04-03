import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"


def killall_on_targets(destinations, cd_dir):
    print("Running kill commands in parallel...")
    procs = []
    for remote in destinations:
        procs.append(
            utils.proc.run_process_over_ssh(
                remote,
                ["./scripts/kill_all_procs.sh", "incl_distr"],
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
        "-g", "--group", type=str, default="1dc", help="hosts group to run on"
    )
    parser.add_argument(
        "-t",
        "--targets",
        type=str,
        default="all",
        help="comma-separated remote hosts' nicknames, or 'all'",
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

    killall_on_targets(destinations, f"{base}/{repo}")
