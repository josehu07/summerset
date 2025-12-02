import argparse

from . import utils


def execute_on_targets(destinations, dst_path, cmd, sequential):
    # convert cmd string to list
    cmd = cmd.strip().split()

    # execute over SSH
    if sequential:
        for remote in destinations:
            proc = utils.proc.run_process_over_ssh(remote, cmd, cd_dir=dst_path)
            proc.wait()
    else:
        print("Running on all hosts in parallel...")
        procs = []
        for remote in destinations:
            procs.append(
                utils.proc.run_process_over_ssh(
                    remote,
                    cmd,
                    cd_dir=dst_path,
                    capture_stdout=True,
                    capture_stderr=True,
                )
            )

        print("Waiting for command results...")
        utils.proc.wait_parallel_procs(procs)


def main():
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
        "-c",
        "--cmd",
        type=str,
        required=True,
        help="command to execute on remote hosts; for now,"
        " only supports a static string without quoted spaces",
    )
    parser.add_argument(
        "-s",
        "--sequential",
        action="store_true",
        help="if set, run for hosts sequentially",
    )
    args = parser.parse_args()

    base, repo, _, remotes, _, _ = utils.config.parse_toml_file(args.group)

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
        raise ValueError("targets list is empty")

    if args.cmd.strip() == "":
        raise ValueError("command to execute cannot be empty")

    DST_PATH = f"{base}/{repo}"
    execute_on_targets(destinations, DST_PATH, args.cmd, args.sequential)


if __name__ == "__main__":
    main()
