import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"

EXCLUDE_NAMES = [
    "results/",
    "backups/",
    "target/",
    ".git/",
    ".vscode/",
    "tla+/*/states/",
    "*/__pycache__/",
]


def compose_rsync_cmd(src_path, dst_path, remote):
    rsync_cmd = ["rsync", "-aP", "--delete"]
    for name in EXCLUDE_NAMES:
        rsync_cmd += ["--exclude", f"{name}"]
    rsync_cmd += [src_path, f"{remote}:{dst_path}"]
    return rsync_cmd


def mirror_folder(remotes, src_path, dst_path, repo_name, sequential):
    """WARNING: this deletes all unmatched files on remote!"""
    # source path must exist, end with the correct folder name, followed by a trailing '/'
    if not os.path.isdir(src_path):
        raise ValueError(f"source path '{src_path}' is not an existing directory")
    src_path = os.path.realpath(src_path)
    src_seg = utils.file.path_get_last_segment(src_path)
    if not repo_name in src_seg:
        raise ValueError(
            f"source folder '{src_seg}' does not contain project name '{repo_name}'"
        )
    if not src_path.endswith("/"):
        src_path += "/"

    # target path must be an absolute path for safety, end with the correct folder name,
    # followed by a trailing '/'
    if not os.path.isabs(dst_path):
        raise ValueError(f"target path '{dst_path}' is not an absolute path")
    dst_seg = utils.file.path_get_last_segment(dst_path)
    if dst_seg != repo_name:
        raise ValueError(
            f"target folder '{dst_seg}' does not match project name '{repo_name}'"
        )
    if not dst_path.endswith("/"):
        dst_path += "/"

    # compose rsync commands
    cmds = []
    for remote in remotes:
        cmd = compose_rsync_cmd(src_path, dst_path, remote)
        cmds.append(cmd)

    # execute
    if sequential:
        for cmd in cmds:
            proc = utils.proc.run_process(cmd)
            proc.wait()
    else:
        print("Running rsync commands in parallel...")
        procs = []
        for cmd in cmds:
            procs.append(
                utils.proc.run_process(cmd, capture_stdout=True, capture_stderr=True)
            )
        print("Waiting for command results...")
        utils.proc.wait_parallel_procs(procs)


def compose_build_cmd(release):
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    cmd += ["--features", "rse-simd"]
    return cmd


def build_on_targets(destinations, dst_path, release, sequential):
    # compose build command over SSH
    cmd = compose_build_cmd(release)

    # execute
    if sequential:
        for remote in destinations:
            proc = utils.proc.run_process_over_ssh(remote, cmd, cd_dir=dst_path)
            proc.wait()
    else:
        print("Running build commands in parallel...")
        procs = []
        for remote in destinations:
            procs.append(
                utils.proc.run_process_over_ssh(
                    remote,
                    # ugly hack to opt-out AVX2 instructions dependency
                    # on UMass datacenter machines
                    cmd if "mass" not in remote else cmd[:-2],
                    cd_dir=dst_path,
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
    parser.add_argument(
        "-b",
        "--build",
        action="store_true",
        help="if set, run cargo build on all nodes",
    )
    parser.add_argument(
        "-r", "--release", action="store_true", help="if set, build in release mode"
    )
    parser.add_argument(
        "-s",
        "--sequential",
        action="store_true",
        help="if set, run for hosts sequentially",
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

    SRC_PATH = "./"
    DST_PATH = f"{base}/{repo}"

    mirror_folder(destinations, SRC_PATH, DST_PATH, repo, args.sequential)

    if args.build:
        build_on_targets(destinations, DST_PATH, args.release, args.sequential)
