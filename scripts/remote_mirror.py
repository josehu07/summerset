import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"

BASE_PATH = "/mnt/eval"

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


def mirror_folder(remotes, src_path, dst_path, repo_name):
    """WARNING: this deletes all unmatched files on remote!"""
    # source path must exist, end with the correct folder name, followed by a trailing '/'
    if not os.path.isdir(src_path):
        raise ValueError(f"source path '{src_path}' is not an existing directory")
    src_path = os.path.realpath(src_path)
    src_seg = utils.path_get_last_segment(src_path)
    if src_seg != repo_name:
        raise ValueError(
            f"source folder '{src_seg}' does not match project name '{repo_name}'"
        )
    if not src_path.endswith("/"):
        src_path += "/"

    # target path must be an absolute path for safety, end with the correct folder name,
    # followed by a trailing '/'
    if not os.path.isabs(dst_path):
        raise ValueError(f"target path '{dst_path}' is not an absolute path")
    dst_seg = utils.path_get_last_segment(dst_path)
    if dst_seg != repo_name:
        raise ValueError(
            f"target folder '{dst_seg}' does not match project name '{repo_name}'"
        )
    if not dst_path.endswith("/"):
        dst_path += "/"

    # compose rsync commands and execute
    cmds = []
    for remote in remotes:
        cmd = compose_rsync_cmd(src_path, dst_path, remote)
        cmds.append(cmd)

    for cmd in cmds:
        proc = utils.run_process(cmd)
        proc.wait()


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-t",
        "--targets",
        type=str,
        required=True,
        help="comma-separated remote hosts' nicknames",
    )
    args = parser.parse_args()

    hosts_config = utils.read_toml_file(TOML_FILENAME)
    repo = hosts_config["repo_name"]
    hosts = hosts_config["hosts"]

    targets = utils.parse_comma_separated(args.targets)
    remotes = []
    for target in targets:
        if target not in hosts:
            raise ValueError(f"nickname '{target}' not found in toml file")
        remotes.append(hosts[target])

    SRC_PATH = "./"
    DST_PATH = f"{BASE_PATH}/{repo}"

    mirror_folder(remotes, SRC_PATH, DST_PATH, repo)
