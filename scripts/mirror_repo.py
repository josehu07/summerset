import os
import sys
import argparse
import subprocess


BASE_PATH = "/mnt/eval"

EXCLUDE_NAMES = ["results/", "target/", ".git/", "tla+/*/states/"]


def run_process(cmd):
    print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd)
    return proc


def parse_comma_separated(l):
    l = l.strip().split(",")
    if len(l) == 0:
        raise ValueError("comma separated list is empty")
    for seg in l:
        if len(seg) == 0:
            raise ValueError(f"comma separated list has empty segment: {l}")
    return l


def path_get_last_segment(path):
    if "/" not in path:
        return None
    eidx = len(path) - 1
    while eidx > 0 and path[eidx] == "/":
        eidx -= 1
    bidx = path[:eidx].rfind("/")
    bidx += 1
    return path[bidx : eidx + 1]


def check_proper_cwd():
    cwd = os.getcwd()
    if "summerset" not in path_get_last_segment(cwd) or not os.path.isdir("scripts/"):
        print(
            "ERROR: script must be run under top-level repo with `python3 scripts/<script>.py ...`"
        )
        sys.exit(1)


def compose_rsync_cmd(src_path, dst_path, target):
    rsync_cmd = ["rsync", "-aP", "--delete"]
    for name in EXCLUDE_NAMES:
        rsync_cmd += ["--exclude", f"{name}"]
    rsync_cmd += [src_path, f"{target}:{dst_path}"]
    return rsync_cmd


def mirror_folder(targets, src_path, dst_path, repo_name):
    """WARNING: this deletes all unmatched files on remote!"""
    # source path must exist, end with the correct folder name, followed by a trailing '/'
    if not os.path.isdir(src_path):
        raise ValueError(f"source path '{src_path}' is not an existing directory")
    src_path = os.path.realpath(src_path)
    src_seg = path_get_last_segment(src_path)
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
    dst_seg = path_get_last_segment(dst_path)
    if dst_seg != repo_name:
        raise ValueError(
            f"target folder '{dst_seg}' does not match project name '{repo_name}'"
        )
    if not dst_path.endswith("/"):
        dst_path += "/"

    # compose rsync commands and execute
    cmds = []
    for target in targets:
        cmd = compose_rsync_cmd(src_path, dst_path, target)
        cmds.append(cmd)

    for cmd in cmds:
        proc = run_process(cmd)
        proc.wait()


if __name__ == "__main__":
    check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-t",
        "--targets",
        type=str,
        required=True,
        help="comma-separated remote machines hostnames",
    )
    parser.add_argument(
        "-s", "--src_path", type=str, default="./", help="src folder path on local host"
    )
    parser.add_argument(
        "-d",
        "--dst_path",
        type=str,
        default=f"{BASE_PATH}/summerset-private",
        help="dst folder absolute path on remote",
    )
    parser.add_argument(
        "-r",
        "--repo_name",
        type=str,
        default="summerset-private",
        help="GitHub repo name",
    )
    args = parser.parse_args()

    targets = parse_comma_separated(args.targets)
    mirror_folder(targets, args.src_path, args.dst_path, args.repo_name)
