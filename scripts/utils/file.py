import sys
import os
import subprocess

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from proc import run_process, run_process_over_ssh, wait_parallel_procs


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
        print("ERROR: script must be run under top-level repo!")
        print("       example: python3 scripts/<paper>/<script>.py")
        sys.exit(1)


def do_cargo_build(release, remotes=None):
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    if remotes is None:
        rc = subprocess.Popen(cmd).wait()
        if rc != 0:
            raise RuntimeError(f"cargo build failed")
    else:
        procs = []
        for host in remotes:
            procs.append(
                run_process_over_ssh(
                    remotes[host],
                    cmd,
                    capture_stdout=True,
                    capture_stderr=True,
                    print_cmd=False,
                )
            )
        wait_parallel_procs(procs, list(remotes.keys()), check_rc=True)


def clear_fs_caches(remotes=None):
    cmd = ["sudo", "bash", "-c", '"echo 3 > /proc/sys/vm/drop_caches"']
    if remotes is None:
        run_process(cmd).wait()
    else:
        procs = []
        for host in remotes:
            procs.append(
                run_process_over_ssh(
                    remotes[host],
                    cmd,
                    capture_stdout=True,
                    capture_stderr=True,
                    print_cmd=False,
                )
            )
        wait_parallel_procs(procs, list(remotes.keys()), check_rc=False)


def remove_files_in_dir(path, remotes=None):
    if remotes is None:
        if not os.path.isdir(path):
            raise RuntimeError(f"{path} is not a directory")
        for name in os.listdir(path):
            child = os.path.join(path, name)
            if not os.path.isfile(child):
                raise RuntimeError(f"{child} is not a regular file")
            os.unlink(child)
    else:
        cmd = ["find", path, "-type", "f", "-exec", "rm", "-f", "{}", "+"]
        procs = []
        for host in remotes:
            procs.append(
                run_process_over_ssh(
                    remotes[host],
                    cmd,
                    capture_stdout=True,
                    capture_stderr=True,
                    print_cmd=False,
                )
            )
        wait_parallel_procs(procs, list(remotes.keys()), check_rc=False)


def fetch_files_of_dir(remote, src_path, dst_path):
    if not os.path.isdir(dst_path):
        raise RuntimeError(f"{dst_path} is not a directory")
    cmd = ["scp", f"{remote}:{src_path}/*", f"{dst_path}/"]
    rc = run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    ).wait()
    if rc != 0:
        raise RuntimeError(f"failed to fetch {remote}:{src_path}/* into {dst_path}/")
