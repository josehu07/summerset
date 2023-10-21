import sys
import os
import subprocess


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


def do_cargo_build(release):
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    proc = subprocess.Popen(cmd)
    return proc.wait()


def kill_all_matching(name):
    print("Kill all:", name)
    assert name.count(" ") == 0
    cmd = f"killall -9 {name} > /dev/null 2>&1"
    os.system(cmd)


def kill_all_local_procs():
    # print("Killing all local procs...")
    cmd = ["sudo", "./scripts/kill_local_procs.sh"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd, capture_stdout=False, capture_stderr=False, print_cmd=True):
    if print_cmd:
        print("Run:", " ".join(cmd))
    stdout, stderr = None, None
    if capture_stdout:
        stdout = subprocess.PIPE
    if capture_stderr:
        stderr = subprocess.PIPE
    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    return proc
