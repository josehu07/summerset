import sys
import os
import subprocess


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


def remove_files_in_dir(path):
    if not os.path.isdir(path):
        raise RuntimeError(f"{path} is not a directory")
    for name in os.listdir(path):
        child = os.path.join(path, name)
        if not os.path.isfile(child):
            raise RuntimeError(f"{child} is not a regular file")
        os.unlink(child)


def do_cargo_build(release):
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace"]
    if release:
        cmd.append("-r")
    proc = subprocess.Popen(cmd)
    return proc.wait()
