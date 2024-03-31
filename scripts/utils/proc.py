import os
import subprocess
import multiprocessing


def check_enough_cpus():
    EXPECTED_CPUS = 40
    cpus = multiprocessing.cpu_count()
    if cpus < EXPECTED_CPUS:
        print(
            f"WARN: benchmarking scripts expect >= {EXPECTED_CPUS} CPUs, found {cpus}"
        )


def kill_all_matching(name):
    print("Kill all:", name)
    assert name.count(" ") == 0
    cmd = f"sudo killall -9 {name} > /dev/null 2>&1"
    os.system(cmd)


def kill_all_local_procs():
    # print("Killing all local procs...")
    cmd = "./scripts/kill_local_procs.sh"
    os.system(cmd)


def run_process(
    cmd,
    cd_dir=None,
    capture_stdout=False,
    capture_stderr=False,
    print_cmd=True,
    cpu_list=None,
    in_netns=None,
):
    stdout, stderr = None, None
    if capture_stdout:
        stdout = subprocess.PIPE
    if capture_stderr:
        stderr = subprocess.PIPE

    if cpu_list is not None and "-" in cpu_list:
        cmd = ["sudo", "taskset", "-c", cpu_list] + cmd

    if in_netns is not None and len(in_netns) > 0:
        cmd = [s for s in cmd if s != "sudo"]
        cmd = ["sudo", "ip", "netns", "exec", in_netns] + cmd

    if print_cmd:
        print("Run:", " ".join(cmd))

    proc = subprocess.Popen(cmd, cwd=cd_dir, stdout=stdout, stderr=stderr)
    return proc


def run_process_over_ssh(
    remote,
    cmd,
    cd_dir=None,
    capture_stdout=False,
    capture_stderr=False,
    print_cmd=True,
    cpu_list=None,
):
    stdout, stderr = None, None
    if capture_stdout:
        stdout = subprocess.PIPE
    if capture_stderr:
        stderr = subprocess.PIPE

    if cpu_list is not None and "-" in cpu_list:
        cmd = ["sudo", "taskset", "-c", cpu_list] + cmd

    if print_cmd:
        print(f"Run on {remote}: {' '.join(cmd)}")

    # ugly hack to solve the quote parsing issue
    config_seg = False
    for i, seg in enumerate(cmd):
        if seg.startswith("--"):
            if seg.strip() == "--config":
                config_seg = True
            else:
                config_seg = False
        elif config_seg:
            new_seg = "\\'".join(seg.split("'"))
            cmd[i] = new_seg

    if cd_dir is None or len(cd_dir) == 0:
        cmd = ["ssh", remote, f". /etc/profile; {' '.join(cmd)}"]
    else:
        cmd = ["ssh", remote, f". /etc/profile; cd {cd_dir}; {' '.join(cmd)}"]

    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    return proc
