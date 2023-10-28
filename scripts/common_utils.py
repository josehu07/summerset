import sys
import os
import subprocess
import statistics


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


def set_tcp_buf_sizes():
    print("Setting TCP buffer sizes...")
    cmd = ["sudo", "./scripts/set_tcp_bufs.sh"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def set_tc_qdisc_netem(mean, jitter, rate, distribution="pareto"):
    os.system(
        f"sudo tc qdisc replace dev lo root netem limit 100000000 "
        f"delay {mean}ms {jitter}ms distribution {distribution} "
        f"rate {rate}gibit 10"
    )


def clear_tc_qdisc_netem():
    os.system("sudo tc qdisc delete dev lo root")


def gather_outputs(protocol, num_clients, path_prefix, tb, te, tgap):
    outputs = dict()
    for c in range(num_clients):
        outputs[c] = {"time": [], "tput": [], "lat": []}
        with open(f"{path_prefix}/{protocol}.{c}.out", "r") as fout:
            started = False
            for line in fout:
                line = line.strip()
                if not started and line.startswith("Elapsed"):
                    started = True
                elif started and len(line) > 0:
                    segs = line.split()
                    outputs[c]["time"].append(float(segs[0]))
                    outputs[c]["tput"].append(float(segs[2]))
                    outputs[c]["lat"].append(float(segs[4]))

    result = {
        "time": [],
        "tput_sum": [],
        "tput_min": [],
        "tput_max": [],
        "tput_avg": [],
        "tput_stdev": [],
        "lat_min": [],
        "lat_max": [],
        "lat_avg": [],
        "lat_stdev": [],
    }
    t = 0
    cidxs = [0 for _ in range(num_clients)]
    while t + tb < te:
        tputs, lats = [], []
        for c in range(num_clients):
            while t + tb > outputs[c]["time"][cidxs[c]]:
                cidxs[c] += 1
            tputs.append(outputs[c]["tput"][cidxs[c]])
            lats.append(outputs[c]["lat"][cidxs[c]])
        result["time"].append(t)
        result["tput_sum"].append(sum(tputs))
        result["tput_min"].append(min(tputs))
        result["tput_max"].append(max(tputs))
        result["tput_avg"].append(sum(tputs) / len(tputs))
        result["tput_stdev"].append(statistics.stdev(tputs))
        result["lat_min"].append(min(lats))
        result["lat_max"].append(max(lats))
        result["lat_avg"].append(sum(lats) / len(lats))
        result["lat_stdev"].append(statistics.stdev(lats))
        t += tgap

    return result


def list_smoothing(l, d, p):
    assert d > 0
    l = l.copy()

    if p > 0:
        for i in range(p, len(l) - p):
            lp = any(map(lambda t: 2 * t < l[i], l[i - p : i]))
            rp = any(map(lambda t: 2 * t < l[i], l[i + 1 : i + p + 1]))
            if lp and rp:
                l[i] = min(l[i - p : i + p + 1])

    result = []
    for i in range(len(l)):
        nums = []
        for j in range(i - d, i + d + 1):
            if j >= 0 and j < len(l):
                nums.append(l[j])
        result.append(sum(nums) / len(nums))

    return result
