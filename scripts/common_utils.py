import sys
import os
import subprocess
import statistics
import random
import multiprocessing


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


def remove_files_in_dir(path):
    if not os.path.isdir(path):
        raise RuntimeError(f"{path} is not a directory")
    for name in os.listdir(path):
        child = os.path.join(path, name)
        if not os.path.isfile(child):
            raise RuntimeError(f"{child} is not a regular file")
        os.unlink(child)


def check_enough_cpus():
    EXPECTED_CPUS = 40
    cpus = multiprocessing.cpu_count()
    if cpus < EXPECTED_CPUS:
        print(
            f"WARN: benchmarking scripts expect >= {EXPECTED_CPUS} CPUs, found {cpus}"
        )


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
    cmd = "sudo ./scripts/kill_local_procs.sh"
    os.system(cmd)


def clear_fs_cache():
    cmd = 'sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"'
    os.system(cmd)


def run_process(
    cmd,
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

    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    return proc


def run_process_over_ssh(remote, cmd, print_cmd=True, cpu_list=None):
    if cpu_list is not None and "-" in cpu_list:
        cmd = ["sudo", "taskset", "-c", cpu_list] + cmd

    if print_cmd:
        print(f"Run on {remote}: {' '.join(cmd)}")

    cmd = ["ssh", remote] + cmd
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def set_tc_qdisc_netem(netns, dev, mean, jitter, rate, distribution="pareto"):
    QLEN_LIMIT = 500000000
    delay_args = f"delay {mean}ms" if mean > 0 else ""
    jitter_args = (
        f"{jitter}ms distribution {distribution}" if mean > 0 and jitter > 0 else ""
    )
    rate_args = f"rate {rate}gibit" if rate > 0 else ""
    os.system(
        f"sudo ip netns exec {netns} tc qdisc replace dev {dev} root netem"
        f" limit {QLEN_LIMIT} {delay_args} {jitter_args} {rate_args}"
    )


def set_all_tc_qdisc_netems(
    num_replicas,
    netns,
    dev,
    ifb,
    mean,
    jitter,
    rate,
    distribution="pareto",
    involve_ifb=False,
):
    for replica in range(num_replicas):
        set_tc_qdisc_netem(
            netns(replica),
            dev(replica),
            mean(replica),
            jitter(replica),
            rate(replica) * 2 if involve_ifb else rate(replica),
            distribution=distribution,
        )
        set_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
            0,
            0,
            rate(replica) * 2 if involve_ifb else 0,
        )


def clear_tc_qdisc_netem(netns, dev):
    os.system(f"sudo ip netns exec {netns} tc qdisc delete dev {dev} root")


def clear_all_tc_qdisc_netems(num_replicas, netns, dev, ifb):
    for replica in range(num_replicas):
        clear_tc_qdisc_netem(
            netns(replica),
            dev(replica),
        )
        clear_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
        )


def gather_outputs(protocol_with_midfix, num_clients, path_prefix, tb, te, tgap):
    outputs = dict()
    for c in range(num_clients):
        outputs[c] = {"time": [], "tput": [], "lat": []}
        with open(f"{path_prefix}/{protocol_with_midfix}.{c}.out", "r") as fout:
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


def list_smoothing(l, d, p, j, m):
    assert d > 0

    # sliding window average
    sl = []
    for i in range(len(l)):
        nums = []
        for k in range(i - d, i + d + 1):
            if k >= 0 and k < len(l):
                nums.append(l[k])
        sl.append(sum(nums) / len(nums))

    # removing ghost spikes
    if p > 0:
        slc = sl.copy()
        for i in range(p, len(slc) - p):
            lp = next(filter(lambda t: 2 * t < slc[i], slc[i - p : i]), None)
            rp = next(filter(lambda t: 2 * t < slc[i], slc[i + 1 : i + p + 1]), None)
            if lp is not None and rp is not None:
                sl[i] = min(slc[i - p : i + p + 1])

    # removing jittering dips
    if j > 0:
        slc = sl.copy()
        for i in range(j, len(slc) - j):
            lj = next(
                filter(lambda t: t > slc[i] and t < 2 * slc[i], slc[i - j : i]), None
            )
            rj = next(
                filter(lambda t: t > slc[i] and t < 2 * slc[i], slc[i + 1 : i + j + 1]),
                None,
            )
            if lj is not None and rj is not None:
                sl[i] = max(lj, rj)

    # 2nd sliding window average
    slc = sl.copy()
    for i in range(len(slc)):
        nums = []
        for k in range(i - d // 2, i + d // 2 + 1):
            if k >= 0 and k < len(slc):
                nums.append(slc[k])
        sl[i] = sum(nums) / len(nums)

    # compensation scaling
    sl = [x * m for x in sl]

    return sl


def list_capping(l1, l2, d, down=True):
    l1c = l1.copy()

    # height capping
    for i, n in enumerate(l1):
        if down and n > 1.05 * l2[i]:
            nums = []
            for k in range(i - d, i + d + 1):
                if k >= 0 and k < len(l2):
                    nums.append(l2[k])
            l1c[i] = random.choice(nums)
        elif not down and n < 1.05 * l2[i]:
            nums = []
            for k in range(i - d, i + d + 1):
                if k >= 0 and k < len(l2):
                    nums.append(l2[k])
            l1c[i] = random.choice(nums)

    # sliding window average
    sl = []
    for i in range(len(l1c)):
        nums = []
        for k in range(i - int(d * 1.5), i + int(d * 1.5) + 1):
            if k >= 0 and k < len(l1c):
                nums.append(l1c[k])
        sl.append(sum(nums) / len(nums))

    return sl


def read_toml_file(filename):
    import toml  # type: ignore

    return toml.load(filename)


def split_remote_string(remote):
    if "@" not in remote:
        raise ValueError(f"invalid remote string '{remote}'")
    segs = remote.strip().split("@")
    if len(segs) != 2 or len(segs[0]) == 0 or len(segs[1]) == 0:
        raise ValueError(f"invalid remote string '{remote}'")
    return segs[0], segs[1]
