import statistics
import random


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


def parse_ycsb_log(protocol_with_midfix, path_prefix, tb, te):
    tputs, tput_stdevs, lats, lat_stdevs = [], [], [], []
    with open(f"{path_prefix}/{protocol_with_midfix}.out", "r") as fout:
        for line in fout:
            if "current ops/sec" in line:
                tput = float(
                    line[line.find("operations; ") + 12 : line.find("current ops/sec")]
                )
                tput_stdev = 0.0

                line = line[line.find("[UPDATE:") :]
                lat = float(line[line.find("Avg=") + 4 : line.find(", 90=")]) / 1000.0
                lat_90p = (
                    float(line[line.find("90=") + 3 : line.find(", 99=")]) / 1000.0
                )
                lat_stdev = (lat_90p - lat) / 4

                tputs.append(tput)
                tput_stdevs.append(tput_stdev)
                lats.append(lat)
                lat_stdevs.append(lat_stdev)

    if len(tputs) <= tb + te:
        raise ValueError(f"YCSB log too short to exclude tb {tb} te {te}")
    tputs = tputs[tb:-te]
    tput_stdevs = tput_stdevs[tb:-te]
    lats = lats[tb:-te]
    lat_stdevs = lat_stdevs[tb:-te]

    return {
        "tput": {
            "mean": sum(tputs) / len(tputs),
            "stdev": (sum(map(lambda s: s**2, tput_stdevs)) / len(tput_stdevs)) ** 0.5,
        },
        "lat": {
            "mean": sum(lats) / len(lats),
            "stdev": (sum(map(lambda s: s**2, lat_stdevs)) / len(lat_stdevs)) ** 0.5,
        },
    }


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
