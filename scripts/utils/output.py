import statistics
import random


def gather_outputs(
    protocol_with_midfix,
    num_clients,
    path_prefix,
    tb,
    te,
    tgap,
    partition=None,
    agg_partitions=None,
    client_start=0,
    client_step=1,
    client_skips=None,
):
    """
    NOTE: latency value of 0.0 means no data!
    """
    assert not (agg_partitions is not None and partition is not None)
    if partition is not None:
        protocol_with_midfix += f".{partition}"

    clients, outputs = [], dict()
    for c in range(client_start, num_clients, client_step):
        if client_skips is not None:
            skip_idxs, skip_every = client_skips
            if c % skip_every in skip_idxs:
                continue
        clients.append(c)

        outputs[c] = {"time": [], "tput": [], "lat": [], "wlat": [], "rlat": []}
        output_path = f"{path_prefix}/{protocol_with_midfix}.{c}.out"
        if agg_partitions is not None:
            p, local_c = c % agg_partitions, c // agg_partitions
            output_path = (
                f"{path_prefix}/{protocol_with_midfix}.{p}.{local_c}.out"
            )
        with open(output_path, "r") as fout:
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
                    outputs[c]["wlat"].append(float(segs[6]))
                    outputs[c]["rlat"].append(float(segs[8]))

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
        "wlat_min": [],
        "wlat_max": [],
        "wlat_avg": [],
        "wlat_stdev": [],
        "rlat_min": [],
        "rlat_max": [],
        "rlat_avg": [],
        "rlat_stdev": [],
    }
    t = 0
    cidxs = {c: 0 for c in clients}
    while t + tb < te:
        tputs, lats, wlats, rlats = [], [], [], []
        for c in clients:
            while (
                cidxs[c] < len(outputs[c]["time"]) - 1
                and t + tb > outputs[c]["time"][cidxs[c]]
            ):
                cidxs[c] += 1
            if outputs[c]["time"][cidxs[c]] > t + tb + 1.0:  # conservative
                tputs.append(0.0)
                lats.append(0.0)
                wlats.append(0.0)
                rlats.append(0.0)
            else:
                tputs.append(outputs[c]["tput"][cidxs[c]])
                lats.append(outputs[c]["lat"][cidxs[c]])
                wlats.append(outputs[c]["wlat"][cidxs[c]])
                rlats.append(outputs[c]["rlat"][cidxs[c]])
        lats = [l for l in lats if l != 0.0]  # lat 0.0 means no data
        wlats = [l for l in wlats if l != 0.0]
        rlats = [l for l in rlats if l != 0.0]
        result["time"].append(t)
        result["tput_sum"].append(sum(tputs))
        result["tput_min"].append(min(tputs))
        result["tput_max"].append(max(tputs))
        result["tput_avg"].append(sum(tputs) / len(tputs))
        result["tput_stdev"].append(
            statistics.stdev(tputs) if len(tputs) > 1 else 0.0
        )
        result["lat_min"].append(min(lats) if len(lats) > 0 else 0.0)
        result["lat_max"].append(max(lats) if len(lats) > 0 else 0.0)
        result["lat_avg"].append(
            sum(lats) / len(lats) if len(lats) > 0 else 0.0
        )
        result["lat_stdev"].append(
            statistics.stdev(lats) if len(lats) > 1 else 0.0
        )
        result["wlat_min"].append(min(wlats) if len(wlats) > 0 else 0.0)
        result["wlat_max"].append(max(wlats) if len(wlats) > 0 else 0.0)
        result["wlat_avg"].append(
            sum(wlats) / len(wlats) if len(wlats) > 0 else 0.0
        )
        result["wlat_stdev"].append(
            statistics.stdev(wlats) if len(wlats) > 1 else 0.0
        )
        result["rlat_min"].append(min(rlats) if len(rlats) > 0 else 0.0)
        result["rlat_max"].append(max(rlats) if len(rlats) > 0 else 0.0)
        result["rlat_avg"].append(
            sum(rlats) / len(rlats) if len(rlats) > 0 else 0.0
        )
        result["rlat_stdev"].append(
            statistics.stdev(rlats) if len(rlats) > 1 else 0.0
        )
        t += tgap

    return result


def gather_ycsb_outputs(
    protocol_with_midfix, path_prefix, tb, te, partition=None
):
    if partition is not None:
        protocol_with_midfix += f".{partition}"

    tputs, tput_stdevs, lats, lat_stdevs = [], [], [], []
    with open(f"{path_prefix}/{protocol_with_midfix}.out", "r") as fout:
        for line in fout:
            if "current ops/sec" in line:
                tput = float(
                    line[
                        line.find("operations; ") + 12 : line.find(
                            "current ops/sec"
                        )
                    ]
                )
                tput_stdev = 0.0

                line = line[line.find("[UPDATE:") :]
                lat = (
                    float(line[line.find("Avg=") + 4 : line.find(", 90=")])
                    / 1000.0
                )
                lat_90p = (
                    float(line[line.find("90=") + 3 : line.find(", 99=")])
                    / 1000.0
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
            "stdev": (sum(map(lambda s: s**2, tput_stdevs)) / len(tput_stdevs))
            ** 0.5,
        },
        "lat": {
            "mean": sum(lats) / len(lats),
            "stdev": (sum(map(lambda s: s**2, lat_stdevs)) / len(lat_stdevs))
            ** 0.5,
        },
    }


def gather_tpcc_outputs(protocol_with_midfix, path_prefix, partition=None):
    if partition is not None:
        protocol_with_midfix += f".{partition}"

    results = {
        "delivery": dict(),
        "newOrder": dict(),
        "orderStatus": dict(),
        "payment": dict(),
        "stockLevel": dict(),
        "aggregate": dict(),
    }
    with open(f"{path_prefix}/{protocol_with_midfix}.run", "r") as fout:
        in_txn_sum_sec, in_agg_sum_sec = False, False
        for line in fout:
            if "ops(total)" in line:
                if line.strip().endswith("_result"):
                    in_agg_sum_sec = True
                else:
                    in_txn_sum_sec = True

            elif in_txn_sum_sec or in_agg_sum_sec:
                segs = line.strip().split()

                txn_type = "aggregate"
                if not in_agg_sum_sec:
                    txn_type = segs[-1]

                result = dict()
                result["txns"] = int(segs[2])
                result["errors"] = int(segs[1])
                result["tput"] = float(segs[3])
                result["lat_avg"] = float(segs[4])
                result["lat_p50"] = float(segs[5])
                result["lat_p95"] = float(segs[6])
                result["lat_p99"] = float(segs[7])
                results[txn_type] = result

                in_txn_sum_sec = False
                in_agg_sum_sec = False

    return results


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
            rp = next(
                filter(lambda t: 2 * t < slc[i], slc[i + 1 : i + p + 1]), None
            )
            if lp is not None and rp is not None:
                sl[i] = min(slc[i - p : i + p + 1])

    # removing jittering dips
    if j > 0:
        slc = sl.copy()
        for i in range(j, len(slc) - j):
            lj = next(
                filter(lambda t: t > slc[i] and t < 2 * slc[i], slc[i - j : i]),
                None,
            )
            rj = next(
                filter(
                    lambda t: t > slc[i] and t < 2 * slc[i],
                    slc[i + 1 : i + j + 1],
                ),
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
