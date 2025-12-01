import os
import argparse

from .. import utils


PHYS_ENV_GROUP = "reg"

YCSB_DIR = lambda base: f"{base}/ycsb"

YCSB_WORKLOAD = lambda w: f"workloads/workload{w}"
YCSB_OUTPUT = lambda w, d, p: f"/tmp/ycsb/workload{w}.{d}.{p}.cleaned.txt"
YCSB_MAPPED = lambda w, d, p: f"/tmp/ycsb/workload{w}.{d}.{p}.rmapped.txt"

KEY_LEN = 8  # for read_mapped option use


def gen_ycsb_trace(
    base, phase, workload, num_keys, ops_cnt, distribution, trace_out
):
    cmd = [
        f"{YCSB_DIR(base)}/bin/ycsb.sh",
        phase,
        "basic",
        "-P",
        f"{YCSB_DIR(base)}/{YCSB_WORKLOAD(workload)}",
        "-p",
        f"recordcount={num_keys}",
        "-p",
        f"operationcount={ops_cnt}",
        "-p",
        f"requestdistribution={distribution}",
    ]
    proc = utils.proc.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )
    out, _ = proc.communicate()
    raw = out.decode()

    # clean the trace
    with open(trace_out, "w+") as fout:
        for line in raw.strip().split("\n"):
            line = line.strip()
            if (
                line.startswith("READ ")
                or line.startswith("SCAN ")
                or line.startswith("UPDATE ")
                or line.startswith("INSERT ")
            ):
                segs = line.split()
                op = segs[0]
                key = segs[2]
                if op == "READ" or op == "SCAN":
                    fout.write(f"{op} {key}\n")
                else:
                    vlen = sum(len(field) for field in segs[4:-1])
                    fout.write(f"{op} {key} {vlen}\n")


def map_ycsb_trace(trace_file, mapped_file, key_map, reader_loc):
    assert isinstance(key_map, dict)

    # read original trace and count read frequency
    key_cnt = dict()
    with open(trace_file, "r") as ftrace:
        for line in ftrace:
            line = line.strip()
            if line.startswith("READ") or line.startswith("SCAN"):
                key = line.split()[1]
                if key not in key_cnt:
                    key_cnt[key] = 1
                else:
                    key_cnt[key] += 1
            elif len(line) > 0:
                key = line.split()[1]
                if key not in key_cnt:
                    key_cnt[key] = 0
    sorted_keys = list(
        sorted(key_cnt.keys(), key=lambda k: key_cnt[k], reverse=True)
    )

    if len(key_map) == 0:
        sorted_cnts = sorted(key_cnt.values(), reverse=True)
        print("  top 10 cnts: ", end="")
        for i in range(min(10, len(sorted_cnts))):
            print(f" {sorted_cnts[i]}", end="")
        print()

    # map to keys of format 'k<number>' with increasing number; if the key is
    # already in current key_map, then keep that instead
    base_num = len(key_map)
    for key in sorted_keys:
        if key not in key_map:
            mapped_key = f"k{reader_loc}{base_num:0{KEY_LEN - 2}d}"
            key_map[key] = mapped_key

            base_num += 1
            if base_num >= 1000000:
                raise RuntimeError("too many keys, exceeding 1M")

    with open(trace_file, "r") as ftrace, open(mapped_file, "w+") as fmapped:
        for line in ftrace:
            line = line.strip()
            if len(line) > 0:
                segs = line.split()
                segs[1] = key_map[segs[1]]
                fmapped.write(" ".join(segs) + "\n")


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-w",
        "--workload",
        type=str,
        required=True,
        help="workload name (letter in a-f)",
    )
    parser.add_argument(
        "-k",
        "--num_keys",
        type=int,
        default=10000,
        help="number of keys (i.e., records)",
    )
    parser.add_argument(
        "-c",
        "--ops_cnt",
        type=int,
        default=100000,
        help="number of operations to generate",
    )
    parser.add_argument(
        "-d",
        "--distribution",
        type=str,
        default="zipfian",
        help="request distribution type",
    )
    parser.add_argument(
        "-r",
        "--read_mapped",
        type=int,
        default=-1,
        help="if non-negative, map keys to range-enabled keys based on read frequency",
    )
    args = parser.parse_args()

    base, _, _, _, _, _ = utils.config.parse_toml_file(PHYS_ENV_GROUP)

    workload = args.workload.lower()
    if (
        len(workload) != 1
        or ord(workload) < ord("a")
        or ord(workload) > ord("f")
    ):
        raise ValueError(f"invalid workload name: {workload}")

    if not os.path.isdir("/tmp/ycsb"):
        os.system("mkdir -p /tmp/ycsb")

    if args.num_keys == 0:
        raise ValueError("invalid num_keys: '{}'", args.num_keys)
    if args.ops_cnt == 0:
        raise ValueError("invalid ops_cnt: '{}'", args.ops_cnt)
    if args.distribution not in {"uniform", "zipfian"}:
        raise ValueError("invalid distribution: '{}'", args.distribution)
    if args.read_mapped >= 0 and args.read_mapped > 9:
        # this value denotes the location of readers, must be in [0, 9]
        raise ValueError("invalid read_mapped: '{}'", args.read_mapped)

    print(f"Generating YCSB-{workload} traces...")
    trace_load = YCSB_OUTPUT(workload, args.distribution, "load")
    trace_run = YCSB_OUTPUT(workload, args.distribution, "run")
    if os.path.isfile(trace_load) and os.path.isfile(trace_run):
        print("  load & run both already there, skipped")
    else:
        for phase in ("load", "run"):
            gen_ycsb_trace(
                base,
                phase,
                workload,
                args.num_keys,
                args.ops_cnt,
                args.distribution,
                trace_load if phase == "load" else trace_run,
            )
            print(f"  {phase}: {trace_load if phase == 'load' else trace_run}")

    # if preparing for read leases...
    if args.read_mapped >= 0:
        print("Mapping traces to range-enabled keys...")
        mapped_load = YCSB_MAPPED(workload, args.distribution, "load")
        mapped_run = YCSB_MAPPED(workload, args.distribution, "run")
        if os.path.isfile(mapped_load) and os.path.isfile(mapped_run):
            print("  load & run both already there, skipped")
        else:
            key_map = dict()
            for phase in ("run", "load"):
                # map 'run' first to honor frequency counting
                map_ycsb_trace(
                    trace_load if phase == "load" else trace_run,
                    mapped_load if phase == "load" else mapped_run,
                    key_map,
                    args.read_mapped,
                )
                print(
                    f"  {phase}: {mapped_load if phase == 'load' else mapped_run}"
                )


if __name__ == "__main__":
    main()
