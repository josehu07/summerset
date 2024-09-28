import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "reg"

YCSB_DIR = lambda base: f"{base}/ycsb"

YCSB_WLOAD = lambda w: f"workloads/workload{w}"
YCSB_TRACE = lambda w: f"/tmp/ycsb/workload{w}.txt"


def gen_ycsb_trace(base, workload, trace_out):
    cmd = [
        f"{YCSB_DIR(base)}/bin/ycsb.sh",
        "run",
        "basic",
        "-P",
        f"{YCSB_DIR(base)}/{YCSB_WLOAD(workload)}",
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


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-w",
        "--workload",
        type=str,
        required=True,
        help="workload name (letter in a-f)",
    )
    args = parser.parse_args()

    base, _, _, _, _, _ = utils.config.parse_toml_file(TOML_FILENAME, PHYS_ENV_GROUP)

    workload = args.workload.lower()
    if len(workload) != 1 or ord(workload) < ord("a") or ord(workload) > ord("f"):
        raise ValueError(f"invalid workload name: {workload}")

    if not os.path.isdir("/tmp/ycsb"):
        os.system("mkdir -p /tmp/ycsb")

    print(f"Generating YCSB-{workload} trace...")
    trace_file = YCSB_TRACE(workload)
    if os.path.isfile(trace_file):
        print(f"  {trace_file} already there, skipped")
    else:
        gen_ycsb_trace(base, workload, trace_file)
        print(f"  Done: {trace_file}")
