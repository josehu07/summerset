import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "1dc"

YCSB_DIR = lambda base: f"{base}/ycsb"
YCSB_TRACE = "/tmp/ycsb_workloada.txt"


def gen_ycsb_a_trace(base):
    cmd = [
        f"{YCSB_DIR(base)}/bin/ycsb.sh",
        "run",
        "basic",
        "-P",
        f"{YCSB_DIR(base)}/workloads/workloada",
    ]
    proc = utils.proc.run_process(
        cmd, capture_stdout=True, capture_stderr=True, print_cmd=False
    )
    out, _ = proc.communicate()
    raw = out.decode()

    # clean the trace
    # TODO: write sampled value size into the trace itself?
    with open(YCSB_TRACE, "w+") as fout:
        for line in raw.strip().split("\n"):
            line = line.strip()
            if line.startswith("READ ") or line.startswith("UPDATE "):
                segs = line.split()
                op = segs[0]
                key = segs[2]
                fout.write(f"{op} {key}\n")


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    base, _, _, _, _, _ = utils.config.parse_toml_file(TOML_FILENAME, PHYS_ENV_GROUP)

    print("Generating YCSB-A trace...")
    if os.path.isfile(YCSB_TRACE):
        print(f"  {YCSB_TRACE} already there, skipped")
    else:
        gen_ycsb_a_trace(base)
        print(f"  Done: {YCSB_TRACE}")
