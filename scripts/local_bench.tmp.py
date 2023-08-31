import sys
import os
import subprocess
import statistics


PERF_STORAGE_ALPHA = 0
PERF_STORAGE_BETA = 0
PERF_NETWORK_ALPHA = 10000
PERF_NETWORK_BETA = 100


def do_cargo_build():
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace", "-r"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd):
    # print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return proc


def kill_all_matching(name, force=False):
    # print("Kill all:", name)
    assert name.count(" ") == 0
    cmd = "killall -9" if force else "killall"
    cmd += f" {name} > /dev/null 2>&1"
    os.system(cmd)


def launch_cluster(protocol, num_replicas, config):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
        "-r",
    ]
    if config is not None and len(config) > 0:
        cmd += ["--config", config]
    return run_process(cmd)


def wait_cluster_setup(proc, num_replicas):
    accepting_clients = [False for _ in range(num_replicas)]

    for line in iter(proc.stdout.readline, b""):
        l = line.decode()
        # print(l, end="", file=sys.stderr)
        if "manager" not in l and "accepting clients" in l:
            replica = int(l[l.find("(") + 1 : l.find(")")])
            assert not accepting_clients[replica]
            accepting_clients[replica] = True

        if accepting_clients.count(True) == num_replicas:
            break


def run_bench_client(protocol, value_size, put_ratio, length_s):
    cmd = [
        "python3",
        "./scripts/local_client.py",
        "-p",
        protocol,
        "-r",
        "bench",
        "-v",
        str(value_size),
        "-w",
        str(put_ratio),
        "-l",
        str(length_s),
    ]
    return run_process(cmd)


def parse_output(output):
    lines = [l.strip() for l in output.split("\n") if l.count("|") == 3]
    assert len(lines) >= 4
    assert lines[0].startswith("Elapsed")
    lines = lines[1:]

    warmup, tail = len(lines) // 3, len(lines) // 10
    lines = lines[warmup:-tail]

    tpts, lats = [], []
    for line in lines:
        segs = line.split()
        tpt = float(segs[2])  # reqs/s
        lat = float(segs[4]) / 1000.0  # ms
        tpts.append(tpt)
        lats.append(lat)

    median_tpt = tpts[len(tpts) // 2]
    median_lat = lats[len(lats) // 2]
    print(f"  med  tpt {median_tpt:9.2f} reqs/s  lat {median_lat:9.2f} ms")

    avg_tpt = sum(tpts) / len(tpts)
    std_tpt = statistics.stdev(tpts)
    avg_lat = sum(lats) / len(lats)
    std_lat = statistics.stdev(lats)
    print(f"  avg  tpt {avg_tpt:9.2f} reqs/s  lat {avg_lat:9.2f} ms")
    print(f"  std  tpt {std_tpt:9.2f}         lat {std_lat:9.2f}")


def bench_round(
    protocol,
    num_replicas,
    value_size,
    put_ratio,
    length_s,
    fault_tolerance=None,
    shards_per_replica=None,
):
    print(
        f"{protocol:<10s}  n={num_replicas:1d}  v={value_size:<9d}  "
        + f"f={fault_tolerance if fault_tolerance is not None else 'x':1}  "
        + f"s={shards_per_replica if shards_per_replica is not None else 'x':1}  "
        + f"w%={put_ratio:<3d}  {length_s:3d}s"
    )
    kill_all_matching("summerset_client", force=True)
    kill_all_matching("summerset_server", force=True)
    kill_all_matching("summerset_manager", force=True)

    configs = []
    if fault_tolerance is not None:
        configs.append(f"fault_tolerance={fault_tolerance}")
    if shards_per_replica is not None:
        configs.append(f"shards_per_replica={shards_per_replica}")

    configs.append(f"perf_storage_a={PERF_STORAGE_ALPHA}")
    configs.append(f"perf_storage_b={PERF_STORAGE_BETA}")
    configs.append(f"perf_network_a={PERF_NETWORK_ALPHA}")
    configs.append(f"perf_network_b={PERF_NETWORK_BETA}")

    proc_cluster = launch_cluster(protocol, num_replicas, "+".join(configs))
    wait_cluster_setup(proc_cluster, num_replicas)

    proc_client = run_bench_client(protocol, value_size, put_ratio, length_s)
    for line in iter(proc_client.stdout.readline, b""):
        l = line.decode()
        print(l, end="", file=sys.stderr)
    out, err = proc_client.communicate()

    proc_cluster.terminate()
    proc_cluster.wait()

    if proc_client.returncode != 0:
        print(err.decode())
    else:
        parse_output(out.decode())


if __name__ == "__main__":
    do_cargo_build()

    def all_protocol_configs(num_replicas):
        quorum_cnt = num_replicas // 2 + 1
        max_fault_tolerance = num_replicas - quorum_cnt

        config_choices = [("MultiPaxos", None, None)]
        for shards_per_replica in range(quorum_cnt, 0, -1):
            config_choices.append(
                ("Crossword", max_fault_tolerance, shards_per_replica)
            )
        config_choices.append(("Crossword", 0, 1))

        return config_choices

    for num_replicas in (3, 5, 7):
        for value_size in (1024, 65536, 4194304):
            for protocol, fault_tolerance, shards_per_replica in all_protocol_configs(
                num_replicas
            ):
                # print(
                #     num_replicas,
                #     value_size,
                #     protocol,
                #     fault_tolerance,
                #     shards_per_replica,
                # )
                bench_round(
                    protocol,
                    num_replicas,
                    value_size,
                    100,
                    60,
                    fault_tolerance=fault_tolerance,
                    shards_per_replica=shards_per_replica,
                )

    bench_round("MultiPaxos", 5, 65536, 0, 60)
    bench_round("Crossword", 5, 65536, 0, 60, fault_tolerance=0, shards_per_replica=1)
