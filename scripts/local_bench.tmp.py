import os
import subprocess
import time
import statistics


def do_cargo_build():
    print("Building everything...")
    cmd = ["cargo", "build", "--workspace", "-r"]
    proc = subprocess.Popen(cmd)
    proc.wait()


def run_process(cmd):
    # print("Run:", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc


def launch_cluster(protocol, num_replicas):
    cmd = [
        "python3",
        "./scripts/local_cluster.py",
        "-p",
        protocol,
        "-n",
        str(num_replicas),
        "-r",
    ]
    return run_process(cmd)


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

    warmup = len(lines) // 3
    lines = lines[warmup:]

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


def bench_round(protocol, num_replicas, value_size, put_ratio, length_s):
    print(
        f"{protocol:<10s}  n={num_replicas:1d}  v={value_size:<9d}  w%={put_ratio:<3d}  {length_s:3d}s"
    )
    os.system("pkill summerset_client")
    os.system("pkill summerset_server")
    os.system("pkill summerset_manager")

    proc_cluster = launch_cluster(protocol, num_replicas)
    time.sleep(5)

    proc_client = run_bench_client(protocol, value_size, put_ratio, length_s)
    out, err = proc_client.communicate()

    proc_cluster.terminate()
    proc_cluster.wait()

    if proc_client.returncode != 0:
        print(err.decode())
    else:
        parse_output(out.decode())


if __name__ == "__main__":
    do_cargo_build()

    for num_replicas in (3, 7):
        for value_size in (1024, 65536, 4194304):
            for protocol in ("MultiPaxos", "RSPaxos"):
                bench_round(protocol, num_replicas, value_size, 100, 45)

    bench_round("MultiPaxos", 7, 4194304, 10, 45)
    bench_round("RSPaxos", 7, 4194304, 10, 45)
