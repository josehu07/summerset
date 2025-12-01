import os
import argparse
import subprocess
import multiprocessing
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


EXPER_NAME = "rs_coding"
BENCH_GROUP_NAMES = ["time_taken", "cpu_usage", "mem_usage"]


def printer_thread(proc, output_file):
    with open(output_file, "w") as fout:
        for line in iter(proc.stdout.readline, b""):
            l = line.decode()
            print(l, end="")
            fout.write(l)
        fout.flush()


def run_criterion_group(output_dir):
    group_names = "|".join(BENCH_GROUP_NAMES)
    cmd = [
        "cargo",
        "bench",
        "--bench",
        "rse_bench",
        "--",
        group_names,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    printer = multiprocessing.Process(
        target=printer_thread, args=(proc, f"{output_dir}/rs_coding.out")
    )
    printer.start()

    proc.wait()
    printer.join()


def parse_bench_results(output_dir):
    results = dict()
    with open(f"{output_dir}/rs_coding.out", "r") as fout:
        group, config, temp = None, None, None
        for line in fout:
            line = line.strip()
            if line.startswith("Benchmarking ") and line.endswith(
                "EFFECTIVE STARTING"
            ):
                group = line[line.find(" ") + 1 : line.find("/")].strip()
                name = line[line.find("/") + 1 : line.find(")") + 1]
                size = int(name[: name.find("@")])
                d = int(name[name.find("(") + 1 : name.find(",")])
                p = int(name[name.find(",") + 1 : name.find(")")])
                config, temp = (size, (d, p)), []
                if config not in results:
                    results[config] = dict()

            if group == "time_taken":
                if config is not None and "time:" in line:
                    segs = line.split()
                    time = float(segs[-4])
                    unit = segs[-3]
                    if unit == "s":
                        time *= 1000
                    elif unit == "ms":
                        pass
                    elif unit == "ns":
                        time /= 1000000
                    else:  # us
                        time /= 1000
                    results[config][group] = time  # ms
                    config, temp = None, None

            elif group == "cpu_usage":
                if (
                    config is not None
                    and line.startswith("cpu: ")
                    and line.endswith("%")
                ):
                    segs = line.split()
                    usage = float(segs[-2])
                    temp.append(usage)
                if config is not None and "time:" in line:
                    temp = sorted(temp)[1:-1]
                    assert len(temp) > 0
                    usage = sum(temp) / len(temp)
                    results[config][group] = usage  # %
                    config, temp = None, None

            else:  # mem_usage
                if (
                    config is not None
                    and line.startswith("mem: ")
                    and line.endswith("B")
                ):
                    segs = line.split()
                    usage = float(segs[-2]) + config[0] * (
                        config[1][1]
                        / (config[1][0] + config[1][1])  # add parity overhead
                    )
                    temp.append(usage / 1024)
                if config is not None and "time:" in line:
                    temp = sorted(temp)[1:-1]
                    assert len(temp) > 0
                    usage = sum(temp) / len(temp)
                    results[config][group] = usage  # KB
                    config, temp = None, None

    return results


def print_bench_results(results):
    print("Results:")
    for r, res in results.items():
        print(
            f"  {r[0]:7d} ({r[1][0]:2d},{r[1][1]:2d})"
            f"  {res['time_taken']:6.3f} ms  |"
            f"  cpu {res['cpu_usage']:5.3f}%  |"
            f"  mem {res['mem_usage']:8.3f} KB"
        )


def plot_bench_results(results, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (4, 1.5),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    _fig = plt.figure("Bench")

    xs, ys = [], []
    for r in results:
        if r[0] not in xs:
            xs.append(r[0])
        if r[1] not in ys:
            ys.append(r[1])
    xs.sort()
    ys.sort(reverse=True)

    data = [[0.0 for _ in xs] for _ in ys]
    vmin, vmax = float("inf"), 0.0
    for r, res in results.items():
        ms = res["time_taken"]
        xi, yi = xs.index(r[0]), ys.index(r[1])
        data[yi][xi] = ms
        if ms > vmax:
            vmax = ms
        if ms < vmin:
            vmin = ms

    cmap = plt.get_cmap("Reds")
    colors = cmap(np.linspace(1.0 - (vmax - vmin) / float(vmax), 0.6, cmap.N))
    new_cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        "Reds", colors
    )

    plt.imshow(data, cmap=new_cmap, aspect=0.6, norm="log")
    plt.colorbar(
        aspect=12,
        shrink=0.7,
        anchor=(0.0, 0.25),
        ticks=[vmin, 1, 10],
        format="{x:.0f}ms",
    )

    def readable_size(size):
        if size >= 1024 * 1024:
            return f"{size // (1024 * 1024)}M"
        elif size >= 1024:
            return f"{size // 1024}K"
        else:
            return size

    def readable_time(ms):
        if ms < 0.1:
            return f"{ms * 1000:.0f}μs"
        elif ms < 1.0:
            return f".{ms * 10:.0f}ms"
        else:
            return f"{ms:.0f}ms"

    for r, res in results.items():
        ms = res["time_taken"]
        xi, yi = xs.index(r[0]), ys.index(r[1])
        plt.text(
            xi,
            yi,
            readable_time(ms),
            horizontalalignment="center",
            verticalalignment="center",
            color="black",
            fontsize=8,
        )

    xticks = [readable_size(x) for x in xs]
    plt.xticks(list(range(len(xticks))), xticks)

    yticks = [f"({d + p},{d})" for d, p in ys]
    plt.yticks(list(range(len(yticks))), yticks)

    plt.tight_layout()

    pdf_name = f"{plots_dir}/rs_coding.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default="./results",
        help="directory to hold outputs and logs",
    )
    parser.add_argument(
        "-p",
        "--plot",
        action="store_true",
        help="if set, do the plotting phase",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        os.system(f"mkdir -p {args.odir}")

    if not args.plot:
        output_path = f"{args.odir}/output/{EXPER_NAME}"
        if not os.path.isdir(output_path):
            os.system(f"mkdir -p {output_path}")

        run_criterion_group(output_path)

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results = parse_bench_results(output_dir)
        print_bench_results(results)

        # plot_bench_results(results, plots_dir)


if __name__ == "__main__":
    main()
