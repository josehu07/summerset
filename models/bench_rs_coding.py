import argparse
import subprocess
import multiprocessing

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


BENCH_GROUP_NAME = "rse_bench"


def printer_thread(proc, output_file):
    with open(output_file, "w") as fout:
        for line in iter(proc.stdout.readline, b""):
            l = line.decode()
            print(l, end="")
            fout.write(l)


def run_criterion_group(output_dir):
    cmd = ["cargo", "bench", "--", BENCH_GROUP_NAME]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    printer = multiprocessing.Process(
        target=printer_thread, args=(proc, f"{output_dir}/rs_coding.out")
    )
    printer.start()

    proc.wait()
    printer.terminate()


def parse_bench_results(output_dir):
    results = dict()
    with open(f"{output_dir}/rs_coding.out", "r") as fout:
        curr_round = None
        for line in fout:
            if line.startswith(f"{BENCH_GROUP_NAME}/"):
                line = line.strip()
                name = line[line.find("/") + 1 : line.find(")") + 1]
                size = int(name[: name.find("@")])
                d = int(name[name.find("(") + 1 : name.find(",")])
                p = int(name[name.find(",") + 1 : name.find(")")])
                curr_round = (size, (d, p))

            if curr_round is not None and "time:" in line:
                segs = line.split()
                time = float(segs[-4])
                unit = segs[-3]
                if unit == "ms":
                    pass
                elif unit == "ns":
                    time /= 1000000
                else:  # us
                    time /= 1000

                results[curr_round] = time
                curr_round = None

    return results


def print_bench_results(results):
    print("Results:")
    for r, ms in results.items():
        print(f"  {r[0]:7d} ({r[1][0]:2d},{r[1][1]:2d})  {ms:6.3f} ms")


def plot_bench_results(results, output_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (6, 3),
            "font.size": 10,
        }
    )
    fig = plt.figure("Bench")

    pdf_name = f"{output_dir}/rs_coding.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    parser.add_argument(
        "-o", "--output_dir", type=str, default="./results", help="output folder"
    )
    args = parser.parse_args()

    if not args.plot:
        run_criterion_group(args.output_dir)

    else:
        results = parse_bench_results(args.output_dir)
        print_bench_results(results)
        plot_bench_results(results, args.output_dir)
