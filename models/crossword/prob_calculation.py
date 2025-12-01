import random
import statistics
import argparse
import pickle
import matplotlib
import matplotlib.pyplot as plt


CLUSTER = 5

# instance size in KBs
SIZES = [2**i for i in range(3, 10)]
SIZES += [1024 * i for i in range(1, 101)]

# tuples of (min_delay in ms, max_bandwidth in Gbps)
POWERS = [(10, 100), (50, 10), (120, 1)]

# delay jitter value (in percentage of min_delay)
JITTERS = [10, 25, 50]

PARETO_ALPHA = 1.16  # log_4(5)
NUM_TRIALS = 10000

QUORUM_COLOR_WIDTH = {
    5: ("red", 1),
    4: ("steelblue", 1.25),
    3: ("dimgray", 1.5),
}


def rand_individual_time(c, s, d, b, jitter):
    pareto = random.paretovariate(PARETO_ALPHA)
    while pareto > 10:
        pareto = random.paretovariate(PARETO_ALPHA)
    t = d + d * (jitter / 100) * (pareto - 1)
    t += (s * c) / (b * 1024 / 8)
    return t


def response_time_sample(n, q, c, s, d, b, jitter):
    ts = [rand_individual_time(c, s, d, b, jitter) for _ in range(n - 1)]
    ts.sort()
    # diffs = [ts[i] - ts[i - 1] for i in range(1, len(ts))]
    # print([int(t) for t in ts], [int(diff) for diff in diffs])
    return ts[q - 2]  # assuming leader itself must have accepted


def response_time_mean_stdev(n, q, c, s, d, b, jitter):
    rts = []
    for _ in range(NUM_TRIALS):
        rts.append(response_time_sample(n, q, c, s, d, b, jitter))
    mean = sum(rts) / len(rts)
    stdev = statistics.stdev(rts)
    return mean, stdev


def calc_fixed_env_result(n, d, b, jitter):
    m = n // 2 + 1
    result = dict()
    for q in range(m, n + 1):
        c = n + 1 - q
        result[(q, c)] = []
        for v in SIZES:
            s = v / m
            mean, stdev = response_time_mean_stdev(n, q, c, s, d, b, jitter)
            result[(q, c)].append((mean, stdev))
    return result


def calc_all_env_results(n):
    results = dict()
    for i, (d, b) in enumerate(POWERS):
        for j, jitter in enumerate(JITTERS):
            result = calc_fixed_env_result(n, d, b, jitter)
            results[(i, j)] = result
            print(f"calculated {d} {b} {jitter}")
    return results


def print_all_env_results(results):
    for i, (d, b) in enumerate(POWERS):
        for j, jitter in enumerate(JITTERS):
            print(f"Env {i},{j}:  d={d}  b={b}  jitter={jitter}")
            for q, c in results[(i, j)]:
                print(f"  config  q={q}  c={c} ", end="")
                for mean, stdev in results[(i, j)][(q, c)]:
                    print(f" {mean:7.2f}", end="")
                print()


def plot_env_result_subplot(i, j, results):
    POWERS = results["powers"]
    JITTERS = results["jitters"]
    VSIZES = results["vsizes"]
    results = results["results"]

    subplot_id = len(POWERS) * 100 + len(JITTERS) * 10
    subplot_id += i * len(JITTERS) + j + 1
    ax = plt.subplot(subplot_id)

    for q, c in results[(i, j)]:
        xs = [s / 1024 for s in VSIZES]
        ys = [t[0] for t in results[(i, j)][(q, c)]]
        plt.plot(
            xs,
            ys,
            label=f"q={q}  c={c}",
            color=QUORUM_COLOR_WIDTH[q][0],
            linewidth=QUORUM_COLOR_WIDTH[q][1],
        )

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(direction="in")

    if i == len(POWERS) - 1 and j == len(JITTERS) - 1:
        plt.xlabel("Instance\nsize (MB)", loc="right", multialignment="left")
        ax.xaxis.set_label_coords(2, 0.18)
    if i < len(POWERS) - 1:
        ax.tick_params(bottom=False, labelbottom=False)

    if i == 0 and j == 0:
        plt.ylabel(
            "Response\ntime (ms)",
            loc="top",
            rotation=0,
            multialignment="left",
            backgroundcolor="white",
        )
        ax.yaxis.set_label_coords(0.45, 1.02)
    if j > 0:
        ax.tick_params(left=False, labelleft=False)

    xright = max(VSIZES) / 1024
    ybottom, ytop = float("inf"), 0
    for jj in range(len(JITTERS)):
        for cf in results[(i, j)]:
            for v in range(len(VSIZES)):
                y = results[(i, jj)][cf][v][0]
                if y > ytop:
                    ytop = y
                if y < ybottom:
                    ybottom = y

    plt.xlim(0, xright * 1.1)
    plt.ylim(0, ytop * 1.2)

    plt.xticks(
        [0, xright],
        ["0", f"{int(xright)}"],
        fontsize="x-small",
        color="dimgray",
    )
    plt.yticks(
        [ybottom, ytop],
        [f"{int(ybottom)}", f"{int(ytop)}"],
        fontsize="x-small",
        color="dimgray",
    )

    if i == len(POWERS) - 1:
        jitter = JITTERS[j]
        plt.text(
            xright * 0.5 if j > 0 else xright * 0.65,
            -ytop * 0.48,
            f"+{jitter / 100:.1f}d",
            horizontalalignment="center",
            verticalalignment="center",
        )
    if j == 0:
        d, b = POWERS[i]
        i_env_strs = {
            0: "datacenter",
            1: "regional",
            2: "wide-area",
        }
        plt.text(
            -xright * 1.05,
            ytop * 0.6 if i < len(POWERS) - 1 else ytop * 0.8,
            f"{i_env_strs[i]}\n{d}ms\n{b}Gbps",
            horizontalalignment="center",
            verticalalignment="center",
        )
    if i == len(POWERS) - 1 and j == 0:
        # plt.text(
        #     -xright * 1.1,
        #     -ytop * 0.5,
        #     "Env.",
        #     horizontalalignment="center",
        #     verticalalignment="center",
        #     # color="dimgray",
        #     weight="bold",
        # )
        plt.text(
            -xright * 1.05,
            0,
            "RTT (d)\nBW (b)",
            horizontalalignment="center",
            verticalalignment="center",
            weight="bold",
        )
        plt.text(
            -xright * 0.48,
            -ytop * 0.48,
            "Base Jitter",
            horizontalalignment="center",
            verticalalignment="center",
            weight="bold",
        )

    print(f"Plotted subplot {subplot_id}")
    return ax


def plot_all_env_results(results, output_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (5, 4),
            "font.size": 10,
            "axes.axisbelow": False,
        }
    )
    fig = plt.figure()

    handles, labels = None, None
    for i in range(len(results["powers"])):
        for j in range(len(results["jitters"])):
            ax = plot_env_result_subplot(i, j, results)
            if i == 0 and j == 0:
                handles, labels = ax.get_legend_handles_labels()

    _lgd = fig.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(0.76, 0.5),
        handlelength=0.8,
        title="Configs",
    )

    fig.subplots_adjust(bottom=0.16, top=0.9, left=0.23, right=0.75)

    plt.savefig(
        f"{output_dir}/calc.envs.r_{CLUSTER}.pdf",
        bbox_inches=0,
    )
    plt.close()


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="./results",
        help="output folder",
    )
    parser.add_argument(
        "-p",
        "--plot",
        action="store_true",
        help="if set, do the plotting phase",
    )
    args = parser.parse_args()

    if not args.plot:
        results = calc_all_env_results(CLUSTER)
        # print_all_env_results(results)

        results = {
            "vsizes": SIZES,
            "powers": POWERS,
            "jitters": JITTERS,
            "results": results,
        }

        with open(f"{args.output_dir}/calc.envs.r_{CLUSTER}.pkl", "wb") as fpkl:
            pickle.dump(results, fpkl)
            print(f"Dumped: {CLUSTER}")

    else:
        with open(f"{args.output_dir}/calc.envs.r_{CLUSTER}.pkl", "rb") as fpkl:
            results = pickle.load(fpkl)
            plot_all_env_results(results, args.output_dir)


if __name__ == "__main__":
    main()
