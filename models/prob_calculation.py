import random
import statistics
import argparse
import pickle

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


CLUSTER = 5

# instance size in KBs
SIZES = [2**i for i in range(3, 10)]
SIZES += [1024 * i for i in range(1, 26)]

# tuples of (min_delay in ms, max bandwidth in Gbps)
POWERS = [(10, 50), (50, 10), (100, 1)]

# standard deviations of the half-normal distribution
# values are percentages w.r.t. min_delay
JITTERS = [10, 20, 50]

NUM_TRIALS = 10000

QUORUM_COLOR_WIDTH = {
    5: ("red", 1.2),
    4: ("steelblue", 1.5),
    3: ("dimgray", 1.8),
}


def mean_individual_time(c, s, d, b):
    return d + (s * c) / (b * 1024 / 8)


def rand_individual_time(c, s, d, b, jit):
    mu = mean_individual_time(c, s, d, b)
    jit = mu * (jit / 100)
    t = random.gauss(mu, jit)
    while t < mu - jit or t > mu + jit:
        t = random.gauss(mu, jit)
    return t


def response_time_sample(n, q, c, s, d, b, jit):
    ts = [rand_individual_time(c, s, d, b, jit) for _ in range(n - 1)]
    ts.sort()
    return ts[q - 2]  # assuming leader itself must have accepted


def response_time_mean_stdev(n, q, c, s, d, b, jit):
    rts = []
    for _ in range(NUM_TRIALS):
        rts.append(response_time_sample(n, q, c, s, d, b, jit))
    mean = sum(rts) / len(rts)
    stdev = statistics.stdev(rts)
    return mean, stdev


def calc_fixed_env_result(n, d, b, jit):
    m = n // 2 + 1
    result = dict()
    for q in range(m, n + 1):
        c = n + 1 - q
        result[(q, c)] = []
        for v in SIZES:
            s = v / m
            mean, stdev = response_time_mean_stdev(n, q, c, s, d, b, jit)
            result[(q, c)].append((mean, stdev))
    return result


def calc_all_env_results(n):
    results = dict()
    for i, (d, b) in enumerate(POWERS):
        for j, jit in enumerate(JITTERS):
            result = calc_fixed_env_result(n, d, b, jit)
            results[(i, j)] = result
            print(f"calculated {d} {b} {jit}")
    return results


def print_all_env_results(results):
    for i, (d, b) in enumerate(POWERS):
        for j, jit in enumerate(JITTERS):
            print(f"Env {i},{j}:  d={d}  b={b}  jit={jit}")
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
        ax.xaxis.set_label_coords(1.95, 0.18)
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
        ax.yaxis.set_label_coords(0.45, 1.05)
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

    plt.xticks([0, xright], ["0", f"{int(xright)}"], fontsize="small", color="dimgray")
    plt.yticks(
        [ybottom, ytop],
        [f"{int(ybottom)}", f"{int(ytop)}"],
        fontsize="small",
        color="dimgray",
    )

    if i == len(POWERS) - 1:
        jit = JITTERS[j]
        plt.text(
            xright * 0.5 if j > 0 else xright * 0.7,
            -ytop * 0.5,
            f"±{jit / 100:.1f}d",
            horizontalalignment="center",
            verticalalignment="center",
        )
    if j == 0:
        d, b = POWERS[i]
        plt.text(
            -xright * 0.95,
            ytop * 0.6 if i < len(POWERS) - 1 else ytop * 0.8,
            f"{d}ms\n{b}Gbps",
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
            -xright * 0.95,
            0,
            "RTT (d)\nBW (b)",
            horizontalalignment="center",
            verticalalignment="center",
            weight="bold",
        )
        plt.text(
            -xright * 0.3,
            -ytop * 0.5,
            "Jitter (σ)",
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

    leg = fig.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(0.76, 0.5),
        handlelength=0.8,
        title="Configs",
    )

    fig.subplots_adjust(bottom=0.15, top=0.85, left=0.2, right=0.75)

    plt.savefig(
        f"{output_dir}/calc.envs.r_{CLUSTER}.png",
        dpi=300,
    )
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o", "--output_dir", type=str, default="./results", help="output folder"
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not args.plot:
        results = calc_all_env_results(CLUSTER)
        print_all_env_results(results)

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
