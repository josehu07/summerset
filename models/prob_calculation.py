import matplotlib  # type: ignore

matplotlib.use("Agg")

import random
import statistics
import matplotlib.pyplot as plt  # type: ignore


NUM_TRIALS = 1000

CLUSTER = 5

# instance size in KBs
SIZES = [2**i for i in range(3, 10)]
SIZES += [1024 * i for i in range(1, 21)]

# tuples of (min_delay in ms, max bandwidth in Gbps)
POWERS = [(10, 25), (20, 10), (40, 1)]

# standard deviations of the half-normal distribution
SIGMAS = [5, 10, 20]


def min_individual_time(c, s, d, b):
    return d + (s * c) / (b * 1024 / 8)


def rand_individual_time(c, s, d, b, sigma):
    mu = min_individual_time(c, s, d, b)
    t = random.gauss(mu, sigma)
    if t < mu:
        t = mu + (mu - t)
    return t


def response_time_sample(n, q, c, s, d, b, sigma):
    ts = [rand_individual_time(c, s, d, b, sigma) for _ in range(n - 1)]
    ts.sort()
    return ts[q - 2]  # assuming leader itself must have accepted


def response_time_mean_stdev(n, q, c, s, d, b, sigma):
    rts = []
    for _ in range(NUM_TRIALS):
        rts.append(response_time_sample(n, q, c, s, d, b, sigma))
    mean = sum(rts) / len(rts)
    stdev = statistics.stdev(rts)
    return mean, stdev


def calc_fixed_env_result(n, d, b, sigma):
    m = n // 2 + 1
    result = dict()
    for q in range(m, n + 1):
        c = n + 1 - q
        result[(q, c)] = []
        for v in SIZES:
            s = v / m
            mean, stdev = response_time_mean_stdev(n, q, c, s, d, b, sigma)
            result[(q, c)].append((mean, stdev))
    return result


def calc_all_env_results(n):
    results = dict()
    for i, (d, b) in enumerate(POWERS):
        for j, sigma in enumerate(SIGMAS):
            result = calc_fixed_env_result(n, d, b, sigma)
            results[(i, j)] = result
    return results


def print_all_env_results(results):
    for i, (d, b) in enumerate(POWERS):
        for j, sigma in enumerate(SIGMAS):
            print(f"Env {i},{j}:  d={d}  b={b}  sigma={sigma}")
            for q, c in results[(i, j)]:
                print(f"  config  q={q}  c={c} ", end="")
                for mean, stdev in results[(i, j)][(q, c)]:
                    print(f" {mean:7.2f}", end="")
                print()


def plot_env_result_subplot(i, j, results):
    subplot_id = len(POWERS) * 100 + len(SIGMAS) * 10
    subplot_id += i * len(SIGMAS) + j + 1
    ax = plt.subplot(subplot_id)

    for q, c in results[(i, j)]:
        xs = [s / 1024 for s in SIZES]
        ys = [t[0] for t in results[(i, j)][(q, c)]]
        plt.plot(xs, ys, label=f"|Q|={q}  l={c}")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    if i == len(POWERS) - 1 and j == len(SIGMAS) - 1:
        plt.xlabel("Round data\nsize (MB)", loc="right")
        ax.xaxis.set_label_coords(2.1, 0.08)
    if i < len(POWERS) - 1:
        ax.tick_params(bottom=False, labelbottom=False)

    if i == 0 and j == 0:
        plt.ylabel("Response time (ms)", loc="top", rotation=0)
        ax.yaxis.set_label_coords(0.5, 1.05)
    if j > 0:
        ax.tick_params(left=False, labelleft=False)

    ytop = 0
    for jj in range(len(SIGMAS)):
        for cf in results[(i, j)]:
            for v in range(len(SIZES)):
                y = results[(i, jj)][cf][v][0]
                if y > ytop:
                    ytop = y
    ytop *= 1.1
    plt.ylim(0, ytop)

    xright = max(SIZES) / 1024
    if i == len(POWERS) - 1:
        sigma = SIGMAS[j]
        plt.text(
            xright * 0.5,
            -ytop * 0.5,
            f"{sigma}ms",
            horizontalalignment="center",
            verticalalignment="center",
            color="dimgray",
        )
    if j == 0:
        d, b = POWERS[i]
        plt.text(
            -xright * 0.95,
            ytop * 0.5,
            f"{d}ms\n{b}Gbps",
            horizontalalignment="center",
            verticalalignment="center",
            color="dimgray",
        )
    if i == len(POWERS) - 1 and j == 0:
        plt.text(
            -xright * 1.1,
            -ytop * 0.5,
            "Env.",
            horizontalalignment="center",
            verticalalignment="center",
            color="dimgray",
            weight="bold",
        )
        plt.text(
            -xright * 0.95,
            -ytop * 0.04,
            "  . .  \nRTT (d)\nBW (b)",
            horizontalalignment="center",
            verticalalignment="center",
            color="dimgray",
        )
        plt.text(
            -xright * 0.3,
            -ytop * 0.5,
            "stdev (σ):",
            horizontalalignment="center",
            verticalalignment="center",
            color="dimgray",
        )

    print(f"Plotted subplot {subplot_id}")
    return ax


def plot_all_env_results(results):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (5, 4),
            "font.size": 10,
        }
    )
    fig = plt.figure()

    handles, labels = None, None
    for i in range(len(POWERS)):
        for j in range(len(SIGMAS)):
            ax = plot_env_result_subplot(i, j, results)
            if i == 0 and j == 0:
                handles, labels = ax.get_legend_handles_labels()

    leg = fig.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(0.76, 0.5),
        handlelength=1,
        title="Configs",
    )

    fig.subplots_adjust(bottom=0.15, top=0.85, left=0.2, right=0.75)

    plt.savefig(
        "results/prob_model.png",
        dpi=300,
    )
    plt.close()


if __name__ == "__main__":
    results = calc_all_env_results(CLUSTER)
    print_all_env_results(results)
    plot_all_env_results(results)
