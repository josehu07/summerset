import matplotlib  # type: ignore

matplotlib.use("Agg")

import random
import statistics
import matplotlib.pyplot as plt  # type: ignore


NUM_TRIALS = 1000

CLUSTER = 5

# instance size in KBs
SIZES = [1024 * i for i in range(1, 17)]

# tuples of (min_delay in ms, max bandwidth in MB/s)
POWERS = [(100, 100), (25, 400), (6, 1600)]

# standard deviations of the half-normal distribution
SIGMAS = [5, 25]


def min_individual_time(c, s, d, b):
    return d + (s * c) / b


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


def plot_all_env_results(results):
    for i, (d, b) in enumerate(POWERS):
        for j, sigma in enumerate(SIGMAS):
            subplot_id = len(SIGMAS) * 100 + len(POWERS) * 10
            subplot_id += j * len(POWERS) + i + 1
            ax = plt.subplot(subplot_id)

            for q, c in results[(i, j)]:
                xs = [s / 1024 for s in SIZES]
                ys = [t[0] for t in results[(i, j)][(q, c)]]
                plt.plot(xs, ys, label=f"|Q|={q}  l={c}", marker="o", markersize=3)

            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)

            plt.xlabel("Instance size (MB)")
            plt.ylabel("Response time (ms)")

            plt.title(f"d={d}  b={b}  σ={sigma}")
            print(f"Plotted subplot {subplot_id}")

    plt.tight_layout()
    plt.savefig("results/prob_model.png", dpi=300)

    plt.close()


if __name__ == "__main__":
    results = calc_all_env_results(CLUSTER)
    print_all_env_results(results)
    plot_all_env_results(results)
