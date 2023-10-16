import os
import sys
import random
import statistics
import pickle


def path_get_last_segment(path):
    if "/" not in path:
        return None
    eidx = len(path) - 1
    while eidx > 0 and path[eidx] == "/":
        eidx -= 1
    bidx = path[:eidx].rfind("/")
    bidx += 1
    return path[bidx : eidx + 1]


def check_proper_cwd():
    cwd = os.getcwd()
    if "summerset" not in path_get_last_segment(cwd) or not os.path.isdir("scripts/"):
        print(
            "ERROR: script must be run under top-level repo with `python3 scripts/<script>.py ...`"
        )
        sys.exit(1)


NUM_TRIALS = 10000

CLUSTER = 5

# instance size in KBs
SIZES = [2**i for i in range(3, 10)]
SIZES += [1024 * i for i in range(1, 51)]

# tuples of (min_delay in ms, max bandwidth in Gbps)
POWERS = [(10, 40), (50, 10), (100, 1)]

# standard deviations of the half-normal distribution
SIGMAS = [5, 15, 30]


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


if __name__ == "__main__":
    check_proper_cwd()

    results = calc_all_env_results(CLUSTER)
    print_all_env_results(results)

    results = {
        "vsizes": SIZES,
        "powers": POWERS,
        "sigmas": SIGMAS,
        "results": results,
    }

    with open(f"results/calc.envs.r_{CLUSTER}.pkl", "wb") as fpkl:
        pickle.dump(results, fpkl)
        print(f"Dumped: {CLUSTER}")
