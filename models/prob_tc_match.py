import random
import statistics
import argparse
import pickle

import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore


HEADER_LEN = 8
MSG_ID_LEN = 8
MSG_LEN = 1024
REPLY_LEN = 32

NUM_MSGS = 20000

DELAY_BASE = 10
DELAY_JITTERS = [1, 2, 3, 4, 5]
PARETO_ALPHA = 1.16  # log_4(5)
RATE_GBIT = 10


def rand_individual_time(size, d, b, jitter):
    pareto = random.paretovariate(PARETO_ALPHA)
    while pareto > 10:
        pareto = random.paretovariate(PARETO_ALPHA)
    t = d + (pareto - 1) * jitter
    t += size / (b * 1024 / 8)
    return t


def sample_many_msgs(d, b, jitter):
    millisecs = []
    for _ in range(NUM_MSGS):
        msg_size = (MSG_LEN + HEADER_LEN + MSG_ID_LEN) / 1024
        reply_size = (REPLY_LEN + HEADER_LEN + MSG_ID_LEN) / 1024

        tm = rand_individual_time(msg_size, d, b, jitter)
        tr = rand_individual_time(reply_size, d, b, jitter)

        millisecs.append(tm + tr)

    return millisecs


def plot_histogram(millisecs, jitter, output_dir):
    plt.hist(millisecs, bins=100)

    plt.xlabel("ms")
    plt.xlim(0, max(millisecs) * 1.1)

    plt.savefig(f"{output_dir}/match.pareto.{jitter}.png")
    plt.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o", "--output_dir", type=str, default="./results", help="output folder"
    )
    args = parser.parse_args()

    for jitter in DELAY_JITTERS:
        millisecs = sample_many_msgs(DELAY_BASE, RATE_GBIT, jitter)
        plot_histogram(millisecs, jitter, args.output_dir)
        print(f"plotted {jitter}")
