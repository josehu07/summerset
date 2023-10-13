import matplotlib  # type: ignore

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # type: ignore
import pickle


CLUSTER = 5


def plot_env_result_subplot(i, j, results):
    POWERS = results["powers"]
    SIGMAS = results["sigmas"]
    VSIZES = results["vsizes"]
    results = results["results"]

    subplot_id = len(POWERS) * 100 + len(SIGMAS) * 10
    subplot_id += i * len(SIGMAS) + j + 1
    ax = plt.subplot(subplot_id)

    for q, c in results[(i, j)]:
        xs = [s / 1024 for s in VSIZES]
        ys = [t[0] for t in results[(i, j)][(q, c)]]
        plt.plot(xs, ys, label=f"|Q|={q}  l={c}")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(direction="in")

    if i == len(POWERS) - 1 and j == len(SIGMAS) - 1:
        plt.xlabel("Round data\nsize (MB)", loc="right", multialignment="left")
        ax.xaxis.set_label_coords(2.15, 0.18)
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
        ax.yaxis.set_label_coords(0.45, 1)
    if j > 0:
        ax.tick_params(left=False, labelleft=False)

    for t in ax.xaxis.get_ticklabels():
        t.set_color("dimgray")
    for t in ax.yaxis.get_ticklabels():
        t.set_color("dimgray")

    xright = max(VSIZES) / 1024
    ytop = 0
    for jj in range(len(SIGMAS)):
        for cf in results[(i, j)]:
            for v in range(len(VSIZES)):
                y = results[(i, jj)][cf][v][0]
                if y > ytop:
                    ytop = y
    ytop *= 1.1

    plt.xlim(0, xright * 1.1)
    plt.ylim(0, ytop)

    if i == len(POWERS) - 1:
        sigma = SIGMAS[j]
        plt.text(
            xright * 0.5 if j > 0 else xright * 0.7,
            -ytop * 0.5,
            f"{sigma}ms",
            horizontalalignment="center",
            verticalalignment="center",
            # color="dimgray",
        )
    if j == 0:
        d, b = POWERS[i]
        plt.text(
            -xright * 0.95,
            ytop * 0.5 if i < len(POWERS) - 1 else ytop * 0.7,
            f"{d}ms\n{b}Gbps",
            horizontalalignment="center",
            verticalalignment="center",
            # color="dimgray",
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
            -ytop * 0.1,
            # "  . .  \nRTT (d)\nBW (b)",
            "RTT (d)\nBW (b)",
            horizontalalignment="center",
            verticalalignment="center",
            # color="dimgray",
            weight="bold",
        )
        plt.text(
            -xright * 0.4,
            -ytop * 0.5,
            # "stdev (σ):",
            "stdev (σ)",
            horizontalalignment="center",
            verticalalignment="center",
            # color="dimgray",
            weight="bold",
        )

    print(f"Plotted subplot {subplot_id}")
    return ax


def plot_all_env_results(results):
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
        for j in range(len(results["sigmas"])):
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
        f"results/calc.envs.r_{CLUSTER}.png",
        dpi=300,
    )
    plt.close()


if __name__ == "__main__":
    with open(f"results/calc.envs.r_{CLUSTER}.pkl", "rb") as fpkl:
        results = pickle.load(fpkl)
        plot_all_env_results(results)
