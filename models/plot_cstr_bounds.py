import matplotlib  # type: ignore

matplotlib.use("Agg")

import numpy as np  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
import matplotlib.patches as mpatches  # type: ignore
from matplotlib.legend_handler import HandlerPatch  # type: ignore
import math


SUBPLOT_ARG = lambda idx: 141 + idx

CLUSTER_SIZES = [3, 5, 7, 9]
SIZE_COLOR_MAP = {
    3: ("seagreen", "palegreen"),
    5: ("orange", "bisque"),
    7: ("steelblue", "powderblue"),
    9: ("chocolate", "mistyrose"),
}

X_TICKS = list(range(1, 10))
Y_TICKS = list(range(1, 6))


def plot_cstr_bound(idx, cluster_size):
    ax = plt.subplot(SUBPLOT_ARG(idx))

    n = cluster_size
    f = n // 2
    m = n - f

    line_color, fill_color = SIZE_COLOR_MAP[cluster_size]

    # Classic Paxos/Raft point
    plt.scatter(
        m, m, marker="s", s=100, color="black", label="Classic Paxos/Raft", zorder=10
    )

    # CRaft point
    craft_q = math.ceil((n + m) / 2)
    plt.scatter(
        craft_q,
        1,
        marker="X",
        s=110,
        color="lightcoral",
        label="RS-Paxos/CRaft",
        zorder=10,
    )

    # boundary lines
    xs = [x for x in range(m, n + 1)]
    ys = [x for x in range(m, 0, -1)]
    plt.plot(
        xs,
        ys,
        linewidth=2,
        marker="o",
        markersize=7,
        color=line_color,
        label="Crossword configs",
        zorder=20,
    )
    plt.vlines(m, ymin=m, ymax=m + 1.5, linestyles="-", color=line_color, zorder=20)
    plt.vlines(n, ymin=1, ymax=2.5, linestyles="-", color=line_color, zorder=20)

    # correct region
    xs = [m, m, n, n]
    ys = [m, m + 1, 2, 1]
    plt.fill(xs, ys, color=fill_color, label="Region of fault-tolerance=f", zorder=0)

    # unused x-axis range
    if cluster_size < CLUSTER_SIZES[-1]:
        xs = [n + 0.9, X_TICKS[-1] + 0.35, X_TICKS[-1] + 0.35, n + 0.8]
        ys = [0.3, 0.3, 0, 0]
        plt.fill(
            xs, ys, hatch="///", fill=False, edgecolor=None, linewidth=0, zorder=10
        )

    # latency & throughput optimized arrows
    plt.arrow(
        m + 0.3,
        m + 1.7,
        -0.9,
        0.9,
        linewidth=1,
        color="dimgray",
        length_includes_head=True,
        head_width=0.3,
        overhang=0.5,
        label="Tradeoff decisions",
    )
    plt.text(
        m + 0.18 if n <= 5 else m + 0.5 if n == 9 else m + 0.4,
        m + 2.78 if n <= 5 else m + 2.0 if n == 9 else m + 2.4,
        "Lat.\noptim.",
        horizontalalignment="left",
        verticalalignment="center",
        color="dimgray",
    )
    plt.arrow(
        n - 0.3,
        3.3,
        0.9,
        -0.9,
        linewidth=1,
        color="dimgray",
        length_includes_head=True,
        head_width=0.3,
        overhang=0.5,
    )
    plt.text(
        n + 0.8 if n <= 5 else n + 0.0 if n == 9 else n + 0.4,
        1 + 1.5 if n <= 5 else 1 + 2.9 if n == 9 else 1 + 2.6,
        "Tput.\noptim.",
        horizontalalignment="left",
        verticalalignment="center",
        color="dimgray",
    )

    plt.axis("scaled")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim((0, X_TICKS[-1] + 0.7))
    plt.ylim((0, Y_TICKS[-1] + 2.7))
    plt.xticks(X_TICKS[:cluster_size], list(map(str, X_TICKS))[:cluster_size])
    plt.yticks(Y_TICKS, list(map(str, Y_TICKS)))

    plt.xlabel("|Quorum|", loc="right")
    plt.ylabel("#Shards\n/replica", loc="top", rotation=0, backgroundcolor="white")
    if idx < 2:
        ax.xaxis.set_label_coords(1.05, -0.1)
    else:
        ax.xaxis.set_label_coords(1.05, -0.18)
    ax.yaxis.set_label_coords(0.2, 0.8)

    # plt.title(
    #     f"|Cluster|={n}  f={f}",
    #     x=0.5,
    #     y=-0.48,
    #     fontsize=11,
    #     # fontweight="bold",
    #     # backgroundcolor=fill_color,
    # )
    plt.text(2.2, -3.2, f"|Cluster|={n}  f={f}", fontsize=11)
    plt.text(1, -3.2, "▬", fontsize=11, color=line_color)

    return ax


def make_legend(fig, handles, labels):
    def make_legend_arrow(
        legend, orig_handle, xdescent, ydescent, width, height, fontsize
    ):
        return mpatches.FancyArrow(
            0,
            0.5 * height,
            width,
            0,
            linewidth=1,
            color="dimgray",
            length_includes_head=True,
            head_width=0.6 * height,
            overhang=0.3,
        )

    def make_legend_polygon(
        legend, orig_handle, xdescent, ydescent, width, height, fontsize
    ):
        return mpatches.Polygon(
            xy=np.array(
                [
                    [0.2 * width, 0.5 * height],
                    [0.2 * width, 1.2 * height],
                    [0.8 * width, 0.5 * height],
                    [0.8 * width, -0.2 * height],
                ]
            ),
            closed=True,
            color="dimgray",
        )

    order = []
    for s in ("Classic", "RS-", "Crossword", "Region", "Tradeoff"):
        for i, l in enumerate(labels):
            if s in l:
                order.append(i)
                break
    sorted_handles = [handles[i] for i in order]
    sorted_labels = [labels[i] for i in order]

    leg = fig.legend(
        sorted_handles,
        sorted_labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.81),
        ncol=len(handles),
        handlelength=1.5,
        handletextpad=0.5,
        handler_map={
            mpatches.FancyArrow: HandlerPatch(patch_func=make_legend_arrow),
            mpatches.Polygon: HandlerPatch(patch_func=make_legend_polygon),
        },
    )
    for h in leg.legend_handles[2:]:
        h.set_color("dimgray")


def plot_all_cstr_bounds():
    matplotlib.rcParams.update(
        {
            "figure.figsize": (10, 3),
            "font.size": 10,
            "axes.axisbelow": False,
        }
    )
    fig = plt.figure()

    handles, labels = None, None
    for idx, cluster_size in enumerate(CLUSTER_SIZES):
        ax = plot_cstr_bound(idx, cluster_size)
        if idx == len(CLUSTER_SIZES) - 1:
            handles, labels = ax.get_legend_handles_labels()

    # single legend group on top
    make_legend(fig, handles, labels)

    plt.tight_layout(pad=1.0)
    plt.savefig(f"results/cstr_bounds.png", dpi=300)


if __name__ == "__main__":
    plot_all_cstr_bounds()
