import argparse
import math
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.legend_handler import HandlerPatch


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
        m,
        m,
        marker="s",
        s=100,
        color="black",
        label="Classic Paxos/Raft",
        zorder=10,
    )

    # CRaft point
    craft_q = math.ceil((n + m) / 2)
    plt.scatter(
        craft_q,
        1,
        marker="X",
        s=110,
        color="lightcoral",
        label="RSPaxos/CRaft",
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
    if n <= 5:
        plt.vlines(
            m, ymin=m, ymax=n, linestyles="-", color=line_color, zorder=20
        )
        plt.vlines(
            n, ymin=1, ymax=n, linestyles="-", color=line_color, zorder=20
        )
        plt.hlines(
            n,
            xmin=m - 0.05,
            xmax=n + 0.05,
            linestyles="-",
            color=line_color,
            zorder=20,
        )
    else:
        plt.vlines(
            m, ymin=m, ymax=m + 1.4, linestyles="-", color=line_color, zorder=20
        )
        plt.vlines(
            n, ymin=1, ymax=m + 1.4, linestyles="-", color=line_color, zorder=20
        )

    # correct region
    xs = [m, m, n, n]
    ys = [m, n, n, 1] if n <= 5 else [m, m + 1.4, m + 1.4, 1]
    plt.fill(xs, ys, color=fill_color, label="Region of tolerance=f", zorder=0)

    # unused x-axis ranges
    xs = [0.4, m - 0.55, m - 0.85, 0.1]
    ys = [0.3, 0.3, 0, 0]
    plt.fill(xs, ys, hatch="///", fill=False, linewidth=0, zorder=10)
    if cluster_size < CLUSTER_SIZES[-1]:
        xs = [n + 1, X_TICKS[-1] + 0.4, X_TICKS[-1] + 0.1, n + 0.7]
        plt.fill(xs, ys, hatch="///", fill=False, linewidth=0, zorder=10)

    # unused y-axis range for 3
    if cluster_size == CLUSTER_SIZES[0]:
        xs = [0, 0.3, 0.3, 0]
        ys = [n + 0.55, n + 0.85, Y_TICKS[-1] + 0.4, Y_TICKS[-1] + 0.1]
        plt.fill(xs, ys, hatch="///", fill=False, linewidth=0, zorder=10)

    # environment tradeoff arrows
    plt.arrow(
        m + 0.1
        if n <= 3
        else m + 0.1
        if n <= 5
        else m - 0.8
        if n <= 7
        else m - 0.4,
        n + 1.1
        if n <= 3
        else m + 2.5
        if n <= 5
        else m + 1.6
        if n <= 7
        else m + 2.0,
        -1.3,
        0,
        linewidth=1,
        color="dimgray",
        length_includes_head=True,
        head_width=0.2,
        head_length=0.25,
        overhang=0.5,
        clip_on=False,
        label="Tradeoff decisions",
        zorder=50,
    )
    plt.text(
        m + 0.3
        if n <= 3
        else m + 0.3
        if n <= 5
        else m - 0.9
        if n <= 7
        else m - 0.2,
        n + 1.1 if n <= 3 else m + 2.6 if n <= 5 else m + 2.1,
        "if high RTT var.",
        horizontalalignment="left",
        verticalalignment="center",
        color="dimgray",
        zorder=50,
    )
    plt.arrow(
        n + 1,
        2,
        0,
        -1.3,
        linewidth=1,
        color="dimgray",
        length_includes_head=True,
        head_width=0.2,
        head_length=0.25,
        overhang=0.5,
        clip_on=False,
    )
    plt.text(
        n + 1.4 if n < 7 else n + 0.4,
        1 + 0.8 if n < 7 else 1 + 2.1,
        "if bw\nlimited",
        horizontalalignment="left",
        verticalalignment="center",
        color="dimgray",
    )

    plt.axis("scaled")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim((0, X_TICKS[-1] + 0.7))
    plt.ylim((0, Y_TICKS[-1] + 2.7))
    plt.xticks(
        X_TICKS[m - 1 : cluster_size],
        list(map(str, X_TICKS))[m - 1 : cluster_size],
    )
    plt.yticks(Y_TICKS[:cluster_size], list(map(str, Y_TICKS))[:cluster_size])

    if idx < 1:
        plt.xlabel("|Quorum| (q)", loc="right")
        ax.xaxis.set_label_coords(1.15, -0.06)
    else:
        plt.xlabel("q")
        ax.xaxis.set_label_coords(1.06, 0.06)
    plt.ylabel(
        "Shards per\nserver (c)",
        loc="top",
        rotation=0,
        backgroundcolor="white",
        zorder=10,
    )
    ax.yaxis.set_label_coords(0.19, 0.76)

    # plt.title(
    #     f"|Cluster|={n}  f={f}",
    #     x=0.5,
    #     y=-0.48,
    #     fontsize=11,
    #     # fontweight="bold",
    #     # backgroundcolor=fill_color,
    # )
    plt.text(5.6, -2.7, f"n={n}, f={f}", fontsize=13, ha="center", va="center")
    plt.text(
        2.2, -2.7, "▬", fontsize=13, color=line_color, ha="center", va="center"
    )

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
            overhang=0.2,
        )

    def make_legend_polygon(
        legend, orig_handle, xdescent, ydescent, width, height, fontsize
    ):
        return mpatches.Polygon(
            xy=np.array(
                [
                    [0.2 * width, 0.5 * height],
                    [0.2 * width, 1.2 * height],
                    [0.8 * width, 1.2 * height],
                    [0.8 * width, -0.2 * height],
                ]
            ),
            closed=True,
            color="dimgray",
        )

    order = []
    for s in ("Classic", "RSPaxos", "Crossword", "Region", "Tradeoff"):
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
        bbox_to_anchor=(0.5, 0.78),
        ncol=len(handles),
        handlelength=1.2,
        columnspacing=1.2,
        handletextpad=0.4,
        handler_map={
            mpatches.FancyArrow: HandlerPatch(patch_func=make_legend_arrow),
            mpatches.Polygon: HandlerPatch(patch_func=make_legend_polygon),
        },
    )
    for h in leg.legend_handles[2:]:
        h.set_color("dimgray")


def plot_all_cstr_bounds(output_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (10, 3),
            "font.size": 12,
            "axes.axisbelow": False,
            "pdf.fonttype": 42,
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

    plt.tight_layout(pad=0.3)
    plt.savefig(f"{output_dir}/cstr_bounds.pdf", bbox_inches=0)


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="./results",
        help="output folder",
    )
    args = parser.parse_args()

    plot_all_cstr_bounds(args.output_dir)


if __name__ == "__main__":
    main()
