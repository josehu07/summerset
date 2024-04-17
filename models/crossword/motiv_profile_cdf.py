import pickle

import matplotlib  # type: ignore

matplotlib.use("Agg")

# from brokenaxes import brokenaxes  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
from matplotlib.lines import Line2D  # type: ignore


TIDB_DATA_SIZE = 73
CRDB_DATA_SIZE = 16


def preprocess_len_cnts(len_cnts, excludes=5):
    print("  Distinct lengths:", len(len_cnts))
    print("  Min & Max:", min(len_cnts.keys()), max(len_cnts.keys()))

    sorted_len_cnts = sorted(len_cnts.items(), key=lambda t: t[1])

    print("  Top 10:")
    tops = reversed(sorted_len_cnts[-10:])
    for l, c in tops:
        print(f"{l:10d} {c:7d}")

    print("  Bottom 10:")
    bottoms = sorted_len_cnts[:10]
    for l, c in bottoms:
        print(f"{l:10d} {c:7d}")

    for l, _ in sorted_len_cnts[-excludes:]:
        del len_cnts[l]

    total, large = 0, 0
    for l, c in len_cnts.items():
        total += c
        if l >= 4 * 1024:
            large += c
    print(f"  Fraction >= 4K: {large / total:.2f}")


def plot_len_cnts_cdfs(len_cnts_tidb, len_cnts_crdb):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.6, 1.05),
            "font.size": 10,
        }
    )
    fig = plt.figure("cdf")

    DBS_DATA_COLOR_ZORDER_ENDX = {
        "TiDB": (len_cnts_tidb, "steelblue", 10, 116 * 1024),
        "CockroachDB": (len_cnts_crdb, "lightcoral", 5, 148 * 1024),
    }

    append_xticks, append_xticklabels = [], []
    for db, (len_cnts, color, zorder, endx) in DBS_DATA_COLOR_ZORDER_ENDX.items():
        x, xmax, xmin = [], 0, float("inf")
        for l, c in len_cnts.items():
            x += [l for _ in range(c)]
            if l > xmax:
                xmax = l
            if l < xmin:
                xmin = l

        xright = 150 * 1024
        step = int(endx / 4096)
        bins = [i * step for i in range(4096)] + [float("inf")]

        plt.hist(
            x,
            bins=bins,
            range=(0, endx),
            density=True,
            cumulative=True,
            histtype="step",
            linewidth=2,
            color=color,
            label=db,
            zorder=zorder,
        )

        adjusted_endx = endx if db == "TiDB" else endx - 2000  # hardcoded
        endx_label = "290KB" if db == "TiDB" else "53MB"  # hardcoded
        plt.vlines(
            adjusted_endx,
            ymin=0.9,
            ymax=1.05,
            colors=color,
            linestyles="solid",
            zorder=zorder,
            linewidth=2,
        )
        plt.vlines(
            adjusted_endx,
            ymin=0,
            ymax=0.9,
            colors="gray",
            linestyles="dashed",
            linewidth=1,
        )

        append_xticks.append(adjusted_endx)
        append_xticklabels.append(endx_label)

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim(0, xright)
    plt.xticks(
        [4096, 32 * 1024, 64 * 1024] + append_xticks,
        ["4KB", "32KB", "64KB"] + append_xticklabels,
    )

    plt.ylim(0, 1.05)
    plt.yticks([0.5, 1.0])

    plt.vlines(
        4096, ymin=0, ymax=1.05, colors="dimgray", linestyles="dashed", zorder=20
    )
    # plt.arrow(
    #     7500,
    #     0.84,
    #     0,
    #     0.17,
    #     color="dimgray",
    #     width=220,
    #     length_includes_head=True,
    #     head_width=1500,
    #     head_length=0.07,
    #     zorder=20,
    # )
    # plt.arrow(
    #     7500,
    #     0.84,
    #     0,
    #     -0.16,
    #     color="dimgray",
    #     width=220,
    #     length_includes_head=True,
    #     head_width=1500,
    #     head_length=0.07,
    #     zorder=20,
    # )
    plt.text(7000, 0.25, "45% & 17% are ≥ 4KB, resp.", color="dimgray", fontsize=9.5)

    # arrow = patches.FancyArrowPatch(
    #     (96 * 1024, 0.45),
    #     (xright, 0.25),
    #     connectionstyle="arc3,rad=.12",
    #     arrowstyle="Simple, tail_width=0.1, head_width=2.5, head_length=4",
    #     color="dimgray",
    # )
    # ax.add_patch(arrow)
    # plt.text(89000, 0.55, "max 290KB", color="dimgray")

    def draw_xaxis_break(xloc):
        xpl, xpr = xloc - 3.5, xloc + 3.5
        xs = [xpl * 1024, xpl * 1024, xpr * 1024, xpr * 1024]
        ys = [-0.1, 0.1, 0.1, -0.1]
        plt.fill(xs, ys, "w", fill=True, linewidth=0, zorder=10, clip_on=False)
        plt.plot(
            [(xpl - 1) * 1024, (xpl + 1) * 1024],
            [-0.1, 0.1],
            color="k",
            linewidth=1,
            zorder=20,
            clip_on=False,
        )
        plt.plot(
            [(xpr - 1) * 1024, (xpr + 1) * 1024],
            [-0.1, 0.1],
            color="k",
            linewidth=1,
            zorder=20,
            clip_on=False,
        )
        plt.text(
            xloc * 1024,
            0,
            "~",
            fontsize=8,
            zorder=30,
            clip_on=False,
            ha="center",
            va="center",
        )

    draw_xaxis_break(100)
    draw_xaxis_break(131)

    plt.ylabel("CDF")

    plt.tight_layout()

    pdf_name = "results/intros/motiv_profile_cdf.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_len_cnts_legend(handles, labels):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.8, 1.3),
            "font.size": 10,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    line_handles = [
        Line2D([0], [0], linewidth=2, color=h.get_edgecolor()) for h in handles
    ]

    lgd = plt.legend(
        line_handles,
        labels,
        handlelength=0.6,
        handletextpad=0.4,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
    )

    pdf_name = "results/intros/legend-motiv_profile.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    len_cnts_tidb, len_cnts_crdb = None, None
    with open("results/intros/length_counts-tidb.pkl", "rb") as fpkl:
        len_cnts_tidb = pickle.load(fpkl)
    print("TiDB --")
    preprocess_len_cnts(len_cnts_tidb)
    print()

    with open("results/intros/length_counts-crdb.pkl", "rb") as fpkl:
        len_cnts_crdb = pickle.load(fpkl)
    len_cnts_crdb = {
        int(l * TIDB_DATA_SIZE / CRDB_DATA_SIZE): c for l, c in len_cnts_crdb.items()
    }
    print("CockroachDB --")
    preprocess_len_cnts(len_cnts_crdb)
    print()

    handles, labels = plot_len_cnts_cdfs(len_cnts_tidb, len_cnts_crdb)
    plot_len_cnts_legend(handles, labels)
