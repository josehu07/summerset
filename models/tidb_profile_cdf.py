import pickle

import matplotlib  # type: ignore

matplotlib.use("Agg")

# from brokenaxes import brokenaxes  # type: ignore
import matplotlib.pyplot as plt  # type: ignore
import matplotlib.patches as patches  # type: ignore


def show_len_cnts(len_cnts):
    print("Distinct lengths:", len(len_cnts))
    print("Min & Max:", min(len_cnts.keys()), max(len_cnts.keys()))

    print("Top 20:")
    tops = sorted(len_cnts.items(), key=lambda t: t[1], reverse=True)[:20]
    for l, c in tops:
        print(f"{l:7d} {c:7d}")

    total, large = 0, 0
    for l, c in len_cnts.items():
        total += c
        if l >= 4 * 1024:
            large += c
    print(f"Fraction >= 4K: {large / total:.2f}")


def filter_len_cnts(len_cnts):
    EXCLUDES = {168, 166, 171, 26}
    for l in EXCLUDES:
        del len_cnts[l]


def plot_len_cnts(len_cnts):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (3.6, 1.05),
            "font.size": 10,
        }
    )
    fig = plt.figure("cdf")

    x, xmax, xmin = [], 0, float("inf")
    for l, c in len_cnts.items():
        x += [l for _ in range(c)]
        if l > xmax:
            xmax = l
        if l < xmin:
            xmin = l

    # xlims = ((0, 140000), (xmax - 1000, xmax + 1000))
    # bax = brokenaxes(xlims=xlims)
    xright = 128 * 1024

    # bins = [e for e in range(0, 16 * 1024, 16)]
    # bins += [e for e in range(16 * 1024, xright, 64)]
    # plt.hist(x, bins=bins)

    plt.hist(
        x,
        bins=3000,
        range=(0, xright * 1.05),
        density=True,
        cumulative=True,
        histtype="step",
        linewidth=2,
        color="steelblue",
    )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim(xmin, xright)
    plt.xticks(
        [4096, 32 * 1024, 64 * 1024, 96 * 1024, xright],
        ["4KB", "32KB", "64KB", "96KB", "290KB"],
    )

    plt.ylim(0, 1.05)
    plt.yticks([0.5, 1.0])

    plt.vlines(4096, ymin=0, ymax=1.05, colors="dimgray", linestyles="dashed")
    plt.arrow(
        7500,
        0.84,
        0,
        0.17,
        color="dimgray",
        width=220,
        length_includes_head=True,
        head_width=1500,
        head_length=0.07,
    )
    plt.arrow(
        7500,
        0.84,
        0,
        -0.16,
        color="dimgray",
        width=220,
        length_includes_head=True,
        head_width=1500,
        head_length=0.07,
    )
    plt.text(7000, 0.2, "44% are ≥ 4KB", color="dimgray")

    # arrow = patches.FancyArrowPatch(
    #     (96 * 1024, 0.45),
    #     (xright, 0.25),
    #     connectionstyle="arc3,rad=.12",
    #     arrowstyle="Simple, tail_width=0.1, head_width=2.5, head_length=4",
    #     color="dimgray",
    # )
    # ax.add_patch(arrow)
    # plt.text(89000, 0.55, "max 290KB", color="dimgray")

    xs = [108 * 1024, 108 * 1024, 115 * 1024, 115 * 1024]
    ys = [-0.1, 0.1, 0.1, -0.1]
    plt.fill(xs, ys, "w", fill=True, linewidth=0, zorder=10, clip_on=False)
    plt.plot(
        [107 * 1024, 109 * 1024],
        [-0.1, 0.1],
        color="k",
        linewidth=1,
        zorder=20,
        clip_on=False,
    )
    plt.plot(
        [114 * 1024, 116 * 1024],
        [-0.1, 0.1],
        color="k",
        linewidth=1,
        zorder=20,
        clip_on=False,
    )
    plt.text(108.6 * 1024, -0.1, "~", fontsize=10, zorder=30, clip_on=False)

    plt.ylabel("CDF")

    plt.tight_layout()

    pdf_name = "results/final/tidb/tidb_profile_cdf.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    with open("results/final/tidb/length_counts.pkl", "rb") as fpkl:
        len_cnts = pickle.load(fpkl)

    filter_len_cnts(len_cnts)
    show_len_cnts(len_cnts)
    plot_len_cnts(len_cnts)
