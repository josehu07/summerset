import sys
import os
import argparse
import time
import random
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "wan"

EXPER_NAME_DATA = "loc_grid_wan"
EXPER_NAME_PLOT = "latency_cdfs"

NUM_REPLICAS = 5
NUM_CLIENTS = 5

LENGTH_SECS = 150
RESULT_SECS_BEGIN = 30
RESULT_SECS_END = 130

PROTOCOLS_BSNAME = {
    "MultiPaxos": "MultiPaxos",
    "LeaderLs": "MultiPaxos",
    "EPaxos": "EPaxos",
    "PQR": "MultiPaxos",
    "PQRLeaderLs": "MultiPaxos",
    "QuorumLs": "QuorumLeases",
    "QuorumLsCtn": "QuorumLeases",
    "Bodega": "Bodega",
}

# NOTE: this script reuses loc_grid_wan results and just plots the latency CDFs
#       for interesting settings.
SETTINGS_RATIO_RW = [
    (0, True),
    (1, True),
    (10, True),
    (1, False),
]


def collect_outputs(output_dir):
    results = dict()
    for put_ratio, is_read in SETTINGS_RATIO_RW:
        results[(put_ratio, is_read)] = dict()
        for pcname, protocol in PROTOCOLS_BSNAME.items():
            results[(put_ratio, is_read)][pcname] = []  # will be a list of 5 lists

            for cgroup in range(NUM_REPLICAS):
                result = utils.output.gather_outputs(
                    f"{protocol}.{pcname}.{put_ratio}",
                    NUM_CLIENTS,
                    output_dir,
                    RESULT_SECS_BEGIN,
                    RESULT_SECS_END,
                    0.1,
                    client_start=cgroup,
                    client_step=NUM_REPLICAS,
                )

                sd, sp, sj, sm = 10, 0, 0, 1
                # setting sm here to compensate for unstabilities of printing
                # things to console
                lat_list = utils.output.list_smoothing(
                    [
                        v
                        for v in result["rlat_avg" if is_read else "wlat_avg"]
                        if v > 0.0
                    ],
                    sd,
                    sp,
                    sj,
                    1 / sm,
                )

                results[(put_ratio, is_read)][pcname].append(lat_list)

    for put_ratio, is_read in SETTINGS_RATIO_RW:
        for pcname in PROTOCOLS_BSNAME:
            # equalize the share across 5 client locations
            lat_lists = results[(put_ratio, is_read)][pcname]
            merged = []
            max_samples = max(map(len, lat_lists))
            for lat_list in lat_lists:
                merged.extend(lat_list)
                supplement = max_samples - len(lat_list)
                if supplement > 0:
                    merged.extend(random.choices(lat_list, k=supplement))

            merged = sorted(lat / 1000 for lat in merged)

            results[(put_ratio, is_read)][pcname] = {
                "sorted": merged,
                "min": (None if len(merged) == 0 else merged[0]),
                "p25": (None if len(merged) == 0 else np.percentile(merged, 25)),
                "p50": (None if len(merged) == 0 else np.percentile(merged, 50)),
                "p75": (None if len(merged) == 0 else np.percentile(merged, 75)),
                "p99": (None if len(merged) == 0 else np.percentile(merged, 99)),
                "max": (None if len(merged) == 0 else merged[-1]),
                "mean": (None if len(merged) == 0 else sum(merged) / len(merged)),
            }

    return results, None


def print_results(results):
    for put_ratio, is_read in SETTINGS_RATIO_RW:
        print(f"puts {put_ratio}  {'read' if is_read else 'write'}")
        for pcname, result in results[(put_ratio, is_read)].items():
            print(f"  {pcname}")
            print(
                f"    mean {result['mean']:7.2f}"
                f"  min {result['min']:7.2f}"
                f"  p25 {result['p25']:7.2f}"
                f"  p50 {result['p50']:7.2f}"
                f"  p75 {result['p75']:7.2f}"
                f"  p99 {result['p99']:7.2f}"
                f"  max {result['max']:7.2f}"
            )


def plot_latency_cdfs(results, put_ratio, is_read, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (2.6, 2.0),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Exper-{put_ratio}-{is_read}")

    PCNAMES_ORDER = [
        "MultiPaxos",
        "LeaderLs",
        "EPaxos",
        "PQR",
        "PQRLeaderLs",
        "QuorumLs",
        "QuorumLsCtn",
        "Bodega",
    ]
    PCNAMES_LABEL_COLOR_STYLE_MARKER_SIZE = {
        "MultiPaxos": ("MultiPaxos", "gray", ":", "o", 3.5),
        "LeaderLs": ("Leader Leases", "0.4", "-", "v", 3.5),
        "EPaxos": ("EPaxos", "lightseagreen", "-", "s", 3.0),
        "PQR": ("PQR", "palevioletred", ":", "d", 3.8),
        "PQRLeaderLs": ("PQR (+ Ldr Ls)", "firebrick", "-", "^", 3.5),
        "QuorumLs": ("Quorum Leases", "mediumseagreen", ":", "X", 3.8),
        "QuorumLsCtn": ("Qrm Ls (passive)", "forestgreen", "-", "p", 3.8),
        "Bodega": ("Bodega", "royalblue", "-", "*", 4.4),
    }

    NUM_MARKERS_PER_SERIES = 11

    for pcname in PCNAMES_ORDER:
        label, color, linestyle, marker, markersize = (
            PCNAMES_LABEL_COLOR_STYLE_MARKER_SIZE[pcname]
        )
        lat_list = results[(put_ratio, is_read)][pcname]["sorted"]
        assert len(lat_list) >= NUM_MARKERS_PER_SERIES

        cdf = plt.ecdf(
            lat_list,
            color=color,
            linewidth=1.2,
            linestyle=linestyle,
            marker=marker,
            markersize=markersize,
            markevery=len(lat_list) // NUM_MARKERS_PER_SERIES,
            label=label,
            zorder=10,
        )

    ax = fig.axes[0]
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.xlim(left=-2)
    plt.xlabel("Latency (ms)")

    plt.ylim(bottom=0.0, top=1.03)
    yticks = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]
    plt.yticks(yticks, list(map(str, yticks)))

    plt.grid(which="major", axis="both", color="lightgray", zorder=5)

    plt.tight_layout()

    pdf_name = (
        f"{plots_dir}/exper-{EXPER_NAME_PLOT}-"
        f"w{put_ratio}-{'rd' if is_read else 'wr'}.pdf"
    )
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


def plot_ylabel(plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (1.0, 2.5),
            "font.size": 9,
            "pdf.fonttype": 42,
        }
    )
    fig = plt.figure(f"Ylabel")

    ylabel = "CDF"

    plt.ylabel(ylabel)
    ax = fig.axes[0]
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.xticks([0], ["Dumb"])
    ax.get_xticklabels()[0].set_alpha(0.0)  # don't show
    plt.tick_params(bottom=False, left=False, labelleft=False)

    fig.align_labels()

    fig.tight_layout()

    pdf_name = f"{plots_dir}/ylabel-{EXPER_NAME_PLOT}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


def plot_legend(handles, labels, plots_dir):
    matplotlib.rcParams.update(
        {
            "figure.figsize": (12.0, 1.0),
            "font.size": 10,
            "pdf.fonttype": 42,
        }
    )
    plt.figure("Legend")

    plt.axis("off")

    # reverse the order of legends?
    handles = handles[::-1]
    labels = labels[::-1]

    lgd = plt.legend(
        handles,
        labels,
        handleheight=0.8,
        handlelength=1.4,
        markerscale=1.6,
        loc="center",
        bbox_to_anchor=(0.5, 0.5),
        ncol=len(labels),
        borderpad=0.3,
        handletextpad=0.5,
        columnspacing=2.0,
        frameon=False,
    )

    pdf_name = f"{plots_dir}/legend-{EXPER_NAME_PLOT}.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default="./results",
        help="directory to hold outputs and logs",
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        os.system(f"mkdir -p {args.odir}")

    if not args.plot:
        print("ERROR: this script uses loc_grid_wan exper's outputs!")

    else:
        output_dir = f"{args.odir}/output/{EXPER_NAME_DATA}"
        plots_dir = f"{args.odir}/plots/{EXPER_NAME_PLOT}"
        if not os.path.isdir(plots_dir):
            os.system(f"mkdir -p {plots_dir}")

        results, _ = collect_outputs(output_dir)
        print_results(results)

        for put_ratio, is_read in SETTINGS_RATIO_RW:
            handles, labels = plot_latency_cdfs(results, put_ratio, is_read, plots_dir)
        plot_ylabel(plots_dir)
        plot_legend(handles, labels, plots_dir)
