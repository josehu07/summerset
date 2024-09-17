import sys
import os
import argparse
import pickle

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import utils

# fmt: off
import matplotlib  # type: ignore
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore
# fmt: on


TOML_FILENAME = "scripts/remote_hosts.toml"
PHYS_ENV_GROUP = "1dc"

PROFILE_LOGS_DIR = lambda db: f"/tmp/{db}/size-profiles"


def collect_profile_logs(remotes, remote_logs_path, saved_logs_path):
    print("Fetching size profile logs...")
    for remote in remotes.values():
        print(f"  from {remote}")
        utils.file.fetch_files_of_dir(remote, remote_logs_path, saved_logs_path)


def parse_profile_results(saved_logs_path):
    profile = dict()
    for log_file in os.listdir(saved_logs_path):
        # the first bunch of ranges are 'system' db ranges and should be excluded!
        # otherwise, small non-tpcc replication messages will dominate the profile
        range_id = int(log_file.split("-")[0])
        if range_id < 70:  # NOTE: currently hardcoded for our profile setup
            continue

        with open(f"{saved_logs_path}/{log_file}", "r") as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue
                if not line.startswith("num_"):
                    segs = line.split(",")

                    num_entries, num_bytes, repeat_cnt = (
                        int(segs[0]),
                        int(segs[1]),
                        int(segs[2]),
                    )
                    if (num_entries, num_bytes) not in profile:
                        profile[(num_entries, num_bytes)] = repeat_cnt
                    else:
                        profile[(num_entries, num_bytes)] += repeat_cnt

    return profile


def dump_profile_len_cnts(profile, pickle_path):
    print()
    print("Profile results summary:")

    total_samples = sum(profile.values())
    print(f"  total samples:  {sum(profile.values())}")
    print(f"  max num_entries:  {max(k[0] for k in profile.keys())}")
    print(f"  max num_bytes:  {max(k[1] for k in profile.keys())}")

    sorted_profile = [(k[0], k[1], v) for k, v in profile.items()]
    sorted_profile.sort(key=lambda tup: tup[2], reverse=True)
    print(f"  top 10 occurrences:")
    for i in range(10):
        num_entries, num_bytes, repeat_cnt = sorted_profile[i]
        percentage = int(repeat_cnt / total_samples * 100)
        print(
            f"    {num_entries:5d}  {num_bytes:10d}  {repeat_cnt:7d}  {percentage:2d}%"
        )

    len_cnts = dict()
    for (_, num_bytes), repeat_cnt in profile.items():
        if num_bytes not in len_cnts:
            len_cnts[num_bytes] = repeat_cnt
        else:
            len_cnts[num_bytes] += repeat_cnt

    with open(pickle_path, "wb") as fpkl:
        pickle.dump(len_cnts, fpkl)


def preprocess_len_cnts(len_cnts, excludes=1):
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
    print()


def plot_len_cnts_cdfs(len_cnts_tidb, len_cnts_crdb, output_dir):
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
            if l > xmax:
                xmax = l
            if l < xmin:
                xmin = l

            # account for manually drawn axis breaks
            draw_l = l
            if db == "TiDB" and l > 95 * 1024 and l < 280 * 1024:
                draw_l = 100 * 1024
            if db == "CockroachDB" and l > 300 * 1024 and l < 60 * 1024 * 1024:
                draw_l = 131 * 1024
            x += [draw_l for _ in range(c)]

        xright = 150 * 1024
        step = int(endx / 8192)
        bins = [i * step for i in range(8192)] + [endx, float("inf")]

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
        endx_label = "290KB" if db == "TiDB" else "63MB"
        plt.vlines(
            endx,
            ymin=0.92,
            ymax=1.05,
            colors=color,
            linestyles="solid",
            zorder=zorder,
            linewidth=2,
        )
        plt.vlines(
            endx,
            ymin=0,
            ymax=0.92,
            colors="gray",
            linestyles="dashed",
            linewidth=1,
        )

        append_xticks.append(endx)
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
    plt.text(16000, 0.22, "~45% are ≥ 4KB", color="dimgray", fontsize=9.5)

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

    pdf_name = f"{output_dir}/motiv_profile_cdf.pdf"
    plt.savefig(pdf_name, bbox_inches=0)
    plt.close()
    print(f"Plotted: {pdf_name}")

    return ax.get_legend_handles_labels()


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-o",
        "--odir",
        type=str,
        default=f"./results",
        help="directory to hold outputs and logs",
    )
    parser.add_argument(
        "-p", "--plot", action="store_true", help="if set, do the plotting phase"
    )
    args = parser.parse_args()

    if not os.path.isdir(args.odir):
        raise RuntimeError(f"results directory {args.odir} does not exist")
    collect_path = f"{args.odir}/intros"

    if not args.plot:
        # downloading raw profiled logs
        os.system(f"mkdir -p {collect_path}")
        _, _, _, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )

        for db_sys in ("cockroach",):
            saved_logs_path = f"{collect_path}/{db_sys}"
            pickle_path = f"{collect_path}/length_counts-{db_sys}.pkl"
            os.system(f"rm -rf {saved_logs_path}")
            os.system(f"mkdir -p {saved_logs_path}")

            collect_profile_logs(remotes, PROFILE_LOGS_DIR(db_sys), saved_logs_path)
            profile = parse_profile_results(saved_logs_path)
            dump_profile_len_cnts(profile, pickle_path)

    else:
        # analyzing & plotting results
        if not os.path.isdir(collect_path):
            raise RuntimeError(f"collect directory {collect_path} does not exist")

        all_len_cnts = dict()
        for db_sys in ("tidb", "cockroach"):
            pickle_path = f"{collect_path}/length_counts-{db_sys}.pkl"
            with open(pickle_path, "rb") as fpkl:
                len_cnts = pickle.load(fpkl)

            print(f"{db_sys} --")
            preprocess_len_cnts(len_cnts, excludes=5 if db_sys == "tidb" else 1)
            all_len_cnts[db_sys] = len_cnts

        plot_len_cnts_cdfs(
            all_len_cnts["tidb"], all_len_cnts["cockroach"], collect_path
        )
