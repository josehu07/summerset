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


def parse_profile_results(collect_path):
    profile = dict()
    for log in os.listdir(collect_path):
        with open(f"{collect_path}/{log}", "r") as f:
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
        os.system(f"rm -rf {collect_path}")
        os.system(f"mkdir -p {collect_path}")

        _, _, _, remotes, _, _ = utils.config.parse_toml_file(
            TOML_FILENAME, PHYS_ENV_GROUP
        )

        for db_sys in ("cockroach",):
            saved_logs_path = f"{collect_path}/{db_sys}"
            pickle_path = f"{collect_path}/length_counts-{db_sys}.pkl"
            os.system(f"mkdir -p {saved_logs_path}")

            collect_profile_logs(remotes, PROFILE_LOGS_DIR(db_sys), saved_logs_path)
            profile = parse_profile_results(saved_logs_path)
            dump_profile_len_cnts(profile, pickle_path)

    else:
        # analyzing & plotting results
        if not os.path.isdir(collect_path):
            raise RuntimeError(f"collect directory {collect_path} does not exist")

        # for db_sys in ("tidb", "cockroach"):
        for db_sys in ("cockroach",):
            pickle_path = f"{collect_path}/length_counts-{db_sys}.pkl"
            with open(pickle_path, "rb") as fpkl:
                len_cnts = pickle.load(fpkl)

            print(f"{db_sys} --")
            preprocess_len_cnts(len_cnts)
            print()
