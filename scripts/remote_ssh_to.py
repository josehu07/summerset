import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"


def ssh_to_remote(remote, no_cd, base, repo):
    ssh_args = ["ssh"]
    if no_cd:
        ssh_args.append(remote)
    else:
        ssh_args += ["-t", remote, f"cd {base}/{repo}; bash --login"]

    os.execvp("ssh", ssh_args)


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-g", "--group", type=str, default="1dc", help="hosts group to run on"
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        required=True,
        help="single remote host's nickname",
    )
    parser.add_argument(
        "-d",
        "--no_cd",
        action="store_true",
        help="if set, don't try changing into repo directory",
    )
    args = parser.parse_args()

    base, repo, _, remotes, _, _ = utils.config.parse_toml_file(
        TOML_FILENAME, args.group
    )

    if args.target not in remotes:
        raise ValueError(f"nickname '{args.target}' not found in toml file")
    ssh_to_remote(remotes[args.target], args.no_cd, base, repo)
