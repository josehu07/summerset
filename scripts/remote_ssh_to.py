import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"


def ssh_to_remote(remote, auto_cd, base, repo):
    ssh_args = ["ssh"]
    if not auto_cd:
        ssh_args.append(remote)
    else:
        ssh_args += ["-t", remote, f"cd {base}/{repo}; bash --login"]

    os.execvp("ssh", ssh_args)


if __name__ == "__main__":
    utils.check_proper_cwd()

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
        "--auto_cd",
        action="store_true",
        help="if set, automatically change into repo directory",
    )
    args = parser.parse_args()

    hosts_config = utils.read_toml_file(TOML_FILENAME)
    base = hosts_config["base_path"]
    repo = hosts_config["repo_name"]
    if args.group not in hosts_config:
        print(f"ERROR: invalid hosts group name '{args.group}'")
        sys.exit(1)
    remotes = hosts_config[args.group]

    if args.target not in remotes:
        raise ValueError(f"nickname '{args.target}' not found in toml file")
    ssh_to_remote(remotes[args.target], args.auto_cd, base, repo)
