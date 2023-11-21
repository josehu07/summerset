import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"


def ssh_to_remote(remote):
    os.execvp("ssh", ["ssh", remote])


if __name__ == "__main__":
    utils.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        required=True,
        help="single remote host's nickname",
    )
    args = parser.parse_args()

    hosts_config = utils.read_toml_file(TOML_FILENAME)
    hosts = hosts_config["hosts"]

    if args.target not in hosts:
        raise ValueError(f"nickname '{args.target}' not found in toml file")
    ssh_to_remote(hosts[args.target])
