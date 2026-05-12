import os
import argparse

from . import utils


def ssh_to_remote(remote, no_cd, base, repo, identity):
    ssh_args = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if identity is not None:
        ssh_args += ["-i", identity]
        
    if no_cd:
        ssh_args.append(remote)
    else:
        ssh_args += ["-t", remote, f"cd {base}/{repo}; bash --login"]

    print(" ".join(ssh_args))
    os.execvp("ssh", ssh_args)


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
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
    parser.add_argument(
        "-i",
        "--identity",
        type=str,
        default=None,
        help="path to ssh identity key file",
    )
    args = parser.parse_args()

    base, repo, _, remotes, _, _ = utils.config.parse_toml_file(args.group)

    if args.target not in remotes:
        raise ValueError(f"nickname '{args.target}' not found in toml file")
    ssh_to_remote(remotes[args.target], args.no_cd, base, repo, args.identity)


if __name__ == "__main__":
    main()
