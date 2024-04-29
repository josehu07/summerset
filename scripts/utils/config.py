import sys
import os
import toml  # type: ignore

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from proc import run_process, run_process_over_ssh
from net import lookup_dns_to_ip


def parse_comma_separated(l):
    l = l.strip().split(",")
    if len(l) == 0:
        raise ValueError("comma separated list is empty")
    for seg in l:
        if len(seg) == 0:
            raise ValueError(f"comma separated list has empty segment: {l}")
    return l


def split_remote_string(remote):
    if "@" not in remote:
        raise ValueError(f"invalid remote string '{remote}'")
    segs = remote.strip().split("@")
    if len(segs) != 2 or len(segs[0]) == 0 or len(segs[1]) == 0:
        raise ValueError(f"invalid remote string '{remote}'")
    return segs[0], segs[1]


def parse_toml_file(filename, group):
    hosts_config = toml.load(filename)
    base = hosts_config["base_path"]
    repo = hosts_config["repo_name"]

    if group not in hosts_config:
        print(f"ERROR: invalid hosts group name '{group}'")
        sys.exit(1)
    remotes = hosts_config[group]

    hosts = sorted(list(remotes.keys()))
    domains = {name: split_remote_string(remote)[1] for name, remote in remotes.items()}
    ipaddrs = {name: lookup_dns_to_ip(domain) for name, domain in domains.items()}

    return base, repo, hosts, remotes, domains, ipaddrs


def check_remote_is_me(remote):
    proc_l = run_process(["hostname"], capture_stdout=True, print_cmd=False)
    out_l, _ = proc_l.communicate()
    hostname_l = out_l.decode().strip()

    proc_r = run_process_over_ssh(
        remote, ["hostname"], capture_stdout=True, print_cmd=False
    )
    out_r, _ = proc_r.communicate()
    hostname_r = out_r.decode().strip()

    if hostname_l != hostname_r:
        raise RuntimeError(f"remote {remote} is not me")
