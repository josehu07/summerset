import sys
import os
import toml

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from proc import run_process, run_process_over_ssh
from net import lookup_dns_to_ip


DEFAULT_USER = "smr"

DEFAULT_TOML_FILENAME = "scripts/remote_hosts.toml"


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


def parse_toml_file(group, filename=DEFAULT_TOML_FILENAME):
    hosts_config = toml.load(filename)
    base = hosts_config["base_path"]
    repo = hosts_config["repo_name"]

    if group not in hosts_config:
        print(f"ERROR: invalid hosts group name '{group}'")
        sys.exit(1)
    remotes = hosts_config[group]
    for host in remotes:
        if (not host.startswith("host")) or (not host[4:].isdigit()):
            raise ValueError(f"invalid remote host name {host}")
        if "@" not in remotes[host]:
            remotes[host] = DEFAULT_USER + "@" + remotes[host]

    hosts = sorted(list(remotes.keys()), key=lambda h: int(h[4:]))
    domains = {
        name: split_remote_string(remote)[1] for name, remote in remotes.items()
    }
    ipaddrs = {
        name: lookup_dns_to_ip(domain) for name, domain in domains.items()
    }

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


class PairsMap:
    def __init__(self, pairs, default=None):
        self.pairs = dict()
        self.default = default

        for (na, nb), val in pairs.items():
            assert isinstance(na, int) and isinstance(nb, int)
            assert isinstance(val, int) and val >= 2

            pair = frozenset({f"host{na}", f"host{nb}"})
            if pair in self.pairs:
                raise ValueError(
                    f"duplicate unordered key pair found: {(na, nb)}"
                )

            self.pairs[pair] = val

    def halved(self):
        res = self
        for pair in res.pairs:
            res.pairs[pair] //= 2
        if res.default is not None:
            res.default //= 2
        return res

    def get(self, ha, hb):
        pair = frozenset({ha, hb})
        if pair in self.pairs:
            return self.pairs[pair]
        else:
            return self.default
