import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from proc import run_process


def set_tc_qdisc_netem(netns, dev, mean, jitter, rate, distribution="pareto"):
    QLEN_LIMIT = 500000000
    delay_args = f"delay {mean}ms" if mean > 0 else ""
    jitter_args = (
        f"{jitter}ms distribution {distribution}" if mean > 0 and jitter > 0 else ""
    )
    rate_args = f"rate {rate}gibit" if rate > 0 else ""
    os.system(
        f"sudo ip netns exec {netns} tc qdisc replace dev {dev} root netem"
        f" limit {QLEN_LIMIT} {delay_args} {jitter_args} {rate_args}"
    )


def set_all_tc_qdisc_netems(
    num_replicas,
    netns,
    dev,
    ifb,
    mean,
    jitter,
    rate,
    distribution="pareto",
    involve_ifb=False,
):
    for replica in range(num_replicas):
        set_tc_qdisc_netem(
            netns(replica),
            dev(replica),
            mean(replica),
            jitter(replica),
            rate(replica) * 2 if involve_ifb else rate(replica),
            distribution=distribution,
        )
        set_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
            0,
            0,
            rate(replica) * 2 if involve_ifb else 0,
        )


def clear_tc_qdisc_netem(netns, dev):
    os.system(f"sudo ip netns exec {netns} tc qdisc delete dev {dev} root")


def clear_all_tc_qdisc_netems(num_replicas, netns, dev, ifb):
    for replica in range(num_replicas):
        clear_tc_qdisc_netem(
            netns(replica),
            dev(replica),
        )
        clear_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
        )


def lookup_dns_to_ip(domain):
    proc = run_process(["dig", "+short", domain], capture_stdout=True, print_cmd=False)
    out, _ = proc.communicate()
    out = out.decode().strip()
    if len(out) == 0:
        raise RuntimeError(f"dns lookup for {domain} failed")
    ip = out.split("\n")[0]
    assert ip.count(".") == 3
    return ip
