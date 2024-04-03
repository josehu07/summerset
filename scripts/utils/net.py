import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from proc import run_process, run_process_over_ssh


def set_tc_qdisc_netem(
    netns, dev, mean, jitter, rate, distribution="pareto", remote=None
):
    QLEN_LIMIT = 500000000
    delay_args = f"delay {mean}ms" if mean > 0 else ""
    jitter_args = (
        f"{jitter}ms distribution {distribution}" if mean > 0 and jitter > 0 else ""
    )
    rate_args = f"rate {rate}gibit" if rate > 0 else ""
    cmd = [
        "sudo",
        "ip",
        "netns",
        "exec",
        netns,
        "tc",
        "qdisc",
        "replace",
        "dev",
        dev,
        "root",
        "netem",
        "limit",
        str(QLEN_LIMIT),
        delay_args,
        jitter_args,
        rate_args,
    ]
    if remote is None:
        run_process(cmd).wait()
    else:
        run_process_over_ssh(
            remote,
            cmd,
            print_cmd=False,
        ).wait()


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
    remote=None,
):
    for replica in range(num_replicas):
        set_tc_qdisc_netem(
            netns(replica),
            dev(replica),
            mean(replica),
            jitter(replica),
            rate(replica) * 2 if involve_ifb else rate(replica),
            distribution=distribution,
            remote=remote,
        )
        set_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
            0,
            0,
            rate(replica) * 2 if involve_ifb else 0,
            remote=remote,
        )


def clear_tc_qdisc_netem(netns, dev, remote=None):
    cmd = [
        "sudo",
        "ip",
        "netns",
        "exec",
        netns,
        "tc",
        "qdisc",
        "delete",
        "dev",
        dev,
        "root",
    ]
    if remote is None:
        run_process(cmd).wait()
    else:
        run_process_over_ssh(
            remote,
            cmd,
            print_cmd=False,
        ).wait()


def clear_all_tc_qdisc_netems(num_replicas, netns, dev, ifb, remote=None):
    for replica in range(num_replicas):
        clear_tc_qdisc_netem(
            netns(replica),
            dev(replica),
            remote=remote,
        )
        clear_tc_qdisc_netem(
            netns(replica),
            ifb(replica),
            remote=remote,
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


def get_interface_name(ip, remote=None):
    proc = None
    if remote is None:
        proc = run_process(
            ["ip", "-f", "inet", "a"], capture_stdout=True, print_cmd=False
        )
    else:
        proc = run_process_over_ssh(
            remote, ["ip", "-f", "inet", "a"], capture_stdout=True, print_cmd=False
        )
    out, _ = proc.communicate()
    out = out.decode().strip()

    interface = None
    for line in out.split("\n"):
        segs = line.strip().split()
        if len(segs) > 1 and segs[0].endswith(":"):
            interface = segs[1][:-1]
        elif interface is not None:
            if segs[0] == "inet" and segs[1].startswith(ip):
                return interface

    return None
