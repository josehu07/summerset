import time
import argparse

from . import utils


IPERF_PORT = 37777
IPERF_SECS = 10

PING_SECS = 5


PAIRS_NETEM_RTT = utils.config.PairsMap(
    # Ref: https://www.usenix.org/system/files/nsdi21-tollman.pdf#page=7
    {
        (0, 1): 77,
        (0, 2): 129,
        (0, 3): 137,
        (0, 4): 221,
        (1, 2): 59,
        (1, 3): 64,
        (1, 4): 146,
        (2, 3): 26,
        (2, 4): 91,
        (3, 4): 98,
    },
    default=0,
)
PAIRS_NETEM_MEAN = PAIRS_NETEM_RTT.halved()
PAIRS_NETEM_JITTER = utils.config.PairsMap({}, default=20)
PAIRS_NETEM_RATE = utils.config.PairsMap({}, default=0)


def iperf_test(remotes, domains, na, nb):
    kill_cmd = ["sudo", "pkill", "-9", "iperf3"]
    utils.proc.run_process_over_ssh(remotes[na], kill_cmd).wait()
    utils.proc.run_process_over_ssh(remotes[nb], kill_cmd).wait()

    iperf_s_cmd = ["iperf3", "-s", "-p", f"{IPERF_PORT}", "-1"]
    proc_s = utils.proc.run_process_over_ssh(remotes[nb], iperf_s_cmd)
    time.sleep(3)

    iperf_c_cmd = [
        "iperf3",
        "-c",
        domains[nb],
        "-p",
        f"{IPERF_PORT}",
        "-t",
        f"{IPERF_SECS}",
        "-N",
        "-4",
        "-O",
        f"{1}",
    ]
    proc_c = utils.proc.run_process_over_ssh(
        remotes[na], iperf_c_cmd, capture_stdout=True, capture_stderr=True
    )
    out, err = proc_c.communicate()

    utils.proc.run_process_over_ssh(remotes[nb], kill_cmd).wait()
    proc_s.wait()

    print(f"\nResult of iperf {na} -> {nb}:")
    print(out.decode())
    if err is not None and len(err) > 0:
        print(err.decode())


def ping_test(remotes, domains, na, nb):
    ping_cmd = ["ping", domains[nb], "-w", f"{PING_SECS}"]
    proc_p = utils.proc.run_process_over_ssh(
        remotes[na], ping_cmd, capture_stdout=True, capture_stderr=True
    )
    out, err = proc_p.communicate()

    print(f"\nResult of ping {na} -> {nb}:")
    print(out.decode())
    if err is not None and len(err) > 0:
        print(err.decode())


def main():
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    parser.add_argument(
        "-m",
        "--netem_asym",
        action="store_true",
        help="demonstrate netem asym setting",
    )
    args = parser.parse_args()

    _, _, hosts, remotes, domains, ipaddrs = utils.config.parse_toml_file(
        args.group
    )

    if args.netem_asym:
        print("Setting tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(
            remotes=remotes, capture_stderr=True
        )
        utils.net.set_tc_qdisc_netems_asym(
            PAIRS_NETEM_MEAN,
            PAIRS_NETEM_JITTER,
            PAIRS_NETEM_RATE,
            remotes=remotes,
            ipaddrs=ipaddrs,
        )
        print()

    for ia in range(len(hosts)):
        for ib in range(ia + 1, len(hosts)):
            na, nb = hosts[ia], hosts[ib]
            iperf_test(remotes, domains, na, nb)
            ping_test(remotes, domains, na, nb)

    if args.netem_asym:
        print("Clearing tc netem qdiscs...")
        utils.net.clear_tc_qdisc_netems_main(remotes=remotes)
