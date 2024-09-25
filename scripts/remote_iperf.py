import sys
import os
import time
import argparse

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import utils


TOML_FILENAME = "scripts/remote_hosts.toml"


IPERF_PORT = 37777
IPERF_SECS = 6

PING_SECS = 5


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


if __name__ == "__main__":
    utils.file.check_proper_cwd()

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "-g", "--group", type=str, default="reg", help="hosts group to run on"
    )
    args = parser.parse_args()

    _, _, hosts, remotes, domains, _ = utils.config.parse_toml_file(
        TOML_FILENAME, args.group
    )

    for ia in range(len(hosts)):
        for ib in range(ia + 1, len(hosts)):
            na, nb = hosts[ia], hosts[ib]
            iperf_test(remotes, domains, na, nb)
            ping_test(remotes, domains, na, nb)
