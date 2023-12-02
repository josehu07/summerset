import sys
import os
import time

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import common_utils as utils


TOML_FILENAME = "scripts/remote_hosts.toml"


IPERF_PORT = 7777
IPERF_SECS = 6

PING_SECS = 5


def iperf_test(remotes, domains, na, nb):
    kill_cmd = ["sudo", "pkill", "-9", "iperf3"]
    utils.run_process_over_ssh(remotes[na], kill_cmd).wait()
    utils.run_process_over_ssh(remotes[nb], kill_cmd).wait()

    iperf_s_cmd = ["iperf3", "-s", "-p", f"{IPERF_PORT}", "-1"]
    proc_s = utils.run_process_over_ssh(remotes[nb], iperf_s_cmd)
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
    proc_c = utils.run_process_over_ssh(remotes[na], iperf_c_cmd)
    out, err = proc_c.communicate()

    utils.run_process_over_ssh(remotes[nb], kill_cmd).wait()
    proc_s.wait()

    print(f"\nResult of iperf {na} -> {nb}:")
    print(out.decode())
    if err is not None and len(err) > 0:
        print(err.decode())


def ping_test(remotes, domains, na, nb):
    ping_cmd = ["ping", domains[nb], "-w", f"{PING_SECS}"]
    proc_p = utils.run_process_over_ssh(remotes[na], ping_cmd)
    out, err = proc_p.communicate()

    print(f"\nResult of ping {na} -> {nb}:")
    print(out.decode())
    if err is not None and len(err) > 0:
        print(err.decode())


if __name__ == "__main__":
    utils.check_proper_cwd()

    hosts_config = utils.read_toml_file(TOML_FILENAME)
    remotes = hosts_config["hosts"]
    domains = {
        name: utils.split_remote_string(remote)[1] for name, remote in remotes.items()
    }
    hosts = sorted(list(remotes.keys()))

    for ia in range(len(hosts)):
        for ib in range(ia + 1, len(hosts)):
            na, nb = hosts[ia], hosts[ib]
            iperf_test(remotes, domains, na, nb)
            ping_test(remotes, domains, na, nb)
