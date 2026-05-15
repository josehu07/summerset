# Bodega Artifact (OSDI '26)

This document describes our artifact for Bodega (OSDI '26) and contains evaluation instructions.

## Contents

This artifact (GitHub [josehu07/summerset](https://github.com/josehu07/summerset/tree/bodega-artifact); branch `bodega-artifact`) contains:

* `src/`: source code of Summerset, a protocol-generic replicated kv-store we built for consensus research
    * `summerset_server/`, `_manager/`, and `_client/`: executable crates, entrance to running the kv-store
    * for more details about Summerset, you may refer to Chapter 5 of [this dissertation](https://research.cs.wisc.edu/wind/Publications/guanzhou-dissertation.pdf)
* `scripts/`: scripts for setting up and running distributed experiments on CloudLab
    * `scripts/bodega/bench_*.py`: main entry point scripts for experiments in our OSDI '26 manuscript
* `tla+/`: formal TLA+ specifications of protocols and model checking configurations

## Preparation

We use [CloudLab](https://www.cloudlab.us/), a common platform used by systems researchers, as our testbed. If you are new to CloudLab and need access, please reach out to the AEC.

> Since AE runs in parallel with paper draft revisions, we were making edits to the draft as we were preparing the artifact, and the Figure numbers below might be off by 1.

Our evaluation setup includes a local development machine from where you run the scripts, and 2 CloudLab clusters mapping to Figure 8.

### Local Machine

Clone the repository to your local machine:

```sh
git clone -b bodega-artifact --single-branch https://github.com/josehu07/summerset.git
cd summerset
```

Install required dependencies for driving experiments from your machine (commands are examples for Linux):

* Latest rust toolchain:
    ```sh
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
* Python `uv` toolchain:
    ```sh
    curl -LsSf https://astral.sh/uv/install.sh | sh
    uv sync
    ```

Test if you can invoke scripts:

```sh
uv run -m scripts.remote_ssh_to -h  # should print help message
```

### CloudLab Clusters

Log in to CloudLab console, and create two experiments, each instantiating a cluster. Please use the following profiles, respectively:
* [smr.dev.wan](https://www.cloudlab.us/p/AdvOSUWMadison/smr.dev.wan): use 5 nodes (default) and leave the semi-round-robin flag unticked; this will create a cluster of 5 nodes in 5 different CloudLab datacenters, mapping to Figure 8. WAN scenario
* [smr.dev.reg](https://www.cloudlab.us/p/AdvOSUWMadison/smr.dev.reg): use 5 nodes (default); this will create a cluster of 5 `c220g5` nodes used with emulated network interface latency, mapping to Figure 8. GEO scenario

> CloudLab resources are tight and you may fail to create the experiments due to insufficient availability. We have no control over CloudLab resources and reservations; we'd kindly ask reviewers to contact the AEC for coordinating availability if needed.
>
> If you cannot acquire the 5 `c220g5` nodes cluster, it is fine to skip, because it is used only in the emulated GEO setting of Figure 9 (a)(b)(c). All other experiments used the actual WAN cluster.

Once the experiments are ready, open `scripts/remote_hosts.toml` and replacement the placeholders with actual node urls (hostx mapping to nodex), like so:

```toml
# (DON'T CHANGE) remote base path and project repo folder name
base_path = "/home/smr"
repo_name = "summerset"

# (SET PROPERLY) for each group, its DNS domain names;
#                please leave username unchanged as smr
[reg]
host0 = "smr@c220g5-xxx.wisc.cloudlab.us"
host1 = "smr@c220g5-xxx.wisc.cloudlab.us"
host2 = "smr@c220g5-xxx.wisc.cloudlab.us"
host3 = "smr@c220g5-xxx.wisc.cloudlab.us"
host4 = "smr@c220g5-xxx.wisc.cloudlab.us"

[wan]
host0 = "smr@c220g5-xxx.wisc.cloudlab.us"
host1 = "smr@hpxxx.utah.cloudlab.us"
host2 = "smr@clnodexxx.clemson.cloudlab.us"
host3 = "smr@pcxxx.cloudlab.umass.edu"
host4 = "smr@aptxxx.apt.emulab.net"
```

You can find the nodes' domain names under the "List View" tab. Please ignore the ssh command with your username -- our image uses a custom username.

Sync the local repo to all remote hosts and build Summerset in release mode:

```sh
uv run -m scripts.remote_mirror -g reg -b -r
uv run -m scripts.remote_mirror -g wan -b -r
```

You can SSH into a desired host (and should be automatically cd'ed into the `~/summerset/` directory) via:

```sh
uv run -m scripts.remote_ssh_to -g reg -t host0  # replaces current shell
uv run -m scripts.remote_ssh_to -g wan -t host0  # replaces current shell
```

Double check that both clusters have been set up properly by running iperf **on host0** of each:

```sh
# on reg host0
uv run -m scripts.remote_iperf -g reg
# on wan host0
uv run -m scripts.remote_iperf -g wan
```

## Evaluation

Evaluation can be done via automated wrapper scripts. These scripts currently require the local machine to be able to SSH to CloudLab nodes via default SSH identity (i.e., custom `-i <xxx.id_rsa>` not yet supported).

### Killing Processes

If an experiment failed in the middle due to occasional flakiness of CloudLab network, you may kill process cleanly before retry:

```sh
# local
uv run -m scripts.remote_killall -g reg -t all [--etcd] [--zookeeper]
uv run -m scripts.remote_killall -g wan -t all [--etcd] [--zookeeper]
```

### Fetching Results & Plotting

Results of each run will be saved to the said host0's `results/output/<exper_name>/`. You may pull them to the same path in local repo via:

```sh
# local
uv run -m scripts.bodega.bench_<exper_name> -f host0
```

Then plot the results to `results/plots/<exper_name>/` via:

```sh
# local
uv run -m scripts.bodega.bench_<exper_name> -p
```

### Experiments

To reproduce the results in our manuscript, please run the following scripts **on the said host**, respectively.

Figure 9 (a)(b)(c) -- run on `reg` cluster `host0`:  

```sh
# on reg host0
uv run -m scripts.bodega.bench_loc_grid_geo
```

Figure 9 (d)(e)(f) -- run on `wan` cluster `host0`:  

```sh
# on wan host0
uv run -m scripts.bodega.bench_loc_grid_wan
```

Figure 10 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_tput_lat_curve
```

Figure 11 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_latency_cdfs
```

Figure 12 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_rlats_on_write
```

Figure 13 & 14 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_writes_sizes
```

Figure 15 is based on simulation, can be run locally:

```sh
# local (note no -m)
uv run models/bodega/failure_tput_sim.py
```

Figure 16 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_wlats_on_conf
```

Figure 17 & 18 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_conf_coverage
```

Figure 19 -- run on `wan` cluster `host0`:

```sh
# on wan host0
uv run -m scripts.bodega.bench_ycsb_zk_etcd
```

## Appendix: Node Setup

Below are the steps we took to set up the CloudLab node image, appended for reference. Our CloudLab profile's image has these steps already completed and can be used out of the box for artifact evaluation.

For a shell command, `$` indicates running it on the local development machine, while `%` indicates running it on a CloudLab remote host.

1. On you local dev machine, change into the repo's path
    1. `$ curl -LsSf https://astral.sh/uv/install.sh | sh`
    2. `$ uv sync`
    3. `$ cd path/to/summerset`
2. Generate a new internal SSH key pair for experiments
    1. `$ ./scripts/setup/sshkey_create.sh`
3. Create CloudLab machines and fill in `scripts/remote_hosts.toml` properly
4. For each of the hosts (examples below are for group `reg` host `host0`), do the following setup work
    1. SSH to it by
        1. `$ uv run -m scripts.remote_ssh_to -g reg -t host0`
    2. Add new user named `smr`
        1. `% sudo adduser smr` (set password to `smr` as well)
        2. `% sudo usermod -aG sudo smr`
        3. `% sudo cp -r .ssh /home/smr/`
        4. `% sudo chown -R smr /home/smr/.ssh`
        5. `% echo "smr ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/smr-pass` (for password-less `sudo`)
    3. Logout back to the local machine, sync the repo folder to the remote host
        1. `% logout`
        2. `$ uv run -m scripts.remote_mirror -g reg -t host0`
        3. `$ uv run -m scripts.remote_ssh_to -g reg -t host0`
    4. Upon SSH using our helper script, you will find the mirrored repo at `/home/smr/summerset` which is automatically cd-ed into
    5. Resize the root partition to make more space
        1. `% ./scripts/setup/resize_partition.sh`
    6. Update Linux kernel version to v6.1.64, the one used for evaluations presented in the paper
        1. `% ./scripts/setup/install_kernel.sh`
        2. `% sudo reboot` (then wait a while until instance ready again)
    7. After rebooting and logging in, double check the kernel version
        1. `$ uv run -m scripts.remote_ssh_to -g reg -t host0`
        2. `% uname -a`
    8. Install necessary dependencies
        1. `% ./scripts/setup/install_devdeps.sh`
        2. `% ./scripts/bodega/install_devdeps.sh`
    9. Set up network interfaces
        1. `% ./scripts/setup/setup_net_devs.sh`
    10. Set up TCP buffer sizes
        1. `% ./scripts/setup/setup_tcp_bufs.sh`
    11. Configure & open TCP ports
        1. `% ./scripts/setup/open_tcp_ports.sh`
    12. Record the SSH key pair for mutual login between remote nodes
        1. `% ./scripts/setup/sshkey_record.sh`

---

Author Contact: [Guanzhou Hu](https://josehu.com); `guanzhou.hu@wisc.edu` / `josehgz@amazon.com`
