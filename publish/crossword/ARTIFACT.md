## Commands Memo for AE

For a shell command, `$` indicates running it on the local development machine, while `%` indicates running it on a CloudLab remote host.

1. On you local dev machine, change into the repo's path
    1. `$ cd path/to/summerset`
2. Create CloudLab machines and fill in `scripts/remote_hosts.toml`
3. Add the following setting to your `~/.ssh/config` for skipping the SSH `known_hosts` check

    ```text
    Host *
        StrictHostKeyChecking no
    ```

4. For each of the hosts (examples below are for `host0`), do the following setup work
    1. SSH to it
    2. Create `/eval` path and acquire its ownership:
        1. `% sudo mkdir /eval`
        2. `% sudo chown -R $USER /eval`
    3. Back to the local machine, sync the repo folder to the remote host
        1. `$ python3 scripts/remote_mirror.py -g 1dc`
    4. On `host0`, you will find the mirrored repo at `/eval/summerset`
    5. Resize the root partition to make more space
        1. `% cd /eval/summerset`
        2. `% ./scripts/setup/resize_partition.sh`
    6. Update Linux kernel version to v6.1.64, the one used for evaluations presented in the paper
        1. `% ./scripts/setup/install_kernel.sh`
        2. `% sudo reboot`
    7. After rebooting, double check the kernel version
        1. `% uname -a`
        2. `% cd /eval/summerset`
    8. Install necessary dependencies
        1. `% ./scripts/setup/install_devdeps.sh`
        2. `% ./scripts/crossword/install_devdeps.sh`
    9. Set up network devices (for netem experiments)
        1. `% ./scripts/setup/setup_net_devs.sh`
    10. Set up TCP buffer sizes
        1. `% ./scripts/setup/setup_tcp_bufs.sh`
    11. Configure & open TCP ports
        1. `% ./scripts/setup/open_tcp_ports.sh`
    12. Record the SSH key pair for mutual login between remote nodes
        1. `% ./scripts/setup/sshkey_record.sh`
