## Node Setup Memos

For a shell command, `$` indicates running it on the local development machine, while `%` indicates running it on a CloudLab remote host.

1. On you local dev machine, change into the repo's path
    1. `$ python3 -m pip install toml`
    2. `$ cd path/to/summerset`
2. Generate a new internal SSH key pair for experiments
    1. `$ ./scripts/setup/sshkey_create.sh`
3. Create CloudLab machines and fill in `scripts/remote_hosts.toml` properly
4. For each of the hosts (examples below are for group `reg` host `host0`), do the following setup work
    1. SSH to it by
        1. `$ python3 scripts/remote_ssh_to.py -g reg -t host0`
    2. Add new user named `smr`
        1. `% sudo adduser smr` (set password to `smr` as well)
        2. `% sudo usermod -aG sudo smr`
        3. `% sudo cp -r .ssh /home/smr/`
        4. `% sudo chown -R smr /home/smr/.ssh`
        5. `% echo "smr ALL=(ALL) NOPASSWD: ALL" | sudo tee /etc/sudoers.d/smr-pass` (for password-less `sudo`)
    3. Logout back to the local machine, sync the repo folder to the remote host
        1. `% logout`
        2. `$ python3 scripts/remote_mirror.py -g reg -t host0`
        3. `$ python3 scripts/remote_ssh_to.py -g reg -t host0`
    4. Upon SSH using our helper script, you will find the mirrored repo at `/home/smr/summerset` which is automatically cd-ed into
    5. Resize the root partition to make more space
        1. `% ./scripts/setup/resize_partition.sh`
    6. Update Linux kernel version to v6.1.64, the one used for evaluations presented in the paper
        1. `% ./scripts/setup/install_kernel.sh`
        2. `% sudo reboot` (then wait a while until instance ready again)
    7. After rebooting and logging in, double check the kernel version
        1. `$ python3 scripts/remote_ssh_to.py -g reg -t host0`
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
