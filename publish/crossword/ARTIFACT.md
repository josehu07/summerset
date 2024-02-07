## Commands Memo for AE

For a shell command, `$` indicates running it on the local development machine, while `%` indicates running it on a CloudLab remote host.

1. On you local dev machine, change into the repo's path
    1. `cd path/to/summerset`
1. Create CloudLab machines and fill in `scripts/remote_hosts.toml`
2. For each of the hosts (examples below are for `host0`), do the following setup work
    1. SSH to it
        1. `$ python3 scripts/remote_ssh_to.py -t host0`
    2. Mount a proper storage device at `/mnt/eval/` (if no separate disk is available on the node, leaving the path under the default `/` mountpoint is fine, as long as enough storage space is usable in that partition)
        1. `% sudo lsblk`: locate an SSD device or partition
        2. `% sudo mkfs.ext4 /dev/DRIVE`
        3. `% sudo mkdir /mnt/eval`
        4. `% sudo mount /dev/DRIVE /mnt/eval`
        5. `% sudo chown -R $USER /mnt/eval`
        6. `% echo "/dev/DRIVE  /mnt/eval  ext4  defaults  0  0" | sudo tee -a /etc/fstab`
    3. Back to the local machine, sync the repo folder to the remote host
        1. `$ python3 scripts/remote_mirror.py -t host0`
    4. On `host0`, you will find the mirrored repo at `/mnt/eval/summerset`
    5. Update Linux kernel version to v.6.1.64, the one used for evaluations presented in the paper
        1. `% cd /mnt/eval/summerset`
        2. `% ./scripts/install_kernel.sh`
        3. `% sudo reboot`
    6. After rebooting, double check the kernel version
        1. `% uname -a`
        3. `% cd /mnt/eval/summerset`
    7. Install necessary dependencies
        1. `% ./scripts/install_devdeps.sh`
    8. Set up TCP buffer sizes
        1. `% ./scripts/setup_tcp_bufs.sh`
    9. (`host0` only) Set up network namespaces and virtual devices
        1. `% ./scripts/setup_net_devs.sh`
    10. (`host0` only) Generate an internal key-pair use by `host0` to ssh to other hosts
        1. `% ssh-keygen -t rsa`
        2. `% cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`
        3. Append the generated `id_rsa.pub` to other hosts `authorized_keys` as well
    12. Build Summerset in release mode:
        1. `% cargo build -r --workspace`
