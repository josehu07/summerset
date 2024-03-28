#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Running sudo apt update & upgrade..."
sudo apt -y update
sudo apt -y --fix-broken install
sudo apt -y upgrade


echo
echo "Installing necessary apt packages..."
sudo apt -y install htop
sudo apt -y install tree
sudo apt -y install cloc
sudo apt -y install python3-pip
sudo apt -y install iperf3
sudo apt -y install iptables-persistent
sudo apt -y install screen


echo
echo "Installing Rust toolchain (please ignore euid error)..."
export RUSTUP_HOME=/usr/local/rustup
export CARGO_HOME=/usr/local/cargo
export PATH=/usr/local/cargo/bin:$PATH
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y
sudo chown -R ${USER} /usr/local/rustup
sudo chown -R ${USER} /usr/local/cargo


echo
echo "Installing necessary pip packages..."
sudo -H pip3 install numpy
sudo -H pip3 install matplotlib
sudo -H pip3 install toml
