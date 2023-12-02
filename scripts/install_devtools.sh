#! /bin/bash

# Usage: sudo scripts/install_devtools.sh


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


echo
echo "Installing Rust toolchain..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"


echo
echo "Installing necessary pip packages..."
pip3 install numpy
pip3 install matplotlib
pip3 install toml
echo
