#! /bin/bash

# Usage: sudo scripts/install_devtools.sh


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Installing Rust toolchain..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

echo
echo "Running sudo apt update & upgrade..."
sudo apt update
sudo apt --fix-broken install
sudo apt -y upgrade

echo
echo "Installing necessary apt packages..."
sudo apt install htop
sudo apt install python3-pip

echo
echo "Installing necessary pip packages..."
pip3 install matplotlib
echo
