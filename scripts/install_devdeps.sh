#! /bin/bash

# Usage: ./scripts/install_devdeps.sh


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
sudo apt -y install default-jre
sudo apt -y install liblog4j2-java


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
echo "Fetching YCSB benchmark..."
cd ..
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
rm ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 ycsb


echo
echo "Fetching ChainPaxos codebase..."
git clone https://github.com/pfouto/chain.git
cd chain
git checkout aa4878d
cd ..
git clone https://github.com/pfouto/chain-client.git
cd chain-client
git checkout ce3a038
echo
