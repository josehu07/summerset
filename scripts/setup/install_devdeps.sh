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
sudo apt -y install htop \
                    tree \
                    cloc \
                    python3-pip \
                    iperf3 \
                    iptables-persistent \
                    screen


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
sudo -H pip3 install numpy \
                     matplotlib \
                     toml


echo
echo "Adding login commands to /etc/profile..."
sudo tee -a /etc/profile <<EOF

# for global installation of rust
export RUSTUP_HOME=/usr/local/rustup
export CARGO_HOME=/usr/local/cargo
export PATH=/usr/local/cargo/bin:\$PATH

# cd into /eval/summerset on login
cd /eval/summerset
EOF
