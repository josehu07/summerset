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
sudo apt -y install wget \
                    curl \
                    htop \
                    tree \
                    cloc \
                    iperf3 \
                    iptables-persistent \
                    screen \
                    build-essential \
                    cmake \
                    autoconf \
                    flex \
                    bison \
                    patch \
                    libssl-dev \
                    zlib1g-dev \
                    libbz2-dev \
                    libreadline-dev \
                    libsqlite3-dev \
                    libncurses-dev \
                    libncursesw5-dev \
                    xz-utils \
                    tk-dev \
                    libxml2-dev \
                    libxmlsec1-dev \
                    libffi-dev \
                    liblzma-dev


echo
echo "Installing Rust toolchain..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
sudo tee -a $HOME/.bashrc <<EOF

# cargo
export RUSTUP_HOME=\$HOME/.rustup
export CARGO_HOME=\$HOME/.cargo
export PATH=\$HOME/.cargo/bin:\$PATH
EOF
sudo tee -a $HOME/.profile <<EOF

# cargo
export RUSTUP_HOME=\$HOME/.rustup
export CARGO_HOME=\$HOME/.cargo
export PATH=\$HOME/.cargo/bin:\$PATH
EOF
source $HOME/.profile


echo
echo "Installing pyenv..."
curl https://pyenv.run | bash
tee -a $HOME/.bashrc <<EOF

# pyenv
export PYENV_ROOT="\$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="\$PYENV_ROOT/bin:\$PATH"
eval "\$(pyenv init -)"
EOF
tee -a $HOME/.profile <<EOF

# pyenv
export PYENV_ROOT="\$HOME/.pyenv"
command -v pyenv >/dev/null || export PATH="\$PYENV_ROOT/bin:\$PATH"
eval "\$(pyenv init -)"
EOF
source $HOME/.profile


echo
echo "Installing Python 3.12..."
pyenv install 3.12
pyenv global 3.12
source $HOME/.profile


echo
echo "Installing necessary pip packages..."
pip3 install numpy \
             matplotlib \
             toml
