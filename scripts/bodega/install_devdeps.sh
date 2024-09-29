#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Installing extra apt packages..."
sudo apt -y install default-jre \
                    liblog4j2-java \
                    libresolv-wrapper \
                    protobuf-compiler


echo
echo "Installing golang 1.23..."
cd ..
wget https://go.dev/dl/go1.23.1.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.23.1.linux-amd64.tar.gz
rm go1.23.1.linux-amd64.tar.gz
tee -a $HOME/.bashrc <<EOF

# golang
export PATH=\$PATH:/usr/local/go/bin
EOF
tee -a $HOME/.profile <<EOF

# golang
export PATH=\$PATH:/usr/local/go/bin
EOF
source $HOME/.profile


echo
echo "Fetching YCSB benchmark..."
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
rm ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 ycsb


echo
echo "Fetching our Apache ZooKeeper fork..."
git clone https://github.com/josehu07/zookeeper.git
cd zookeeper
git checkout bodega
cd ..


echo
echo "Fetching & building our etcd fork..."
git clone https://github.com/josehu07/etcd.git
cd etcd
git checkout bodega
make build
