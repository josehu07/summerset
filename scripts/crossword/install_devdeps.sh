#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Installing extra apt packages..."
sudo apt -y install default-jre \
                    liblog4j2-java


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


echo
echo "Fetching CockroachDB codebase..."
# TODO: add me
