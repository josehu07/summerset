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
                    libresolv-wrapper


echo
echo "Fetching YCSB benchmark..."
cd ..
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
rm ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 ycsb


echo
echo "Fetching our Apache ZooKeeper fork..."
git clone https://github.com/josehu07/zookeeper.git
cd zookeeper
git checkout bodega
