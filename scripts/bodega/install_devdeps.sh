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
echo "Fetching Apache ZooKeeper..."
curl -O --location https://dlcdn.apache.org/zookeeper/zookeeper-3.9.2/apache-zookeeper-3.9.2-bin.tar.gz
tar xfvz apache-zookeeper-3.9.2-bin.tar.gz
rm apache-zookeeper-3.9.2-bin.tar.gz
mv apache-zookeeper-3.9.2-bin zookeeper
