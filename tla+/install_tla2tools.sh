#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Installing java apt packages..."
sudo apt -y install default-jre
sudo apt -y install liblog4j2-java


echo
echo "Fetching tla2tools.jar..."
cd ..
curl -O --location https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
