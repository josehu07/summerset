#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


echo
echo "Fetching ubuntu-mainline-kernel.sh..."
wget https://raw.githubusercontent.com/pimlie/ubuntu-mainline-kernel.sh/master/ubuntu-mainline-kernel.sh
sudo install ubuntu-mainline-kernel.sh /usr/local/bin/
rm ubuntu-mainline-kernel.sh


echo
echo "Checking latest kernel version:"
sudo ubuntu-mainline-kernel.sh -c


echo
echo "Installing longterm kernel v6.1.64..."
sudo ubuntu-mainline-kernel.sh -i "6.1.64"


echo
echo "Be sure to reboot after successful installation!"
