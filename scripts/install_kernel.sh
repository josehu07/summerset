#! /bin/bash

# Usage: sudo scripts/install_kernel.sh


echo
echo "Fetching ubuntu-mainline-kernel.sh..."
wget https://raw.githubusercontent.com/pimlie/ubuntu-mainline-kernel.sh/master/ubuntu-mainline-kernel.sh
sudo install ubuntu-mainline-kernel.sh /usr/local/bin/
rm ubuntu-mainline-kernel.sh


echo
echo "Checking latest kernel version:"
sudo ubuntu-mainline-kernel.sh -c


echo
echo "Installing latest stable kernel..."
sudo ubuntu-mainline-kernel.sh -i "6.5.7"


echo
echo "Be sure to reboot after successful installation!"
echo
