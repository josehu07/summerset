#! /bin/bash

# Usage: sudo scripts/setup_tcp_ports.sh


echo
echo "Allowing required server-side TCP ports:"
sudo ufw allow "52700:52702/tcp"
sudo ufw allow "52800:52802/tcp"
sudo ufw allow "50000:50299/tcp"
sudo ufw allow "52600:52601/tcp"


echo
echo "Allowing required client-side TCP ports:"
sudo ufw allow "40000:41599/tcp"
