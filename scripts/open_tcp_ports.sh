#! /bin/bash

# Usage: sudo scripts/open_tcp_ports.sh


NUM_SERVERS=3
NUM_CLIENTS=16


echo
echo "Deleting old conflicting rules..."
sudo iptables -D INPUT -p tcp --match multiport --dports "52700:$((52700+${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "52800:$((52800+${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "50000:$((50000+100*${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "52600:52601" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "40000:$((40000+100*${NUM_CLIENTS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --dport "7777" -j ACCEPT



echo
echo "Allowing required server-side TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "52700:$((52700+${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "52800:$((52800+${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "50000:$((50000+100*${NUM_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "52600:52601" -j ACCEPT


echo
echo "Allowing required client-side TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "40000:$((40000+100*${NUM_CLIENTS}-1))" -j ACCEPT


echo
echo "Allowing required iperf port..."
sudo iptables -A INPUT -p tcp --dport "7777" -j ACCEPT


echo
echo "Listing current iptables rules:"
sudo iptables -L
echo
