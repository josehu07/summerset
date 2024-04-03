#! /bin/bash


MAX_SERVERS=9
MAX_CLIENTS=21


echo
echo "Deleting old conflicting rules..."
sudo iptables -D INPUT -p tcp --match multiport --dports "52700:$((52700+${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "52800:$((52800+${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "50000:$((50000+100*${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "52600:52601" -j ACCEPT
sudo iptables -D INPUT -p tcp --match multiport --dports "40000:$((40000+100*${MAX_CLIENTS}-1))" -j ACCEPT
sudo iptables -D INPUT -p tcp --dport "37777" -j ACCEPT


echo
echo "Allowing required server-side TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "52700:$((52700+${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "52800:$((52800+${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "50000:$((50000+100*${MAX_SERVERS}-1))" -j ACCEPT
sudo iptables -A INPUT -p tcp --match multiport --dports "52600:52601" -j ACCEPT


echo
echo "Allowing required client-side TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "40000:$((40000+100*${MAX_CLIENTS}-1))" -j ACCEPT


echo
echo "Allowing required iperf port..."
sudo iptables -A INPUT -p tcp --dport "37777" -j ACCEPT


echo
echo "Listing current iptables rules:"
sudo iptables -L


echo
echo "Making iptables rules persistent:"
sudo iptables-save | sudo tee /etc/iptables/rules.v4
