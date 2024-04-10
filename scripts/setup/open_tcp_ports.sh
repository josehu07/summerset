#! /bin/bash


PORT_RANGE_MIN=40000
PORT_RANGE_MAX=59999


echo
echo "Deleting old conflicting rules..."
sudo iptables -D INPUT -p tcp --match multiport --dports "${PORT_RANGE_MIN}:${PORT_RANGE_MAX}" -j ACCEPT
sudo iptables -D INPUT -p tcp --dport "37777" -j ACCEPT


echo
echo "Allowing required TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "${PORT_RANGE_MIN}:${PORT_RANGE_MAX}" -j ACCEPT


echo
echo "Allowing required iperf port..."
sudo iptables -A INPUT -p tcp --dport "37777" -j ACCEPT


echo
echo "Listing current iptables rules:"
sudo iptables -L


echo
echo "Making iptables rules persistent:"
sudo iptables-save | sudo tee /etc/iptables/rules.v4


if [ $(id -u) -ne 0 ];
then
    echo
    echo "Adding start-up changes to /etc/sysctl.conf..."
    sudo tee -a /etc/sysctl.conf <<EOF

# For safer port usage...
net.ipv4.ip_local_port_range = 60000 60999
EOF
    sudo sysctl --system
fi
