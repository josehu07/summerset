#! /bin/bash


PORT_RANGE_MIN=20000
PORT_RANGE_MAX=49999


echo
echo "Deleting old conflicting rules..."
sudo iptables -D INPUT -p tcp --match multiport --dports "${PORT_RANGE_MIN}:${PORT_RANGE_MAX}" -j ACCEPT


echo
echo "Allowing may-use TCP ports..."
sudo iptables -A INPUT -p tcp --match multiport --dports "${PORT_RANGE_MIN}:${PORT_RANGE_MAX}" -j ACCEPT


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
net.ipv4.ip_local_port_range = $((${PORT_RANGE_MAX}+1)) 60999
EOF
    sudo sysctl --system
fi
