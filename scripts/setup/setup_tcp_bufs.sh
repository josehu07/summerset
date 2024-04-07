#! /bin/bash


echo
echo "Per-socket TCP send/receive buffer:"
echo "  min       default   max"
echo "  536870912 1073741824 1073741824" | sudo tee /proc/sys/net/ipv4/tcp_rmem
echo "  536870912 1073741824 1073741824" | sudo tee /proc/sys/net/ipv4/tcp_wmem


echo
echo "System-wide total buffer size:"
echo "  min      default   max"
echo "  78643200 104857600 125829120" | sudo tee /proc/sys/net/ipv4/tcp_mem


echo
echo "Max value of setsockopt:"
echo "  1073741824" | sudo tee /proc/sys/net/core/rmem_max
echo "  1073741824" | sudo tee /proc/sys/net/core/wmem_max


echo
echo "Default value of network socket:"
echo "  1073741824" | sudo tee /proc/sys/net/core/rmem_default
echo "  1073741824" | sudo tee /proc/sys/net/core/wmem_default


if [ $(id -u) -ne 0 ];
then
    echo
    echo "Adding start-up changes to /etc/sysctl.conf..."
    sudo tee -a /etc/sysctl.conf <<EOF

# For safer port usage...
net.ipv4.ip_local_port_range = 53000 60999

# For summerset benchmarking...
net.ipv4.tcp_rmem = 536870912 1073741824 1073741824
net.ipv4.tcp_wmem = 536870912 1073741824 1073741824
net.ipv4.tcp_mem = 78643200 104857600 125829120
net.core.rmem_max = 1073741824
net.core.wmem_max = 1073741824
net.core.rmem_default = 1073741824
net.core.wmem_default = 1073741824
EOF
fi
