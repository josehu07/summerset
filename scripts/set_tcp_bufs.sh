#! /bin/bash

echo "  Per-socket TCP send/receive buffer:"
echo "    min     default   max"
echo "    1048576 268435456 268435456" | sudo tee /proc/sys/net/ipv4/tcp_rmem
echo "    1048576 268435456 268435456" | sudo tee /proc/sys/net/ipv4/tcp_wmem

echo "  System-wide total buffer size:"
echo "    min      default  max"
echo "    19660800 26214400 31457280" | sudo tee /proc/sys/net/ipv4/tcp_mem

echo "  Max value of setsockopt:"
echo "    268435456" | sudo tee /proc/sys/net/core/rmem_max
echo "    268435456" | sudo tee /proc/sys/net/core/wmem_max

echo "  Default value of network socket:"
echo "    268435456" | sudo tee /proc/sys/net/core/rmem_default
echo "    268435456" | sudo tee /proc/sys/net/core/wmem_default
