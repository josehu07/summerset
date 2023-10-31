#! /bin/bash

# Usage: sudo scripts/setup_tcp_bufs.sh


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
echo
