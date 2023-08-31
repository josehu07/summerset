#! /usr/bin/bash

echo "Per-socket TCP send/receive buffer:"
echo "min default max"
echo "4096 131072 33554432" | sudo tee /proc/sys/net/ipv4/tcp_rmem
echo "4096 131072 33554432" | sudo tee /proc/sys/net/ipv4/tcp_wmem
echo

echo "System-wide total buffer size:"
echo "min default max"
echo "1538757 16413408 24620112" | sudo tee /proc/sys/net/ipv4/tcp_mem
echo

echo "Max value of setsockopt:"
echo "33554432" | sudo tee /proc/sys/net/core/rmem_max
echo "33554432" | sudo tee /proc/sys/net/core/wmem_max
echo

echo "Default value of network socket:"
echo "131072" | sudo tee /proc/sys/net/core/rmem_default
echo "131072" | sudo tee /proc/sys/net/core/wmem_default
echo
