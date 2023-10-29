#! /bin/bash

# Usage: sudo scripts/setup_net_devs.sh

MAX_SERVERS=9
MAX_CLIENTS=21

MTU=65535
QLEN=500000000


echo
echo "Deleting existing namespaces & veths..."
sudo ip -all netns delete
sudo ip link delete brgm
for v in $(ip link show | grep veth | cut -d' ' -f 2 | rev | cut -c2- | rev | cut -d '@' -f 1)      
do
    sudo ip link delete $v
done


echo
echo "Adding namespaces for servers..."
for (( s = 0; s < $MAX_SERVERS; s++ ))
do
    sudo ip netns add ns$s
    sudo ip netns set ns$s $s
done


echo
echo "Creating bridge device for manager..."
sudo ip link add brgm type bridge
sudo ip link set brgm mtu $MTU
sudo ip link set brgm txqlen $QLEN
sudo ip addr add "10.0.0.0/16" dev brgm
sudo ip link set brgm up


echo
echo "Creating & assigning veths for servers..."
for (( s = 0; s < $MAX_SERVERS; s++ ))
do
    sudo ip link add veths$s type veth peer name veths${s}m
    sudo ip link set veths$s mtu $MTU
    sudo ip link set veths$s txqlen $QLEN
    sudo ip link set veths${s}m mtu $MTU
    sudo ip link set veths${s}m txqlen $QLEN
    sudo ip link set veths${s}m up
    sudo ip link set veths${s}m master brgm
    sudo ip link set veths$s netns ns$s
    sudo ip netns exec ns$s ip addr add "10.0.1.$s/16" dev veths$s
    sudo ip netns exec ns$s ip link set veths$s up
done


echo
echo "Creating & assigning veths for clients..."
for (( c = 0; c < $MAX_CLIENTS; c++ ))
do
    sudo ip link add vethc$c type veth peer name vethc${c}m
    sudo ip link set vethc$c mtu $MTU
    sudo ip link set vethc$c txqlen $QLEN
    sudo ip link set vethc${c}m mtu $MTU
    sudo ip link set vethc${c}m txqlen $QLEN
    sudo ip link set vethc${c}m up
    sudo ip link set vethc${c}m master brgm
    sudo ip addr add "10.0.2.$c/16" dev vethc$c
    sudo ip link set vethc$c up
done


echo
echo "Listing devices in default namespace:"
sudo ip link show


echo
echo "Listing all named namespaces:"
sudo ip netns list


for (( s = 0; s < $MAX_SERVERS; s++ ))
do
    echo
    echo "Listing devices in namespace ns$s:"
    sudo ip netns exec ns$s ip link show
done
