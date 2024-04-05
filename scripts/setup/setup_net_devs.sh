#! /bin/bash


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
echo "Loading ifb module & creating ifb devices..."
sudo rmmod ifb
sudo modprobe ifb  # by default, add ifb0 & ifb1 automatically
for (( s = 2; s < $MAX_SERVERS; s++ ))
do
    sudo ip link add ifb$s type ifb
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
echo "Redirecting veth ingress to ifb..."
for (( s = 0; s < $MAX_SERVERS; s++ ))
do
    sudo ip link set ifb$s mtu $MTU
    sudo ip link set ifb$s txqlen $QLEN
    sudo ip link set ifb$s netns ns$s
    sudo ip netns exec ns$s ip link set ifb$s up
    sudo ip netns exec ns$s tc qdisc replace dev ifb$s root noqueue
    sudo ip netns exec ns$s tc qdisc add dev veths$s ingress
    sudo ip netns exec ns$s tc filter add dev veths$s parent ffff: protocol all u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifb$s
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
echo "Creating ifb for the main interface as well..."
MAIN_ETH=$(ip -o -4 route show to default | awk '{print $5}')
sudo ip link add ifbe type ifb
sudo ip link set ifbe up
sudo tc qdisc replace dev ifbe root noqueue
sudo tc qdisc add dev $MAIN_ETH ingress
sudo tc filter add dev $MAIN_ETH parent ffff: protocol all u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifbe


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


if [ $(id -u) -ne 0 ];
then
    echo
    echo "Adding start-up changes to /etc/crontab..."
    sudo cp scripts/setup/setup_net_devs.sh /etc/
    sudo chmod +x /etc/setup_net_devs.sh
    sudo tee -a /etc/crontab <<EOF

# For summerset benchmarking...
@reboot root /etc/setup_net_devs.sh
EOF
    sudo systemctl enable cron.service
fi
