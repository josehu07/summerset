#! /bin/bash


echo
echo "Deleting existing namespaces & veths..."
sudo ip -all netns delete
sudo ip link delete brgm
for v in $(ip link show | grep veth | cut -d' ' -f 2 | rev | cut -c2- | rev | cut -d '@' -f 1)      
do
    sudo ip link delete $v
done


echo
echo "Reloading the ifb kernel module..."
sudo rmmod ifb
sudo modprobe ifb


echo
echo "Creating ifb for the main interface..."
MAIN_ETH=$(ip -o -4 route show to default | awk '{print $5}')
sudo ip link add ifbe type ifb
sudo ip link set ifbe up
sudo tc qdisc replace dev ifbe root noqueue
sudo tc qdisc add dev $MAIN_ETH ingress
sudo tc filter add dev $MAIN_ETH parent ffff: protocol all u32 match u32 0 0 flowid 1:1 action mirred egress redirect dev ifbe


echo
echo "Listing devices in default namespace:"
sudo ip link show


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
