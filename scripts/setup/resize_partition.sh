#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


TARGET_DEV=/dev/sda


echo
echo "Using fdisk to resize the root partition..."
sed -e 's/\s*\([\+0-9a-zA-Z]*\).*/\1/' << EOF | sudo fdisk ${TARGET_DEV}
    d  # delete partition
    3  # choose partition 3
    n  # new partition
    3  # use partition number 3
       # default start sec
       # default end sec
    N  # not removing ext3 signature
    p  # print the resulting table
    w  # sync to disk
    q  # done, quit
EOF


echo
echo "Reloading the partition table..."
sudo partprobe ${TARGET_DEV}


echo
echo "Resizing the filesystem on-line..."
sudo resize2fs ${TARGET_DEV}
