#! /bin/bash


TARGET_DEV=/dev/sda
PART_NUMBER=3
PART_SIZE=200G


echo
echo "Using fdisk to resize the root partition..."
sed -e 's/\s*\([\+0-9a-zA-Z]*\).*/\1/' << EOF | sudo fdisk ${TARGET_DEV}
    d               # delete partition
    ${PART_NUMBER}  # choose old partition
    n               # new partition
    ${PART_NUMBER}  # use partition number
                    # default start sec
    +${PART_SIZE}   # default end sec
    p               # print the resulting table
    w               # sync to disk
    q               # done, quit
EOF


echo
echo "Reloading the partition table..."
sudo partprobe ${TARGET_DEV}


echo
echo "Resizing the filesystem on-line..."
sudo resize2fs ${TARGET_DEV}${PART_NUMBER}
