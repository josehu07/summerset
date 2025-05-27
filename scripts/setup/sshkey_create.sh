#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


SETUP_DIR=$(dirname $(realpath $0))


echo
echo "Generating new key pair under ${SETUP_DIR}/..."
ssh-keygen -t rsa -f ${SETUP_DIR}/sshkey_id_rsa -C "summerset@cloudlab" -N ""
