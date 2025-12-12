#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


SETUP_DIR=$(dirname $(realpath $0))
CONFIG_DIR=/etc/ssh/${USER}


echo
echo "Creating global config dir ${CONFIG_DIR}..."
sudo mkdir ${CONFIG_DIR}
sudo chown -R ${USER} ${CONFIG_DIR}
chmod 700 ${CONFIG_DIR}


echo
echo "Saving private key globally & setting config..."
cp ${SETUP_DIR}/sshkey_id_rsa ${CONFIG_DIR}/cluster_id_rsa
chmod 600 ${CONFIG_DIR}/cluster_id_rsa
echo "    IdentityFile ${CONFIG_DIR}/cluster_id_rsa" | sudo tee -a /etc/ssh/ssh_config
echo "    IdentityFile ~/.ssh/id_rsa" | sudo tee -a /etc/ssh/ssh_config
echo "    StrictHostKeyChecking no" | sudo tee -a /etc/ssh/ssh_config


echo
echo "Saving public key globally..."
cp ${SETUP_DIR}/sshkey_id_rsa.pub ${CONFIG_DIR}/cluster_id_rsa.pub
chmod 644 ${CONFIG_DIR}/cluster_id_rsa.pub


echo
echo "Adding public key to global authorized_keys..."
cp ${SETUP_DIR}/sshkey_id_rsa.pub ${CONFIG_DIR}/authorized_keys
chmod 600 ${CONFIG_DIR}/authorized_keys
echo "AuthorizedKeysFile .ssh/authorized_keys ${CONFIG_DIR}/authorized_keys" | sudo tee -a /etc/ssh/sshd_config


echo
echo "Increasing MaxStartups to accommodate clients..."
sudo sh -c 'sudo sed -i.bak -E "s/^[#]*MaxStartups.*/MaxStartups 500/" /etc/ssh/sshd_config'


echo
echo "Restarting the sshd daemon service..."
sudo systemctl restart sshd
