#! /bin/bash

# Usage:
#    To archive: ./publish/<paper>/archive_results.sh <conf_name>
#    To extract: ./publish/<paper>/archive_results.sh <conf_name> ex


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi

if [ $# -le 0 ];
then
    echo "ERROR: please give conference name!"
    exit 1
fi


TAR_NAME="$1.tar.xz"

DROPBOX_DIR="~/Dropbox/UW-Madison/ADSL-Systems-Lab/Data-Backups/Crossword"


if [ $# -ge 2 ] && [ "$2" = "ex" ];
then
    # extracting...
    echo
    echo "Downloading archive from Dropbox..."
    cp ${DROPBOX_DIR}/${TAR_NAME} backups/

    echo
    echo "Extracting results/ <- backups/${TAR_NAME}..."
    tar -xf backups/${TAR_NAME} -C results/

else
    # archiving...
    echo
    echo "Archiving results/ -> backups/${TAR_NAME}..."
    cd results/
    tar -Jcf ${TAR_NAME} *
    cd ..
    mv results/${TAR_NAME} backups/

    echo
    echo "Replicating archive to Dropbox..."
    cp backups/${TAR_NAME} ${DROPBOX_DIR}/
fi
