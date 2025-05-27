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


BK_TAR_NAME="crossword.$1.tar.xz"

DROPBOX_DIR="${HOME}/Dropbox/UW-Madison/Research/Data-Backups/Crossword"
DB_TAR_NAME="$1.tar.xz"


if [ $# -ge 2 ] && [ "$2" = "ex" ];
then
    # extracting...
    echo
    echo "Downloading archive from Dropbox..."
    cp ${DROPBOX_DIR}/${DB_TAR_NAME} backups/${BK_TAR_NAME}

    echo
    echo "Extracting results/ <- backups/${BK_TAR_NAME}..."
    tar -xf backups/${BK_TAR_NAME} -C results/

else
    # archiving...
    echo
    echo "Archiving results/ -> backups/${BK_TAR_NAME}..."
    cd results/
    COPYFILE_DISABLE=1 tar -Jcf ${BK_TAR_NAME} *
    cd ..
    mv results/${BK_TAR_NAME} backups/

    echo
    echo "Replicating archive to Dropbox..."
    cp backups/${BK_TAR_NAME} ${DROPBOX_DIR}/${DB_TAR_NAME}
fi
