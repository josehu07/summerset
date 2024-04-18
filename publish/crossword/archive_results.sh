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


if [ $# -ge 2 ] && [ "$2" = "ex" ];
then
    # extracting...
    echo "Extracting results/ <- backups/${TAR_NAME}..."
    tar -xf backups/${TAR_NAME} -C results/

else
    # archiving...
    echo "Archiving results/ -> backups/${TAR_NAME}..."
    cd results/
    tar -Jcf ${TAR_NAME} *
    cd ..
    mv results/${TAR_NAME} backups/
fi
