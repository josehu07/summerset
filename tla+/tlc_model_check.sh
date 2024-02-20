#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


if [ $# -ne 2 ];
then
    echo "ERROR: wrong number of arguments"
    echo "Example: ./tla+/tlc_model_check.sh multipaxos_smr_style MultiPaxos"
    exit 1
fi


SPEC_DIR=$1
SPEC_NAME=$2


cd "tla+/${SPEC_DIR}/"

java -XX:+UseParallelGC \
     -jar ../../../tla2tools.jar \
     -cleanup \
     -noGenerateSpecTE \
     -difftrace \
     -fpmem 0.95 \
     -workers auto \
     -metadir "/tmp/${SPEC_DIR}/states" \
     -config "${SPEC_NAME}_MC.cfg" "${SPEC_NAME}_MC.tla"
