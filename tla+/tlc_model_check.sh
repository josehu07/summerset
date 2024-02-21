#! /bin/bash


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


if [ $# -lt 2 ];
then
    echo "ERROR: too few arguments"
    echo "Example: ./tla+/tlc_model_check.sh multipaxos_smr_style MultiPaxos [small]"
    exit 1
fi


SPEC_DIR=$1
SPEC_NAME=$2
CFG_SUFFIX=""

if [ $# -ge 3 ];
then
    CFG_SUFFIX="_$3"
fi


cd "tla+/${SPEC_DIR}/"

java -XX:+UseParallelGC \
     -jar ../../../tla2tools.jar \
     -cleanup \
     -noGenerateSpecTE \
     -difftrace \
     -fpmem 0.95 \
     -workers auto \
     -metadir "/tmp/${SPEC_DIR}/states" \
     -config "${SPEC_NAME}_MC${CFG_SUFFIX}.cfg" \
     "${SPEC_NAME}_MC.tla"
