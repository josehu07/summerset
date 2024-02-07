#! /usr/bin/bash

# Requires:
#   sudo apt install python3-pip python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins
#   Add user ~/.local/bin to PATH

# Usage:
#    1. save final bench plots to results/final/...
#    2. run from repo root: ./pubtools/<paper>/crop_bench_figs.sh


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


PLOT_FILES=("models/cstr_bounds"
             "rs_coding/rs_coding"
             "adaptive/exper-adaptive"
             "breakdown/exper-breakdown"
             "breakdown/legend-breakdown"
             "failover/exper-failover"
             "unbalanced/exper-unbalanced"
             "unbalanced/legend-unbalanced"
             "critical/exper-critical-5.small.50.dc"
             "critical/exper-critical-5.small.50.wan"
             "critical/exper-critical-5.large.50.dc"
             "critical/exper-critical-5.large.50.wan"
             "critical/exper-critical-5.mixed.50.dc"
             "critical/exper-critical-5.mixed.50.wan"
             "critical/exper-critical-cluster_size"
             "critical/exper-critical-write_ratio"
             "critical/legend-critical"
             "critical/legend-critical-minor"
             "ycsb_3sites/exper-ycsb_3sites"
             "ycsb_3sites/legend-ycsb_3sites")
PLOT_FILES_BOTTOM_MORE=("critical/ylabels-critical"
                        "motivation/legend-motiv_profile"
                        "failover/legend-failover"
                        "adaptive/legend-adaptive")
PLOT_FILES_RIGHT_MORE=("motivation/motiv_profile_cdf")


echo
echo "Cropping bench plots..."
for FILE_NAME in ${PLOT_FILES[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done


echo
echo "Cropping plots with more space at the bottom..."
for FILE_NAME in ${PLOT_FILES_BOTTOM_MORE[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p4 0 50 0 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done


echo
echo "Cropping plots with more space to the right..."
for FILE_NAME in ${PLOT_FILES_RIGHT_MORE[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p4 0 0 5 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
