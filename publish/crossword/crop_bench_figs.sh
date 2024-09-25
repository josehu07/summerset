#! /bin/bash

# Requires:
#   sudo apt install python3-pip python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins
#   Add user ~/.local/bin to PATH

# Usage:
#    1. save final bench plots to results/...
#    2. run from repo root: ./publish/<paper>/crop_bench_figs.sh


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


PLOT_FILES=("intros/motiv_profile_cdf"
            "models/cstr_bounds"
            "plots/breakdown/exper-breakdown"
            "plots/breakdown/legend-breakdown"
            "plots/adaptive/exper-adaptive"
            "plots/failover/exper-failover"
            "plots/unbalanced/exper-unbalanced"
            "plots/unbalanced/legend-unbalanced"
            "plots/critical/exper-critical-5.small.50.1dc"
            "plots/critical/exper-critical-5.small.50.wan"
            "plots/critical/exper-critical-5.large.50.1dc"
            "plots/critical/exper-critical-5.large.50.wan"
            "plots/critical/exper-critical-5.mixed.50.1dc"
            "plots/critical/exper-critical-5.mixed.50.wan"
            "plots/critical/exper-critical-cluster_size"
            "plots/critical/exper-critical-write_ratio"
            "plots/critical/legend-critical"
            "plots/critical/legend-critical-minor"
            "plots/load_sizes/exper-load_sizes"
            "plots/load_sizes/legend-load_sizes"
            "plots/staleness/exper-staleness"
            "plots/bw_utils/exper-bw_utils"
            "plots/ycsb_trace/exper-ycsb_trace"
            "plots/cockroach/exper-cockroach"
            "plots/cockroach/legend-cockroach")
PLOT_FILES_BOTTOM_MORE=("intros/legend-motiv_profile"
                        "plots/critical/ylabels-critical"
                        "plots/failover/legend-failover"
                        "plots/adaptive/legend-adaptive"
                        "plots/staleness/legend-staleness"
                        "plots/bw_utils/legend-bw_utils"
                        "plots/ycsb_trace/legend-ycsb_trace")


echo
echo "Cropping bench plots..."
for FILE_NAME in ${PLOT_FILES[@]};
do
    echo "    cropping results/${FILE_NAME}.pdf"
    pdfcropmargins -p 5 -t 255 -mo -o results "results/${FILE_NAME}.pdf"
done


echo
echo "Cropping plots with more space at the bottom..."
for FILE_NAME in ${PLOT_FILES_BOTTOM_MORE[@]};
do
    echo "    cropping results/${FILE_NAME}.pdf"
    pdfcropmargins -p4 5 50 5 5 -t 255 -mo -o results "results/${FILE_NAME}.pdf"
done


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
