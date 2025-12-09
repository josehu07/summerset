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


PLOT_FILES=("plots/loc_grid_geo/exper-loc_grid_geo-w0"
            "plots/loc_grid_geo/exper-loc_grid_geo-w1"
            "plots/loc_grid_geo/exper-loc_grid_geo-w10"
            "plots/loc_grid_wan/exper-loc_grid_wan-w0"
            "plots/loc_grid_wan/exper-loc_grid_wan-w1"
            "plots/loc_grid_wan/exper-loc_grid_wan-w10"
            "plots/latency_cdfs/exper-latency_cdfs-w1-wr"
            "plots/latency_cdfs/exper-latency_cdfs-w0-rd"
            "plots/latency_cdfs/exper-latency_cdfs-w1-rd"
            "plots/latency_cdfs/exper-latency_cdfs-w10-rd"
            "plots/latency_cdfs/legend-latency_cdfs"
            "plots/access_cnts/intro-access_cnts"
            "plots/ycsb_zk_etcd/exper-ycsb_zk_etcd-uniform"
            "plots/ycsb_zk_etcd/exper-ycsb_zk_etcd-zipfian"
            "plots/ycsb_zk_etcd/legend-ycsb_zk_etcd"
            "plots/rlats_on_write/exper-rlats_on_write"
            "plots/wlats_on_conf/exper-wlats_on_conf"
            "plots/conf_coverage/exper-conf_coverage-site"
            "plots/conf_coverage/exper-conf_coverage-keys"
            "plots/writes_sizes/exper-writes_sizes-puts"
            "plots/writes_sizes/exper-writes_sizes-size"
            "plots/tput_lat_curve/exper-tput_lat_curve"
            "ftsim/result-ftsim")
PLOT_FILES_BOTTOM_MORE=("plots/loc_grid_geo/ylabels-loc_grid_geo"
                        "plots/loc_grid_wan/ylabels-loc_grid_wan"
                        "plots/latency_cdfs/ylabel-latency_cdfs"
                        "plots/ycsb_zk_etcd/ylabels-ycsb_zk_etcd")
PLOT_FILES_LEFT_MORE=("plots/loc_grid_geo/legend-loc_grid_geo"
                      "plots/loc_grid_wan/legend-loc_grid_wan")


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
    pdfcropmargins -p4 5 100 5 5 -t 255 -mo -o results "results/${FILE_NAME}.pdf"
done

echo
echo "Cropping plots with more space at the left..."
for FILE_NAME in ${PLOT_FILES_LEFT_MORE[@]};
do
    echo "    cropping results/${FILE_NAME}.pdf"
    pdfcropmargins -p4 50 5 5 10 -t 255 -mo -o results "results/${FILE_NAME}.pdf"
done


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
