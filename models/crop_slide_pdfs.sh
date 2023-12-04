#! /usr/bin/bash

# Requires:
#   sudo apt install python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins --user --upgrade

# Usage:
#    1. save slides exported PDF as results/slide-figures.pdf
#    2. run: ./models/crop_slides_pdfs.sh

ORIGINAL_PDF=results/slide-figures.pdf
TAKE_PAGES=9


echo
echo "Deleting old results..."
rm results/slides/*.pdf


echo
echo "Separating desired pages..."
pdfseparate -l $TAKE_PAGES $ORIGINAL_PDF "results/slides/slide-%d.pdf"


echo
echo "Cropping separated pages..."
for FILE in $(ls results/slides/ | grep .pdf);
do
    echo "    cropping $FILE"
    pdfcropmargins -p 0 -t 255 -mo -o results "results/slides/$FILE"
done


echo
echo "Cropping extra files..."
EXTRA_FILES=("models/cstr_bounds"
             "rs_coding/rs_coding"
             "adaptive/exper-adaptive"
             "breakdown/exper-breakdown" "breakdown/legend-breakdown"
             "failover/exper-failover"
             "unbalanced/exper-unbalanced" "unbalanced/legend-unbalanced"
             "critical/exper-critical-5.small.50.dc" "critical/exper-critical-5.small.50.wan"
             "critical/exper-critical-5.large.50.dc" "critical/exper-critical-5.large.50.wan"
             "critical/exper-critical-5.mixed.50.dc" "critical/exper-critical-5.mixed.50.wan"
             "critical/exper-critical-cluster_size" "critical/exper-critical-write_ratio"
             "critical/legend-critical" "critical/legend-critical-minor"
             "ycsb_3sites/exper-ycsb_3sites" "ycsb_3sites/legend-ycsb_3sites")
for FILE_NAME in ${EXTRA_FILES[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done
EXTRA_FILES_BOTTOM_MORE=("critical/ylabels-critical" "motivation/legend-motiv_profile"
                         "failover/legend-failover" "adaptive/legend-adaptive")
for FILE_NAME in ${EXTRA_FILES_BOTTOM_MORE[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p4 0 50 0 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done
EXTRA_FILES_RIGHT_MORE=("motivation/motiv_profile_cdf")
for FILE_NAME in ${EXTRA_FILES_RIGHT_MORE[@]};
do
    echo "    cropping results/final/${FILE_NAME}.pdf"
    pdfcropmargins -p4 0 0 5 0 -t 255 -mo -o results "results/final/${FILE_NAME}.pdf"
done


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
rm "results/slide-figures.pdf:Zone.Identifier"


echo
echo "Renaming cropped slide pages..."
TARGET_NAMES=("status_diagram" "log_in_action" "rs_codeword_space" "policy-multipaxos"
              "policy-rspaxos" "policy-balanced_rr_2" "policy-balanced_rr_3"
              "policy-unbalanced" "concurrent_failures")
for IDX in ${!TARGET_NAMES[@]};
do
    OLD_NAME="slide-$((IDX+1)).pdf"
    NEW_NAME="${TARGET_NAMES[$IDX]}.pdf"
    echo "    renaming $OLD_NAME to $NEW_NAME"
    mv "results/slides/$OLD_NAME" "results/slides/$NEW_NAME"
done
echo
