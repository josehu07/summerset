#! /usr/bin/bash

# Requires:
#   sudo apt install python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins --user --upgrade

# Usage:
#    1. save slides exported PDF as results/slide-figures.pdf
#    2. run: ./models/crop_slides_pdfs.sh

ORIGINAL_PDF=results/slide-figures.pdf
TAKE_PAGES=8


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
EXTRA_FILES=("cstr_bounds" "rs_coding"
             "backup/adaptive/exper-adaptive" "backup/adaptive/legend-adaptive"
             "backup/bd_n_space/exper-bd_n_space" "backup/bd_n_space/legend-bd_n_space"
             "backup/failover/exper-failover" "backup/failover/legend-failover"
             "backup/unbalanced/exper-unbalanced" "backup/unbalanced/legend-unbalanced")
for FILE_NAME in ${EXTRA_FILES[@]};
do
    echo "    cropping results/${FILE_NAME}.pdf"
    pdfcropmargins -p 0 -t 255 -mo -o results "results/${FILE_NAME}.pdf"
done

echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
rm "results/slide-figures.pdf:Zone.Identifier"

echo
echo "Renaming cropped files..."
TARGET_NAMES=("status_diagram" "log_in_action" "rs_codeword_space" "policy-multipaxos"
              "policy-rspaxos" "policy-balanced_rr" "policy-unbalanced" "concurrent_failures")
for IDX in ${!TARGET_NAMES[@]};
do
    OLD_NAME="slide-$((IDX+1)).pdf"
    NEW_NAME="${TARGET_NAMES[$IDX]}.pdf"
    echo "    renaming $OLD_NAME to $NEW_NAME"
    mv "results/slides/$OLD_NAME" "results/slides/$NEW_NAME"
done
echo
