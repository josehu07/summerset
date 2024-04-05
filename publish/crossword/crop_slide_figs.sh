#! /bin/bash

# Requires:
#   sudo apt install python3-pip python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins
#   Add user ~/.local/bin to PATH

# Usage:
#    1. save slides exported PDF as results/slide-figures.pdf
#    2. run from repo root: ./publish/<paper>/crop_slide_figs.sh


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


ORIGINAL_PDF=results/slide-figures.pdf
TAKE_PAGES=9
TARGET_NAMES=("status_diagram"
              "log_in_action"
              "rs_codeword_space"
              "policy-multipaxos"
              "policy-rspaxos"
              "policy-balanced_rr_2"
              "policy-balanced_rr_3"
              "policy-unbalanced"
              "concurrent_failures")


echo
echo "Deleting old results..."
rm results/slides/*.pdf
rm "results/slide-figures.pdf:Zone.Identifier"


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
echo "Renaming cropped slide pages..."
for IDX in ${!TARGET_NAMES[@]};
do
    OLD_NAME="slide-$((IDX+1)).pdf"
    NEW_NAME="${TARGET_NAMES[$IDX]}.pdf"
    echo "    renaming $OLD_NAME to $NEW_NAME"
    mv "results/slides/$OLD_NAME" "results/slides/$NEW_NAME"
done
echo


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
