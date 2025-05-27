#! /bin/bash

# Requires:
#   sudo apt install python3-pip python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins
#   Add user ~/.local/bin to PATH

# Usage:
#    1. save algo exported PDF as results/algo-block-fig.pdf
#    2. run from repo root: ./publish/<paper>/crop_algo_block.sh


if [ $(id -u) -eq 0 ];
then
    echo "Please run this script as normal user!"
    exit 1
fi


ORIGINAL_PDF=results/algo-block-fig.pdf
TAKE_PAGES=1
TARGET_NAMES=("algo_block_fig")


echo
echo "Deleting old results..."
rm results/algo/*.pdf
rm "results/algo-block-fig.pdf:Zone.Identifier"


echo
echo "Separating desired pages..."
pdfseparate -l $TAKE_PAGES $ORIGINAL_PDF "results/algo/slide-%d.pdf"


echo
echo "Cropping separated pages..."
for FILE in $(ls results/algo/ | grep .pdf);
do
    echo "    cropping $FILE"
    pdfcropmargins -p 0 -t 255 -mo -o results "results/algo/$FILE"
done


echo
echo "Renaming cropped slide pages..."
for IDX in ${!TARGET_NAMES[@]};
do
    OLD_NAME="slide-$((IDX+1)).pdf"
    NEW_NAME="${TARGET_NAMES[$IDX]}.pdf"
    echo "    renaming $OLD_NAME to $NEW_NAME"
    mv "results/algo/$OLD_NAME" "results/algo/$NEW_NAME"
done


echo
echo "Deleting uncropped files..."
rm results/*_uncropped.pdf
