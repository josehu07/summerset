#! /usr/bin/bash

# Requires:
#   sudo apt install python3-tk ghostscript poppler-utils
#   pip3 install pdfCropMargins --user --upgrade

# Usage:
#    1. save slides exported PDF as results/slide-figures.pdf
#    2. run: ./models/crop_slides_pdfs.sh results/slide-figures.pdf <take_pages>


if [ $# -ne 2 ];
then
    echo "Usage: ./models/crop_slides_pdfs.sh results/slide-figures.pdf <take_pages>"
    exit 1
fi

ORIGINAL_PDF=$1
TAKE_PAGES=$2


echo
echo "Separating desired pages..."
pdfseparate -l $TAKE_PAGES $ORIGINAL_PDF "results/slides/slide-%d.pdf"


echo
echo "Cropping pages individual PDFs..."
for FILE in $(ls results/slides/ | grep .pdf);
do
    echo "    cropping $FILE"
    pdfcropmargins -p 5 -mo -o results/slides "results/slides/$FILE"
done

echo
echo "Deleting old uncropped files..."
for FILE in $(ls results/slides/ | grep _uncropped.pdf);
do
    rm "results/slides/$FILE"
done
echo
