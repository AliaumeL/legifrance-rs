#!/usr/bin/env bash
# ceseda.sh
#
# Aliaume Lopez
# GPL-3
#
# This script downloads the JADE dataset, extracts it, indexes the documents,
# searches for documents related to the CESEDA, and saves the results in a CSV file.
#
# Usage: ./ceseda.sh


# Download all of the JADE (jurisprudence administrative) dataset
# extract it, and index the resulting documents.
dilarxiv --tarballs --fond JADE --extract --index

# Search for all documents related to the CESEDA (Code de l'entrée et du séjour des étrangers et du droit d'asile)
# save the list of matching files in a text document.
dilarxiv --query "CESEDA OR \"code de l'entrée et du séjour des étrangers et du droit d'asile\"" \
         --save ceseda-files.txt

# Transform the text document containing the list of matching files
# into a CSV with file metadata (title, date, judge, etc.)
# this creates a `ceseda-files.txt.csv` file.
dilarxiv --csv ceseda-files.txt 
