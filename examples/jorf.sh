#!/usr/bin/env bash
# jorf.sh
#
# Aliaume Lopez
# GPL-3
#
# This script downloads the JORF dataset, extracts it, indexes the documents,
# searches for documents related to `aliaume lopez`, and saves the results in a CSV file.
#
# Usage: ./jorf-name.sh


# Download all of the JADE (jurisprudence administrative) dataset
# extract it, and index the resulting documents.
dilarxiv --tarballs --fond JORF --extract --index

# Search for all documents related to the CESEDA (Code de l'entrée et du séjour des étrangers et du droit d'asile)
# save the list of matching files in a text document.
dilarxiv --query "Aliaume Lopez" \
         --save jorf-files.txt

# Transform the text document containing the list of matching files
# into a CSV with file metadata
# this creates a `jorf-files.txt.csv` file.
dilarxiv --csv jorf-files.txt 
