#!/usr/bin/env bash
# ceseda-oneshot.sh
#
# Aliaume Lopez
# GPL-3
#
# This script downloads the JADE dataset, extracts it, indexes the documents,
# searches for documents related to the CESEDA, and saves the results in a CSV file.
# This is the `oneshot` variant of `ceseda.sh`, which is intended to be run once
# and tries to minimise memory footprint and disk usage, at the expense of 
# speed and the ability to run multiple queries on the same dataset.
#
# Usage: ./ceseda-oneshot.sh

dilarxiv-oneshot --fond JADE \
                 --to-csv "ceseda-files.txt.csv" \
                 --query "CESEDA OR \"code de l'entrée et du séjour des étrangers et du droit d'asile\""
