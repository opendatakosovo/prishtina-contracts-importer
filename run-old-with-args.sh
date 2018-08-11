#!/usr/bin/env bash
source ./venv/bin/activate
file=$1
python procurement_parser_old_with_arguments.py $file
