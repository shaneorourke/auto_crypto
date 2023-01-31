#!/bin/bash

PATH=$(dirname "$0")

cd $PATH &&
source env/bin/activate &&
python3 log_reader.py &&
deactivate
