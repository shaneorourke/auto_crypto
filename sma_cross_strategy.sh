#!/bin/bash

PATH=$(dirname "$0")

cd $PATH &&
source env/bin/activate &&
python3 sma_cross_strategy.py &&
deactivate
