#!/usr/bin/env bash
cmake .
make
./nvmkv 1000000

cd Figure
pip3 install -r requirements.txt
python3 plot_insert.py
python3 plot_query.py