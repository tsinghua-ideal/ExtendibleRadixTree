#!/usr/bin/env bash
cmake .
make
./nvmkv 1000000

cd Figure
pip3 install -r requirements.txt
python plot_insert.py
python plot_insert.py