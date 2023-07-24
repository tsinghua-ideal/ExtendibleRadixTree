#!/usr/bin/env bash
cmake .
make
./nvmkv 1000000 "/mnt/pm/test"

cd Figure
pip install -r requirements.txt
python plot_insert.py
python plot_insert.py