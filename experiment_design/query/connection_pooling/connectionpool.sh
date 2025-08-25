#!/bin/bash

for i in {1..11}; do
    echo "Run #$i"
    python3 pool.py  
    echo "-----------------------------------"
done
