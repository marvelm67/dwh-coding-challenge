#!/bin/bash

# DWH Coding Challenge - Local Run Script (without Docker)
echo "Running DWH Coding Challenge Solution locally..."

# Check if virtual environment exists
if [ ! -d "../.venv" ]; then
    echo "Creating Python virtual environment..."
    cd ..
    python3 -m venv .venv
    cd solution
fi

# Activate virtual environment and install requirements
echo "Installing dependencies..."
source ../.venv/bin/activate
pip install -r requirements.txt

# Run the main script
echo "Executing solution..."
python main.py

echo "Local execution completed!"
