#!/bin/bash

# DWH Coding Challenge - Build and Run Script
echo "Building Docker image for DWH Coding Challenge..."

# Navigate to parent directory for proper build context
cd ..

# Build the Docker image using solution/Dockerfile
docker build -f solution/Dockerfile -t dwh-challenge .

if [ $? -eq 0 ]; then
    echo "Docker image built successfully!"
    echo "Running the container..."
    
    # Run the container
    docker run --rm dwh-challenge
else
    echo "Failed to build Docker image!"
    exit 1
fi
