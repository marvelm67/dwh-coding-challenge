# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY solution/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the solution code
COPY solution/main.py .

# Create data directory and copy data files
# Note: Build context should be the parent directory
COPY data ./data

# Run the main script
CMD ["python", "main.py"]
