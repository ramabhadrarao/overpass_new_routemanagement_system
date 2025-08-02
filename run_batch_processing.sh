#!/bin/bash
# run_batch_processing.sh
# Script to run batch processing for 5800 routes
# Path: /run_batch_processing.sh

echo "====================================="
echo "HPCL Route Batch Processing"
echo "====================================="
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install/upgrade requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Check if MongoDB is running
echo "Checking MongoDB connection..."
python -c "from pymongo import MongoClient; client = MongoClient('mongodb://localhost:27017/'); client.server_info()" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: MongoDB is not running. Please start MongoDB first."
    exit 1
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p route_data
mkdir -p static/uploads
mkdir -p static/pdfs
mkdir -p logs

# Run batch processing
echo ""
echo "Starting batch processing..."
echo "This will process all pending routes in the database."
echo ""

# Process with specified number of workers
WORKERS=10  # Adjust based on your system capabilities

# Option 1: Process all pending routes in database
python -m utils.batch_processor --pending --workers $WORKERS

# Option 2: Process from CSV file
# python -m utils.batch_processor --csv path/to/your/routes.csv --workers $WORKERS

echo ""
echo "Batch processing completed!"
echo "Check the logs directory for detailed processing logs."












# Run batch processing:
# bash# Make script executable
# chmod +x run_batch_processing.sh

# # Run batch processing
# ./run_batch_processing.sh

# Or use Python directly:
# bash# Process from CSV
# python -m utils.batch_processor --csv your_routes.csv --workers 10

# # Process all pending routes
# python -m utils.batch_processor --pending --workers 10