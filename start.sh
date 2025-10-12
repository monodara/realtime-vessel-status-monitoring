#!/bin/bash
# Script to start the real-time vessel monitoring system

echo "Real-time Vessel Status Monitoring System Startup Script"
echo "========================================================"

# Check Python environment
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 not found, please install first"
    exit 1
fi

# Check dependencies
echo "Checking dependencies..."
if ! python3 -c "import fastapi" &> /dev/null; then
    echo "Installing dependencies..."
    pip3 install -r requirements.txt
fi

echo ""
echo "Please select component to start:"
echo "1) Backend API service"
echo "2) Frontend dashboard"
echo "3) Both"
echo -n "Enter your choice (1/2/3): "
read choice

case $choice in
    1)
        echo "Starting backend API service..."
        cd streaming_processor
        uvicorn streaming_api:app --host 0.0.0.0 --port 8000
        ;;
    2)
        echo "Starting frontend dashboard..."
        cd frontend
        streamlit run vessel_dashboard.py
        ;;
    3)
        echo "Starting backend API service..."
        cd streaming_processor
        uvicorn streaming_api:app --host 0.0.0.0 --port 8000 &
        API_PID=$!
        cd ../frontend
        echo "Starting frontend dashboard..."
        streamlit run vessel_dashboard.py
        echo "Stopping backend API service..."
        kill $API_PID
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac