# Real-time Vessel Status Monitoring System

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![FastAPI](https://img.shields.io/badge/fastapi-0.68.0-green)
![Streamlit](https://img.shields.io/badge/streamlit-1.12.0-orange)
![Pandas](https://img.shields.io/badge/pandas-1.3.0-yellow)
![Plotly](https://img.shields.io/badge/plotly-5.0.0-blue)
![Kafka](https://img.shields.io/badge/kafka-2.8.0-red)
![AsyncIO](https://img.shields.io/badge/asyncio-%20-lightblue)

A real-time vessel position and speed monitoring system based on streaming data technology. This system continuously processes AIS data to provide real-time visualization of global vessel positions, speeds, and status information with sliding window aggregation capabilities.

## Data Source

This project uses AIS data from the repository: https://github.com/tayljordan/ais

The system uses ```Kafka``` and ```asyncio``` to simulate real-time streaming data from static AIS datasets.

## Project Structure

```
realtime-vessel-status-monitoring/
├── data/                         # Data directory
│   └── ais_data.json             # Raw AIS data file
├── requirements.txt              # Project dependencies
├── src/                          # Core source code
│   ├── data_processor.py         # AIS data processor
│   └── sliding_window_aggregator.py # Sliding window aggregator
├── streaming_processor/          # Streaming processing module
│   ├── streaming_api.py          # FastAPI streaming API
│   └── kafka_producer.py         # Kafka producer (alternative option)
└── frontend/                     # Frontend visualization
    └── vessel_dashboard.py       # Streamlit dashboard
```

## Features

### 1. Streaming Data Ingestion
- Real-time generation of simulated AIS data streams
- Update vessel position and speed every 10 seconds
- Support for both WebSocket and REST API data transmission methods

### 2. Data Cleaning and Time Synchronization
- Clean invalid coordinate data (latitude/longitude out of range)
- Handle missing values and outliers
- Timestamp standardization

### 3. Real-time Visualization
- Interactive map showing vessel positions
- Real-time statistics panel
- Speed distribution and trend charts
- Vessel type distribution visualization

### 4. Sliding Window Aggregation
- Real-time calculation of statistical metrics (average/maximum/minimum speed)
- Vessel status classification (stationary/slow/moderate/fast)
- Course and position range statistics
- Trend analysis functionality

## Installation

```bash
pip install -r requirements.txt
```

## Running the System

### 1. Start the streaming API service

```bash
cd realtime-vessel-status-monitoring
uvicorn streaming_processor.streaming_api:app --host 0.0.0.0 --port 8000
```

### 2. Start the frontend dashboard

In a new terminal window:

```bash
cd realtime-vessel-status-monitoring
streamlit run frontend/vessel_dashboard.py
```

## Technology Stack

- **Backend API**: FastAPI + asyncio
- **Streaming Data Processing**: WebSocket + REST API
- **Optional Streaming**: Apache Kafka (kafka-python)
- **Data Processing**: Pandas, NumPy
- **Visualization**: Streamlit, Plotly, Folium
- **Sliding Window Aggregation**: Custom aggregator implementation

## Core Implementation

### Data Simulation and Processing
- Simulate realistic vessel movement patterns based on AIS data characteristics
- Calculate position changes based on course and speed
- Add random variations for realistic data patterns

### Sliding Window Aggregation
- Maintain data cache within configurable time windows
- Real-time calculation of various statistical metrics
- Provide trend analysis functionality

### Visualization Components
- Real-time updating map display with color-coded vessel speeds
- Dynamic statistics metrics showing fleet status
- Interactive trend and distribution charts