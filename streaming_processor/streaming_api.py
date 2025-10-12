import asyncio
import json
import uvicorn
import sys
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List, Dict
import time
import threading
from datetime import datetime

# Add the project root to Python path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processor import AISDataProcessor
from src.sliding_window_aggregator import SlidingWindowAggregator

app = FastAPI(title="Real-time Vessel Status Streaming API",
              description="Streaming API for real-time vessel position and status data",
              version="1.0.0")

# Global data processor
processor = AISDataProcessor("data/ais_data.json")

# Sliding window aggregator
aggregator = SlidingWindowAggregator(window_duration_minutes=5)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                # If sending fails, remove connection
                self.disconnect(connection)

manager = ConnectionManager()

@app.websocket("/ws/vessel-data")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for streaming vessel data"""
    await manager.connect(websocket)
    try:
        while True:
            # Generate new streaming data
            streaming_data = processor.generate_streaming_data()
            
            # Add timestamp
            for vessel in streaming_data:
                vessel['TIMESTAMP'] = datetime.utcnow().isoformat()
            
            # Update sliding window aggregator
            aggregator.add_data(streaming_data)
            
            # Calculate sliding window aggregates
            window_aggregates = aggregator.calculate_aggregates()
            trend_data = aggregator.calculate_trend('SOG')
            
            # Send data
            message = {
                "timestamp": datetime.utcnow().isoformat(),
                "vessels": streaming_data,
                "stats": processor.get_vessel_stats(streaming_data),
                "sliding_window_aggregates": window_aggregates,
                "trend_data": trend_data
            }
            
            await manager.send_personal_message(json.dumps(message), websocket)
            
            # Wait 10 seconds
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/vessel-data")
async def get_vessel_data():
    """REST API endpoint for current vessel data"""
    streaming_data = processor.generate_streaming_data()
    
    # Update sliding window aggregator
    aggregator.add_data(streaming_data)
    
    stats = processor.get_vessel_stats(streaming_data)
    window_aggregates = aggregator.calculate_aggregates()
    trend_data = aggregator.calculate_trend('SOG')
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "vessels": streaming_data,
        "stats": stats,
        "sliding_window_aggregates": window_aggregates,
        "trend_data": trend_data
    }

@app.get("/api/vessel-stats")
async def get_vessel_stats(window_minutes: int = 5):
    """Get sliding window statistics"""
    # Update aggregator's time window
    temp_aggregator = SlidingWindowAggregator(window_duration_minutes=window_minutes)
    
    # Get current aggregate data
    window_aggregates = aggregator.calculate_aggregates()
    
    return {
        "window_minutes": window_minutes,
        "aggregates": window_aggregates,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/sliding-window-aggregates")
async def get_sliding_window_aggregates():
    """Get sliding window aggregate data"""
    aggregates = aggregator.calculate_aggregates()
    trend_data = aggregator.calculate_trend('SOG')
    
    return {
        "sliding_window_aggregates": aggregates,
        "trend_data": trend_data,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/")
async def read_root():
    """Root endpoint"""
    return {"message": "Real-time Vessel Status Streaming API", "status": "running"}

if __name__ == "__main__":
    # Run data push in background
    uvicorn.run(app, host="0.0.0.0", port=8000)