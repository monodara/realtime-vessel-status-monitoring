import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional
import statistics
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlidingWindowAggregator:
    """
    Sliding window aggregator for computing real-time aggregate statistics of vessel data
    """
    
    def __init__(self, window_duration_minutes: int = 5):
        """
        Initialize sliding window aggregator
        
        Args:
            window_duration_minutes: Window duration in minutes
        """
        self.window_duration_minutes = window_duration_minutes
        self.data_buffer = deque()  # Store timestamped data
        self.max_buffer_size = 10000  # Limit buffer size
        
    def add_data(self, vessels: List[Dict], timestamp: Optional[datetime] = None):
        """
        Add new vessel data to sliding window
        
        Args:
            vessels: List of vessel data
            timestamp: Timestamp, use current time if not provided
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        for vessel in vessels:
            # Add timestamp to each record
            record = {
                'timestamp': timestamp,
                **vessel
            }
            self.data_buffer.append(record)
        
        # Clean up old data beyond window time
        self._cleanup_old_data()
        
        # Limit buffer size
        if len(self.data_buffer) > self.max_buffer_size:
            # Remove oldest elements
            while len(self.data_buffer) > self.max_buffer_size * 0.8:
                self.data_buffer.popleft()
    
    def _cleanup_old_data(self):
        """Clean up old data beyond sliding window time range"""
        time_threshold = datetime.utcnow() - timedelta(minutes=self.window_duration_minutes)
        
        # Remove outdated data
        while self.data_buffer and self.data_buffer[0]['timestamp'] < time_threshold:
            self.data_buffer.popleft()
    
    def get_current_window_data(self) -> List[Dict]:
        """Get all data within current window"""
        time_threshold = datetime.utcnow() - timedelta(minutes=self.window_duration_minutes)
        return [record for record in self.data_buffer if record['timestamp'] >= time_threshold]
    
    def calculate_aggregates(self) -> Dict:
        """
        Calculate aggregate statistics in sliding window
        
        Returns:
            Aggregate statistics dictionary
        """
        if not self.data_buffer:
            return {}
        
        # Get data within window
        window_data = self.get_current_window_data()
        if not window_data:
            return {}
        
        df = pd.DataFrame(window_data)
        
        # Calculate aggregate metrics
        aggregates = {
            'window_duration_minutes': self.window_duration_minutes,
            'data_points_count': len(window_data),
            'timestamp': datetime.utcnow().isoformat(),
        }
        
        # Calculate location-related metrics
        if 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
            # Active area boundaries
            if not df.empty:
                aggregates.update({
                    'latitude_range': {
                        'min': float(df['LATITUDE'].min()),
                        'max': float(df['LATITUDE'].max()),
                        'avg': float(df['LATITUDE'].mean())
                    },
                    'longitude_range': {
                        'min': float(df['LONGITUDE'].min()),
                        'max': float(df['LONGITUDE'].max()),
                        'avg': float(df['LONGITUDE'].mean())
                    }
                })
        
        # Calculate speed-related metrics
        if 'SOG' in df.columns:
            speed_data = pd.to_numeric(df['SOG'], errors='coerce').dropna()
            if len(speed_data) > 0:
                aggregates.update({
                    'speed_stats': {
                        'count': int(len(speed_data)),
                        'mean': float(speed_data.mean()),
                        'median': float(speed_data.median()),
                        'std': float(speed_data.std()) if len(speed_data) > 1 else 0.0,
                        'min': float(speed_data.min()),
                        'max': float(speed_data.max()),
                        'q25': float(speed_data.quantile(0.25)),
                        'q75': float(speed_data.quantile(0.75))
                    },
                    'vessel_status': {
                        'stationary': int((speed_data == 0).sum()),  # Stationary vessels
                        'slow': int(((speed_data > 0) & (speed_data <= 5)).sum()),  # Slow vessels
                        'moderate': int(((speed_data > 5) & (speed_data <= 15)).sum()),  # Moderate vessels
                        'fast': int((speed_data > 15).sum())  # Fast vessels
                    }
                })
        
        # Calculate course-related metrics
        if 'COG' in df.columns:
            course_data = pd.to_numeric(df['COG'], errors='coerce').dropna()
            if len(course_data) > 0:
                aggregates.update({
                    'course_stats': {
                        'count': int(len(course_data)),
                        'mean': float(course_data.mean()),
                        'std': float(course_data.std()) if len(course_data) > 1 else 0.0,
                        'min': float(course_data.min()),
                        'max': float(course_data.max())
                    }
                })
        
        # Vessel type statistics
        if 'TYPE' in df.columns:
            type_counts = df['TYPE'].value_counts()
            aggregates['vessel_type_distribution'] = type_counts.to_dict()
        
        # Calculate unique MMSI count (deduplication)
        if 'MMSI' in df.columns:
            aggregates['unique_vessels'] = int(df['MMSI'].nunique())
        
        return aggregates
    
    def calculate_trend(self, metric: str = 'SOG', lookback_windows: List[int] = [1, 5, 15]) -> Dict:
        """
        Calculate metric trends
        
        Args:
            metric: Metric to calculate trend for
            lookback_windows: Lookback windows in minutes
            
        Returns:
            Trend analysis results
        """
        if not self.data_buffer or metric not in self.data_buffer[0]:
            return {}
        
        trends = {}
        
        for window in lookback_windows:
            # Get data within specified time window
            time_threshold = datetime.utcnow() - timedelta(minutes=window)
            window_data = [record for record in self.data_buffer if record['timestamp'] >= time_threshold]
            
            if window_data:
                df = pd.DataFrame(window_data)
                if metric in df.columns:
                    values = pd.to_numeric(df[metric], errors='coerce').dropna()
                    if len(values) > 0:
                        trends[f'window_{window}_min'] = {
                            'count': int(len(values)),
                            'mean': float(values.mean()),
                            'trend': 'increasing' if len(values) > 1 and values.iloc[-1] > values.iloc[0] else 'decreasing'
                        }
        
        return {
            'metric': metric,
            'trends': trends,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_active_vessels(self) -> List[Dict]:
        """Get active (moving) vessels within window"""
        window_data = self.get_current_window_data()
        df = pd.DataFrame(window_data)
        
        if 'SOG' in df.columns:
            active_df = df[df['SOG'] > 0.5]  # Vessels with speed > 0.5 knots considered active
            return active_df.to_dict('records')
        else:
            return []
    
    def get_stationary_vessels(self) -> List[Dict]:
        """Get stationary vessels within window"""
        window_data = self.get_current_window_data()
        df = pd.DataFrame(window_data)
        
        if 'SOG' in df.columns:
            stationary_df = df[df['SOG'] <= 0.5]  # Vessels with speed <= 0.5 knots considered stationary
            return stationary_df.to_dict('records')
        else:
            return []

# Example usage
if __name__ == "__main__":
    # Create sliding window aggregator (5-minute window)
    aggregator = SlidingWindowAggregator(window_duration_minutes=5)
    
    # Simulate some data
    sample_vessels = [
        {
            'MMSI': '123456789',
            'NAME': 'Vessel_A',
            'LATITUDE': 40.7128,
            'LONGITUDE': -74.0060,
            'SOG': 12.5,
            'COG': 180,
            'TYPE': 'Cargo'
        },
        {
            'MMSI': '987654321',
            'NAME': 'Vessel_B',
            'LATITUDE': 34.0522,
            'LONGITUDE': -118.2437,
            'SOG': 0,
            'COG': 0,
            'TYPE': 'Tanker'
        }
    ]
    
    # Add data
    aggregator.add_data(sample_vessels)
    
    # Calculate aggregate stats
    aggregates = aggregator.calculate_aggregates()
    print("Aggregates:", aggregates)
    
    # Calculate trends
    trends = aggregator.calculate_trend('SOG')
    print("Trends:", trends)