import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AISDataProcessor:
    """
    AIS Data Processor for cleaning, transforming and simulating streaming data
    """
    
    def __init__(self, data_file: str = "data/ais_data.json"):
        """
        Initialize the data processor
        
        Args:
            data_file: Path to AIS data file
        """
        self.data_file = data_file
        self.raw_data = None
        self.processed_data = None
        self.vessel_positions = {}  # Store current vessel positions
        self.load_data()
        
    def load_data(self):
        """Load AIS data"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                self.raw_data = json.load(f)
            logger.info(f"Loaded {len(self.raw_data)} AIS records from {self.data_file}")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            self.raw_data = []
    
    def clean_data(self) -> pd.DataFrame:
        """
        Clean and preprocess AIS data
        
        Returns:
            Cleaned DataFrame
        """
        if not self.raw_data:
            logger.warning("No raw data to clean")
            return pd.DataFrame()
        
        # Convert raw data to DataFrame
        df = pd.DataFrame(self.raw_data)
        
        # Convert data types
        numeric_columns = ['LATITUDE', 'LONGITUDE', 'COG', 'SOG', 'HEADING', 'DRAUGHT', 'A', 'B', 'C', 'D']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert timestamps
        if 'TSTAMP' in df.columns:
            df['TSTAMP'] = pd.to_datetime(df['TSTAMP'], format='%Y-%m-%d %H:%M:%S GMT', errors='coerce')
        
        # Remove invalid coordinates (NaN or out of range)
        df = df.dropna(subset=['LATITUDE', 'LONGITUDE'])
        df = df[(df['LATITUDE'] >= -90) & (df['LATITUDE'] <= 90)]
        df = df[(df['LONGITUDE'] >= -180) & (df['LONGITUDE'] <= 180)]
        
        # Remove negative SOG values
        if 'SOG' in df.columns:
            df = df[df['SOG'] >= 0]
        
        logger.info(f"Cleaned data: {len(df)} valid records out of {len(self.raw_data)} total records")
        self.processed_data = df
        return df
    
    def simulate_vessel_movement(self, vessel_data: Dict, time_delta: timedelta) -> Dict:
        """
        Simulate vessel position movement
        
        Args:
            vessel_data: Current vessel data
            time_delta: Time change amount
            
        Returns:
            Updated vessel data
        """
        if pd.isna(vessel_data.get('SOG')) or pd.isna(vessel_data.get('COG')):
            return vessel_data
        
        # If speed is 0, keep original position
        if vessel_data['SOG'] == 0:
            return vessel_data
        
        # Calculate position change (simplified model)
        # 1 degree of longitude is approximately 111.32 km
        # 1 degree of latitude is approximately 111.32 km
        speed = vessel_data['SOG']  # knots (nautical miles per hour)
        course = vessel_data['COG']  # course (degrees)
        
        # Convert time change to hours
        hours = time_delta.total_seconds() / 3600.0
        
        # Calculate displacement (nautical miles)
        distance_nautical_miles = speed * hours
        
        # Convert distance to lat/lon changes (simplified calculation)
        if 0 <= course < 360:
            # Convert angle to radians
            course_rad = np.radians(course)
            
            # Calculate latitude and longitude changes
            lat_delta = distance_nautical_miles * np.cos(course_rad) / 60.0  # 1 degree = 60 nautical miles
            lon_delta = distance_nautical_miles * np.sin(course_rad) / (60.0 * np.cos(np.radians(vessel_data['LATITUDE'])))
            
            # Update position
            new_lat = vessel_data['LATITUDE'] + lat_delta
            new_lon = vessel_data['LONGITUDE'] + lon_delta
            
            # Check boundaries
            new_lat = max(-90, min(90, new_lat))
            new_lon = max(-180, min(180, new_lon))
            
            # Create new data copy
            updated_data = vessel_data.copy()
            updated_data['LATITUDE'] = new_lat
            updated_data['LONGITUDE'] = new_lon
            updated_data['TSTAMP'] = datetime.utcnow()
            
            # Randomly add some realism - speed and course may change slightly
            variation_factor = random.uniform(0.95, 1.05)
            updated_data['SOG'] = max(0, vessel_data['SOG'] * variation_factor)
            
            # Course may also change (unless at anchor)
            if vessel_data['SOG'] > 0.5:  # Only change course when moving
                heading_change = random.uniform(-5, 5)
                updated_data['COG'] = (vessel_data['COG'] + heading_change) % 360
            
            return updated_data
        else:
            return vessel_data
    
    def get_random_vessels(self, count: int = 10) -> List[Dict]:
        """
        Get random vessel data (for simulation)
        
        Args:
            count: Number of vessels
            
        Returns:
            List of vessel data
        """
        if self.processed_data is None:
            self.clean_data()
        
        if self.processed_data.empty:
            logger.warning("No processed data available")
            return []
        
        # Randomly select specified number of vessels
        sampled = self.processed_data.sample(n=min(count, len(self.processed_data)), replace=True)
        return sampled.to_dict('records')
    
    def generate_streaming_data(self) -> List[Dict]:
        """
        Generate data for streaming
        
        Returns:
            Updated vessel data list
        """
        if not self.vessel_positions:
            # If no initial data, select some vessels from raw data
            initial_vessels = self.get_random_vessels(20)
            for vessel in initial_vessels:
                self.vessel_positions[vessel.get('MMSI')] = vessel
        
        updated_vessels = []
        time_now = datetime.utcnow()
        
        # Update each vessel's position
        for mmsi, vessel in list(self.vessel_positions.items()):
            # Use shorter time increment (10 seconds) to simulate 10-second updates
            time_delta = timedelta(seconds=10)
            updated_vessel = self.simulate_vessel_movement(vessel, time_delta)
            
            # Update timestamp
            updated_vessel['TSTAMP'] = time_now
            
            # Randomly decide whether to update this vessel's position (simulate data update frequency)
            if random.random() > 0.3:  # 70% probability to update
                self.vessel_positions[mmsi] = updated_vessel
            
            updated_vessels.append(self.vessel_positions[mmsi])
        
        # Randomly add new vessels or remove some vessels to simulate real scenario
        if random.random() > 0.8:  # 20% probability to add new vessels
            new_vessels = self.get_random_vessels(random.randint(1, 3))
            for vessel in new_vessels:
                mmsi = vessel.get('MMSI')
                if mmsi not in self.vessel_positions:
                    self.vessel_positions[mmsi] = vessel
        
        # Remove some vessels (simulate leaving the area)
        if len(self.vessel_positions) > 10 and random.random() > 0.9:  # 10% probability to remove vessels
            mmsi_list = list(self.vessel_positions.keys())
            vessels_to_remove = random.sample(mmsi_list, min(2, len(mmsi_list)))
            for mmsi in vessels_to_remove:
                del self.vessel_positions[mmsi]
        
        return updated_vessels

    def get_vessel_stats(self, vessels: List[Dict]) -> Dict:
        """
        Get vessel statistics
        
        Args:
            vessels: List of vessel data
            
        Returns:
            Statistics dictionary
        """
        if not vessels:
            return {}
        
        df = pd.DataFrame(vessels)
        
        stats = {
            'total_vessels': len(vessels),
            'avg_sog': df['SOG'].mean() if 'SOG' in df.columns else 0,
            'max_sog': df['SOG'].max() if 'SOG' in df.columns else 0,
            'min_sog': df['SOG'].min() if 'SOG' in df.columns else 0,
            'vessel_types': df['TYPE'].value_counts().to_dict() if 'TYPE' in df.columns else {},
            'active_vessels': len(df[df['SOG'] > 0]) if 'SOG' in df.columns else 0,
            'stationary_vessels': len(df[df['SOG'] == 0]) if 'SOG' in df.columns else 0
        }
        
        return stats

if __name__ == "__main__":
    # Test data processor
    processor = AISDataProcessor("data/ais_data.json")
    cleaned_data = processor.clean_data()
    print(f"Cleaned data shape: {cleaned_data.shape}")
    
    # Test streaming data generation
    streaming_data = processor.generate_streaming_data()
    print(f"Generated {len(streaming_data)} streaming records")
    print("Sample record:", streaming_data[0] if streaming_data else None)