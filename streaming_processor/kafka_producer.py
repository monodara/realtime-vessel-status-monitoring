import asyncio
import json
import logging
import sys
import os
from datetime import datetime
from aiokafka import AIOKafkaProducer

# Add the project root to Python path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processor import AISDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VesselKafkaProducer:
    """
    Kafka producer for vessel data streaming
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092', topic: str = 'vessel-data'):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka server addresses
            topic: Topic name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.processor = AISDataProcessor("data/ais_data.json")
        self.producer = None
        
    async def start_producer(self):
        """Start Kafka producer"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers
        )
        await self.producer.start()
        logger.info(f"Kafka producer started for topic: {self.topic}")
        
    async def stop_producer(self):
        """Stop Kafka producer"""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    async def send_vessel_data(self):
        """Send vessel data to Kafka topic"""
        try:
            streaming_data = self.processor.generate_streaming_data()
            
            # Add timestamp
            for vessel in streaming_data:
                vessel['TIMESTAMP'] = datetime.utcnow().isoformat()
                
            message = {
                "timestamp": datetime.utcnow().isoformat(),
                "vessels": streaming_data,
                "stats": self.processor.get_vessel_stats(streaming_data)
            }
            
            # Serialize message
            message_json = json.dumps(message)
            
            # Send to Kafka
            await self.producer.send_and_wait(
                self.topic,
                message_json.encode('utf-8')
            )
            
            logger.info(f"Sent {len(streaming_data)} vessel records to Kafka topic {self.topic}")
            
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {e}")
    
    async def run_streaming(self, interval: int = 10):
        """
        Run continuous streaming
        
        Args:
            interval: Send interval in seconds
        """
        await self.start_producer()
        
        try:
            while True:
                await self.send_vessel_data()
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        finally:
            await self.stop_producer()

# Example usage
if __name__ == "__main__":
    # Create and run Kafka producer
    kafka_producer = VesselKafkaProducer()
    
    # Run streaming (in actual application, this would run in background)
    # asyncio.run(kafka_producer.run_streaming())
    print("Kafka producer module ready")