#!/usr/bin/env python3
import asyncio
import aiomqtt
import json
import logging
import argparse
import sys
from datetime import datetime
import sqlite3
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

class AsyncMQTTForwarder:
    def __init__(self, db_path: str, mqtt_host: str, mqtt_port: int = 1883,
                 mqtt_username: Optional[str] = None, mqtt_password: Optional[str] = None,
                 mqtt_use_tls: bool = False):
        """Initialize the async MQTT forwarder"""
        self.db_path = db_path
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_use_tls = mqtt_use_tls
        
        self.setup_logging()
        self.message_queue = asyncio.Queue()
        self.is_running = False
        self.reconnect_interval = 5  # seconds
        self.mqtt_client = None
        self.executor = ThreadPoolExecutor(max_workers=1)

    def setup_logging(self):
        """Configure logging"""
        self.logger = logging.getLogger('AsyncMQTTForwarder')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    async def connect_mqtt(self):
        """Create and connect MQTT client"""
        while True:
            try:
                # Create new client for each connection attempt
                self.mqtt_client = aiomqtt.Client(
                    hostname=self.mqtt_host,
                    port=self.mqtt_port,
                    username=self.mqtt_username,
                    password=self.mqtt_password,
                    tls=self.mqtt_use_tls
                )
                await self.mqtt_client.connect()
                self.logger.info("Connected to MQTT broker")
                return True
            except Exception as e:
                self.logger.error(f"Failed to connect to MQTT broker: {e}")
                await asyncio.sleep(self.reconnect_interval)

    def format_mqtt_topic(self, contest_data: Dict[str, Any]) -> str:
        """Format MQTT topic based on contest data"""
        contest = contest_data.get('contest', 'unknown').replace(' ', '_')
        callsign = contest_data.get('callsign', 'unknown')
        category = contest_data.get('category', 'unknown')
        power = contest_data.get('power', 'unknown').lower()
        
        return f"contest/score/{contest}/{category}/{power}/{callsign}"

    def format_mqtt_payload(self, contest_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format contest data for MQTT publishing"""
        return {
            "timestamp": contest_data.get('timestamp'),
            "callsign": contest_data.get('callsign'),
            "contest": contest_data.get('contest'),
            "score": contest_data.get('score'),
            "qsos": contest_data.get('qsos'),
            "multipliers": contest_data.get('multipliers'),
            "band_breakdown": contest_data.get('band_breakdown', []),
            "power": contest_data.get('power'),
            "assisted": contest_data.get('assisted'),
            "category": contest_data.get('category'),
            "club": contest_data.get('club'),
            "section": contest_data.get('section'),
            "qth": contest_data.get('qth', {})
        }

    async def process_message(self, contest_data: Dict[str, Any]):
        """Process and publish contest data to MQTT"""
        try:
            if not self.mqtt_client or not self.mqtt_client.is_connected():
                await self.connect_mqtt()

            topic = self.format_mqtt_topic(contest_data)
            payload = self.format_mqtt_payload(contest_data)
            json_payload = json.dumps(payload)
            
            await self.mqtt_client.publish(
                topic=topic,
                payload=json_payload.encode(),
                qos=1
            )
            self.logger.debug(f"Published to {topic}")
            
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")
            # Force reconnection on next message
            self.mqtt_client = None

    async def process_queue(self):
        """Process messages from queue asynchronously"""
        while self.is_running:
            try:
                contest_data = await self.message_queue.get()
                await self.process_message(contest_data)
                self.message_queue.task_done()
            except Exception as e:
                self.logger.error(f"Error processing queue: {e}")
                await asyncio.sleep(1)

    async def start(self):
        """Start the async MQTT forwarder"""
        try:
            self.is_running = True
            self.logger.info("Starting async MQTT forwarder")
            await self.connect_mqtt()
            
            # Start queue processing
            asyncio.create_task(self.process_queue())
            
        except Exception as e:
            self.logger.error(f"Error starting forwarder: {e}")
            raise

    async def stop(self):
        """Stop the async MQTT forwarder"""
        self.is_running = False
        if self.mqtt_client:
            await self.mqtt_client.disconnect()
        await self.message_queue.join()
        self.executor.shutdown()
        self.logger.info("MQTT forwarder stopped")

    async def add_message(self, contest_data: Dict[str, Any]):
        """Add contest data to processing queue"""
        await self.message_queue.put(contest_data)

async def run_forwarder(args):
    """Run the MQTT forwarder"""
    forwarder = AsyncMQTTForwarder(
        args.db,
        args.mqtt_host,
        args.mqtt_port,
        args.mqtt_username,
        args.mqtt_password,
        args.mqtt_use_tls
    )
    
    if args.debug:
        forwarder.logger.setLevel(logging.DEBUG)
    
    try:
        await forwarder.start()
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await forwarder.stop()
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

def main():
    """Main function for running the MQTT forwarder"""
    parser = argparse.ArgumentParser(description='Async Contest Score MQTT Forwarder')
    parser.add_argument('--db', default='contest_data.db',
                      help='Database file path')
    parser.add_argument('--mqtt-host', required=True,
                      help='MQTT broker hostname')
    parser.add_argument('--mqtt-port', type=int, default=1883,
                      help='MQTT broker port')
    parser.add_argument('--mqtt-username',
                      help='MQTT username')
    parser.add_argument('--mqtt-password',
                      help='MQTT password')
    parser.add_argument('--mqtt-use-tls', action='store_true',
                      help='Use TLS for MQTT connection')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Run the async forwarder
    asyncio.run(run_forwarder(args))

if __name__ == "__main__":
    main()
    
