#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import json
import logging
import argparse
import sys
import time
from datetime import datetime
import sqlite3
import threading
import queue
from typing import Dict, Any, Optional

class MQTTForwarder:
    def __init__(self, db_path: str, mqtt_host: str, mqtt_port: int = 1883,
                 mqtt_username: Optional[str] = None, mqtt_password: Optional[str] = None,
                 mqtt_use_tls: bool = False):
        """Initialize the MQTT forwarder"""
        self.db_path = db_path
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_use_tls = mqtt_use_tls
        
        self.setup_logging()
        self.setup_mqtt()
        self.message_queue = queue.Queue()
        self.is_running = False

    def setup_logging(self):
        """Configure logging"""
        self.logger = logging.getLogger('MQTTForwarder')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def setup_mqtt(self):
        """Setup MQTT client with connection handling"""
        self.mqtt_client = mqtt.Client()
        
        # Setup callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        
        # Configure authentication if provided
        if self.mqtt_username and self.mqtt_password:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        
        # Configure TLS if requested
        if self.mqtt_use_tls:
            self.mqtt_client.tls_set()

    def on_connect(self, client, userdata, flags, rc):
        """Callback for when MQTT connection is established"""
        if rc == 0:
            self.logger.info("Connected to MQTT broker")
        else:
            self.logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection"""
        self.logger.warning("Disconnected from MQTT broker")
        while not self.mqtt_client.is_connected():
            try:
                self.logger.info("Attempting to reconnect...")
                self.mqtt_client.reconnect()
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Reconnection failed: {e}")
                time.sleep(10)

    def format_mqtt_topic(self, contest_data: Dict[str, Any]) -> str:
        """Format MQTT topic based on contest data
        Format: contest/score/{contest}/{category}/{power}/{callsign}"""
        
        # Clean and normalize data
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

    def process_message(self, contest_data: Dict[str, Any]):
        """Process and publish contest data to MQTT"""
        try:
            topic = self.format_mqtt_topic(contest_data)
            payload = self.format_mqtt_payload(contest_data)
            
            # Convert payload to JSON string
            json_payload = json.dumps(payload)
            
            # Publish with QoS 1 to ensure delivery
            self.mqtt_client.publish(topic, json_payload, qos=1)
            self.logger.debug(f"Published to {topic}: {json_payload}")
            
        except Exception as e:
            self.logger.error(f"Error publishing message: {e}")

    def start(self):
        """Start the MQTT forwarder"""
        try:
            # Connect to MQTT broker
            self.mqtt_client.connect(self.mqtt_host, self.mqtt_port)
            self.mqtt_client.loop_start()
            
            self.is_running = True
            self.logger.info("MQTT forwarder started")
            
            # Start processing thread
            self.processing_thread = threading.Thread(target=self._process_queue)
            self.processing_thread.daemon = True
            self.processing_thread.start()
            
        except Exception as e:
            self.logger.error(f"Error starting MQTT forwarder: {e}")
            raise

    def stop(self):
        """Stop the MQTT forwarder"""
        self.is_running = False
        if hasattr(self, 'processing_thread'):
            self.processing_thread.join()
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.logger.info("MQTT forwarder stopped")

    def add_message(self, contest_data: Dict[str, Any]):
        """Add contest data to processing queue"""
        self.message_queue.put(contest_data)

    def _process_queue(self):
        """Process messages from queue"""
        while self.is_running:
            try:
                # Get message with timeout to allow checking is_running
                contest_data = self.message_queue.get(timeout=1.0)
                self.process_message(contest_data)
                self.message_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")

def main():
    """Main function for running the MQTT forwarder"""
    parser = argparse.ArgumentParser(description='Contest Score MQTT Forwarder')
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
    
    # Create and start forwarder
    forwarder = MQTTForwarder(
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
        forwarder.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        forwarder.stop()
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
