#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import json
import logging
import time
from datetime import datetime
import signal
import sys
import sqlite3
import argparse
import traceback

class ContestMQTTPublisher(ContestDataSubscriber):
    def __init__(self, db_path, mqtt_config, debug=False):
        """
        Initialize publisher with MQTT configuration
        
        Args:
            db_path: Path to SQLite database
            mqtt_config: Dict containing MQTT connection settings
            debug: Boolean to enable debug logging
        """
        # Setup enhanced logging first
        self.setup_logging(debug)
        self.logger.debug("Initializing ContestMQTTPublisher")
        self.logger.debug(f"MQTT Config: {json.dumps(mqtt_config, indent=2)}")
        
        # Store MQTT config
        self.mqtt_config = mqtt_config
        
        # Initialize superclass
        super().__init__(db_path)
        
        # Setup MQTT client
        self.setup_mqtt()
        
    def setup_logging(self, debug=False):
        """Configure detailed logging"""
        self.logger = logging.getLogger('ContestMQTTPublisher')
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        
        # Create formatters
        debug_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        info_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console = logging.StreamHandler()
        console.setFormatter(debug_formatter if debug else info_formatter)
        self.logger.addHandler(console)
        
        # File handler for debug log
        if debug:
            debug_handler = logging.FileHandler('mqtt_publisher_debug.log')
            debug_handler.setFormatter(debug_formatter)
            debug_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(debug_handler)

    def setup_mqtt(self):
        """Initialize MQTT client with detailed logging"""
        self.logger.debug("Setting up MQTT client")
        
        # Enable MQTT client logging if in debug mode
        if self.logger.getEffectiveLevel() == logging.DEBUG:
            mqtt.Client.enable_logger(self.logger)
        
        self.mqtt_client = mqtt.Client(client_id=self.mqtt_config.get('client_id'))
        
        # Set up callbacks with enhanced logging
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_publish = self.on_publish
        self.mqtt_client.on_log = self.on_mqtt_log
        
        # Configure authentication if provided
        if self.mqtt_config.get('username'):
            self.logger.debug("Configuring MQTT authentication")
            self.mqtt_client.username_pw_set(
                self.mqtt_config['username'],
                self.mqtt_config.get('password')
            )
        
        # Configure TLS if requested
        if self.mqtt_config.get('use_tls'):
            self.logger.debug("Configuring TLS for MQTT connection")
            self.mqtt_client.tls_set()
        
        try:
            self.logger.info(f"Connecting to MQTT broker at {self.mqtt_config['host']}:{self.mqtt_config['port']}")
            self.mqtt_client.connect(
                self.mqtt_config['host'],
                self.mqtt_config['port']
            )
            self.mqtt_client.loop_start()
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            self.logger.debug(traceback.format_exc())
            raise

    def on_connect(self, client, userdata, flags, rc):
        """Enhanced connection callback with debugging"""
        rc_codes = {
            0: "Connected successfully",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        
        if rc == 0:
            self.logger.info(f"Connected to MQTT broker at {self.mqtt_config['host']}:{self.mqtt_config['port']}")
            self.logger.debug(f"Connection flags: {flags}")
        else:
            self.logger.error(f"Connection failed: {rc_codes.get(rc, f'Unknown error ({rc})')}")

    def on_disconnect(self, client, userdata, rc):
        """Enhanced disconnection callback"""
        if rc == 0:
            self.logger.info("Cleanly disconnected from MQTT broker")
        else:
            self.logger.warning(f"Unexpectedly disconnected from MQTT broker with code: {rc}")
            self.logger.info("Attempting to reconnect...")
            try:
                self.mqtt_client.reconnect()
            except Exception as e:
                self.logger.error(f"Reconnection failed: {e}")
                self.logger.debug(traceback.format_exc())

    def on_publish(self, client, userdata, mid):
        """Callback for successful message publication"""
        self.logger.debug(f"Message {mid} published successfully")

    def on_mqtt_log(self, client, userdata, level, buf):
        """Callback for MQTT client logging"""
        self.logger.debug(f"MQTT Log: {buf}")

    def process_record(self, record):
        """Process and publish contest record with enhanced logging"""
        try:
            self.logger.debug(f"Processing record: {json.dumps(record['score_data'])}")
            
            # Build topic and payload
            topic = self.build_topic(record)
            payload = self.build_payload(record)
            
            self.logger.debug(f"Publishing to topic: {topic}")
            self.logger.debug(f"Payload: {payload}")
            
            # Publish with QoS 1 and get message info
            info = self.mqtt_client.publish(topic, payload, qos=1)
            
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.debug(f"Message queued successfully with ID: {info.mid}")
            else:
                self.logger.error(f"Failed to queue message, error code: {info.rc}")
            
        except Exception as e:
            self.logger.error(f"Error publishing record: {e}")
            self.logger.debug(traceback.format_exc())

def parse_arguments():
    """Parse and validate command line arguments"""
    parser = argparse.ArgumentParser(
        description='Contest Score MQTT Publisher',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Basic usage:
    %(prog)s --db contest_data.db --host localhost

  With authentication:
    %(prog)s --db contest_data.db --host mqtt.example.com --username user --password pass

  With TLS:
    %(prog)s --db contest_data.db --host mqtt.example.com --tls
        """
    )
    
    parser.add_argument('--db', required=True,
                       help='Path to contest database')
    
    parser.add_argument('--host', required=True,
                       help='MQTT broker hostname')
    
    parser.add_argument('--port', type=int, default=1883,
                       help='MQTT broker port (default: 1883)')
    
    parser.add_argument('--username',
                       help='MQTT username')
    
    parser.add_argument('--password',
                       help='MQTT password')
    
    parser.add_argument('--client-id',
                       help='MQTT client ID (default: auto-generated)')
    
    parser.add_argument('--tls', action='store_true',
                       help='Use TLS for MQTT connection')
    
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')

    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Configure MQTT settings from arguments
    mqtt_config = {
        'host': args.host,
        'port': args.port,
        'username': args.username,
        'password': args.password,
        'client_id': args.client_id,
        'use_tls': args.tls
    }
    
    try:
        # Create publisher instance
        publisher = ContestMQTTPublisher(
            db_path=args.db,
            mqtt_config=mqtt_config,
            debug=args.debug
        )
        
        # Log startup information
        if args.debug:
            publisher.logger.info("Contest Score MQTT Publisher starting up")
            publisher.logger.debug("Configuration:")
            publisher.logger.debug(f"  Database: {args.db}")
            publisher.logger.debug(f"  MQTT Host: {args.host}")
            publisher.logger.debug(f"  MQTT Port: {args.port}")
            publisher.logger.debug(f"  MQTT TLS: {args.tls}")
            publisher.logger.debug(f"  Debug Mode: ON")
        
        # Run the publisher
        publisher.run()
        
    except KeyboardInterrupt:
        publisher.logger.info("Shutting down...")
    except Exception as e:
        publisher.logger.error(f"Fatal error: {e}")
        if args.debug:
            publisher.logger.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        if 'publisher' in locals():
            publisher.cleanup()

if __name__ == "__main__":
    main()
