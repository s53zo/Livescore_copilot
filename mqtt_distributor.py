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

class ContestDataSubscriber:
    """Base class for subscribing to contest database updates"""
    def __init__(self, db_path, polling_interval=5):
        self.db_path = db_path
        self.polling_interval = polling_interval
        self.last_processed_id = 0
        self.running = True
        self.last_check_time = datetime.now()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info("Received shutdown signal, stopping...")
        self.running = False

    def get_new_records(self):
        """Fetch new records from the database since last check"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # On first run (last_processed_id = 0), get records from last 5 minutes only
                if self.last_processed_id == 0:
                    self.logger.info("First run - getting records from last 5 minutes only")
                    cursor.execute("""
                        SELECT 
                            cs.id,
                            cs.timestamp,
                            cs.contest,
                            cs.callsign,
                            cs.score,
                            cs.qsos,
                            cs.multipliers,
                            cs.power,
                            cs.assisted,
                            cs.transmitter
                        FROM contest_scores cs
                        WHERE cs.timestamp >= datetime('now', '-5 minutes')
                        ORDER BY cs.id
                    """)
                else:
                    # After first run, get records since last processed ID
                    cursor.execute("""
                        SELECT 
                            cs.id,
                            cs.timestamp,
                            cs.contest,
                            cs.callsign,
                            cs.score,
                            cs.qsos,
                            cs.multipliers,
                            cs.power,
                            cs.assisted,
                            cs.transmitter
                        FROM contest_scores cs
                        WHERE cs.id > ?
                        ORDER BY cs.id
                    """, (self.last_processed_id,))
                
                scores = cursor.fetchall()
                
                if scores:
                    self.logger.info(f"Found {len(scores)} new records")
                    if self.last_processed_id == 0:
                        self.logger.info("Processing initial records from last 5 minutes")
                    
                # If we have new scores, get their associated data
                results = []
                for score in scores:
                    score_id = score[0]
                    
                    # Get band breakdown
                    cursor.execute("""
                        SELECT band, mode, qsos, points, multipliers
                        FROM band_breakdown
                        WHERE contest_score_id = ?
                    """, (score_id,))
                    band_data = cursor.fetchall()
                    
                    # Get QTH info
                    cursor.execute("""
                        SELECT dxcc_country, cq_zone, iaru_zone, 
                               arrl_section, state_province, grid6
                        FROM qth_info
                        WHERE contest_score_id = ?
                    """, (score_id,))
                    qth_data = cursor.fetchone()
                    
                    # Combine all data
                    results.append({
                        'score_data': score,
                        'band_data': band_data,
                        'qth_data': qth_data
                    })
                    
                    # Update last processed ID
                    self.last_processed_id = score_id
                
                return results
                    
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching new records: {e}")
            return None

    def process_record(self, record):
        """Process a new contest record - override this in subclasses"""
        raise NotImplementedError("Subclasses must implement process_record")

    def run(self):
        """Main processing loop with improved polling"""
        self.logger.info(f"Starting data subscriber (polling every {self.polling_interval} seconds)...")
        
        while self.running:
            try:
                start_time = datetime.now()
                
                # Get new records
                records = self.get_new_records()
                
                if records:
                    self.logger.info(f"Found {len(records)} new records")
                    process_start = datetime.now()
                    
                    for record in records:
                        self.process_record(record)
                    
                    process_time = (datetime.now() - process_start).total_seconds()
                    self.logger.debug(f"Processed {len(records)} records in {process_time:.2f} seconds")
                
                # Calculate time until next check
                elapsed = (datetime.now() - start_time).total_seconds()
                wait_time = max(0, self.polling_interval - elapsed)
                
                if wait_time > 0:
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(f"Waiting {wait_time:.2f} seconds until next check")
                    time.sleep(wait_time)
                    
                # Log polling stats in debug mode
                if self.logger.isEnabledFor(logging.DEBUG):
                    total_time = (datetime.now() - self.last_check_time).total_seconds()
                    self.logger.debug(f"Poll cycle completed in {total_time:.2f} seconds")
                    self.last_check_time = datetime.now()
                
            except Exception as e:
                self.logger.error(f"Error in processing loop: {e}")
                self.logger.debug(traceback.format_exc())
                time.sleep(self.polling_interval)  # Wait before retry

    def cleanup(self):
        """Cleanup resources"""
        pass

class ContestMQTTPublisher(ContestDataSubscriber):
    def __init__(self, db_path, mqtt_config, debug=False, polling_interval=5):
        # Setup enhanced logging first
        self.setup_logging(debug)
        self.logger.debug("Initializing ContestMQTTPublisher")
        self.logger.debug(f"MQTT Config: {json.dumps(mqtt_config, indent=2)}")
        
        # Store MQTT config
        self.mqtt_config = mqtt_config
        
        # Initialize superclass with polling interval
        super().__init__(db_path, polling_interval)
        
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
        
        # Create MQTT client
        self.mqtt_client = mqtt.Client(client_id=self.mqtt_config.get('client_id'))
        
        # Enable MQTT client logging if in debug mode
        if self.logger.getEffectiveLevel() == logging.DEBUG:
            self.mqtt_client.enable_logger()  # Changed: removed self.logger argument
        
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
            self.logger.info("MQTT client started successfully")
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

    def build_topic(self, record):
        """
        Build MQTT topic hierarchy from contest record.
        Replaces '/' in callsigns with '_' to avoid MQTT level separation.
        Format: contest/live/v1/{contest}/{callsign_safe}
        """
        score_data = record['score_data']
        contest = score_data[2].replace(' ', '_')
        callsign = score_data[3]
        
        # Replace '/' with '_' in the callsign for MQTT topic safety
        callsign_safe = callsign.replace('/', '_') 
        
        return f"contest/live/v1/{contest}/{callsign_safe}"

    def get_contest_totals(self, contest, timestamp):
        """Get current contest totals including band breakdowns"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get latest scores for all stations in this contest
                cursor.execute("""
                    WITH latest_scores AS (
                        SELECT cs.id, cs.callsign, cs.score, cs.qsos, cs.power,
                               cs.timestamp, cs.assisted, cs.transmitter
                        FROM contest_scores cs
                        WHERE cs.contest = ? 
                        AND cs.timestamp <= ?
                        AND (cs.callsign, cs.timestamp) IN (
                            SELECT cs2.callsign, MAX(cs2.timestamp)
                            FROM contest_scores cs2
                            WHERE cs2.contest = ?
                            AND cs2.timestamp <= ?
                            GROUP BY cs2.callsign
                        )
                    )
                    SELECT 
                        ls.callsign,
                        ls.score,
                        ls.qsos,
                        ls.power,
                        ls.assisted,
                        ls.transmitter,
                        ls.id,
                        ls.timestamp
                    FROM latest_scores ls
                    ORDER BY ls.score DESC
                """, (contest, timestamp, contest, timestamp))
                
                results = []
                for row in cursor.fetchall():
                    callsign, score, qsos, power, assisted, transmitter, score_id, ts = row
                    
                    # Get band breakdown
                    cursor.execute("""
                        SELECT bb.band, SUM(bb.qsos) as total_qsos
                        FROM contest_scores cs
                        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                        WHERE cs.callsign = ? 
                        AND cs.contest = ? 
                        AND cs.timestamp = ?
                        GROUP BY bb.band
                        ORDER BY bb.band
                    """, (callsign, contest, ts))
                    
                    band_qsos = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    results.append({
                        'callsign': callsign,
                        'score': score,
                        'qsos': qsos,
                        'power': power,
                        'assisted': assisted,
                        'transmitter': transmitter,
                        'band_qsos': band_qsos
                    })
                
                self.logger.debug(f"Retrieved scores for {len(results)} stations in {contest}")
                return results
                
        except Exception as e:
            self.logger.error(f"Error getting contest totals: {e}")
            self.logger.debug(traceback.format_exc())
            return []

    def build_payload(self, record):
        """Build JSON payload with just the station's contest data"""
        try:
            score_data = record['score_data']
            qth_data = record['qth_data']
                
            # Build payload with just the essential station data
            payload = {
                "sq": score_data[0],
                "t": int(datetime.strptime(score_data[1], '%Y-%m-%d %H:%M:%S').timestamp()),
                "contest": score_data[2],
                "callsign": score_data[3],
                "score": score_data[4],
                "qsos": score_data[5],
                "mults": score_data[6],
                "power": score_data[7],
                "assisted": score_data[8],
                "tx": score_data[9],
                "bands": {},
            }
    
            # Add band breakdown if available
            if record['band_data']:
                for band_info in record['band_data']:
                    band_key = f"{band_info[0]}m"
                    payload["bands"][band_key] = {
                        "mode": band_info[1],
                        "qsos": band_info[2],
                        "points": band_info[3],
                        "mults": band_info[4]
                    }
    
            # Add QTH info if available
            if qth_data:
                payload["qth"] = {
                    "dxcc": qth_data[0],
                    "cqz": qth_data[1],
                    "ituz": qth_data[2],
                    "section": qth_data[3],
                    "state": qth_data[4],
                    "grid": qth_data[5]
                }
    
            return json.dumps(payload)
                
        except Exception as e:
            self.logger.error(f"Error building payload: {e}")
            self.logger.debug(traceback.format_exc())
            return None

    def process_record(self, record):
        """Process and publish contest record with enhanced logging"""
        try:
            self.logger.debug(f"Processing record: {json.dumps(record['score_data'])}")
            
            # Build topic and payload
            topic = self.build_topic(record)
            payload = self.build_payload(record)
            
            if payload:
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

    def cleanup(self):
        """Cleanup resources and stop MQTT client"""
        try:
            self.logger.info("Stopping MQTT client...")
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self.logger.info("MQTT client stopped")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
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
    
    parser.add_argument('--poll-interval', type=int, default=5,
                       help='Database polling interval in seconds (default: 5)')

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
        # Create publisher instance with polling interval
        publisher = ContestMQTTPublisher(
            db_path=args.db,
            mqtt_config=mqtt_config,
            debug=args.debug,
            polling_interval=args.poll_interval  # Pass the polling interval
        )
        
        # Log startup information
        if args.debug:
            publisher.logger.info("Contest Score MQTT Publisher starting up")
            publisher.logger.debug("Configuration:")
            publisher.logger.debug(f"  Database: {args.db}")
            publisher.logger.debug(f"  MQTT Host: {args.host}")
            publisher.logger.debug(f"  MQTT Port: {args.port}")
            publisher.logger.debug(f"  MQTT TLS: {args.tls}")
            publisher.logger.debug(f"  Polling Interval: {args.poll_interval} seconds")
            publisher.logger.debug(f"  Debug Mode: ON")
        
        # Run the publisher
        publisher.run()
        
    except KeyboardInterrupt:
        if publisher:
            publisher.logger.info("Shutting down...")
        else:
            logger.info("Shutting down...")
    except Exception as e:
        if publisher:
            publisher.logger.error(f"Fatal error: {e}")
            if args.debug:
                publisher.logger.debug(traceback.format_exc())
        else:
            logger.error(f"Fatal error during initialization: {e}")
            if args.debug:
                logger.debug(traceback.format_exc())
        sys.exit(1)
    finally:
        if publisher:
            publisher.cleanup()

if __name__ == "__main__":
    main()
