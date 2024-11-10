#!/usr/bin/env python3
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import logging
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import traceback
import sqlite3
import re
import threading
import time
import queue
from callsign_utils import CallsignLookup
import asyncio
from mqtt_forwarder import AsyncMQTTForwarder

# Add BatchProcessor class at the top level, before ContestDatabaseHandler
class BatchProcessor:
    def __init__(self, db_handler, batch_interval=60):
        self.db_handler = db_handler
        self.batch_interval = batch_interval  # seconds
        self.queue = queue.Queue()
        self.is_running = False
        self.processing_thread = None
        self.batch_size = 0  # Track size for logging
        
    def start(self):
        """Start the batch processing thread"""
        if not self.is_running:
            self.is_running = True
            self.processing_thread = threading.Thread(target=self._process_batch_loop)
            self.processing_thread.daemon = True
            self.processing_thread.start()
            logging.info("Batch processor started")
    
    def stop(self):
        """Stop the batch processing thread"""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join()
            logging.info("Batch processor stopped")
    
    def add_to_batch(self, xml_data):
        """Add XML data to processing queue"""
        self.queue.put(xml_data)
        self.batch_size += 1
        logging.debug(f"Added to batch. Current size: {self.batch_size}")
    
    def _process_batch_loop(self):
        """Main processing loop - runs every batch_interval seconds"""
        while self.is_running:
            start_time = time.time()
            batch = []
            
            # Collect all available items from queue
            try:
                while True:
                    batch.append(self.queue.get_nowait())
                    self.batch_size -= 1
            except queue.Empty:
                pass
            
            # Process batch if we have any items
            if batch:
                try:
                    batch_start = time.time()
                    logging.info(f"Processing batch of {len(batch)} items")
                    
                    # Combine all XML data
                    combined_xml = "\n".join(batch)
                    
                    # Process combined data
                    contest_data = self.db_handler.parse_xml_data(combined_xml)
                    if contest_data:
                        self.db_handler.store_data(contest_data)
                    
                    batch_time = time.time() - batch_start
                    logging.info(f"Batch processed in {batch_time:.2f} seconds")
                    
                except Exception as e:
                    logging.error(f"Error processing batch: {e}")
            
            # Calculate sleep time for next batch
            elapsed = time.time() - start_time
            sleep_time = max(0, self.batch_interval - elapsed)
            time.sleep(sleep_time)

class ContestDatabaseHandler:
    def __init__(self, db_path='contest_data.db', mqtt_host='localhost', mqtt_port=1883,
                 mqtt_username=None, mqtt_password=None, mqtt_use_tls=False):
        self.db_path = db_path
        self.callsign_lookup = CallsignLookup()
        self.setup_database()
        self.batch_processor = BatchProcessor(self)
        self.batch_processor.start()
        # Create event loop for async operations
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Create MQTT forwarder instance
        self.mqtt_forwarder = AsyncMQTTForwarder(
            self.db_path, mqtt_host, mqtt_port, mqtt_username, mqtt_password, mqtt_use_tls
        )
        self.mqtt_forwarder.start()

    def process_submission(self, xml_data):
        """Add submission to batch instead of processing immediately"""
        self.batch_processor.add_to_batch(xml_data)

    def cleanup(self):
        self.batch_processor.stop()
        self.mqtt_forwarder.stop()
        if hasattr(self, 'loop'):
            self.loop.close()

    def setup_database(self):
        """Create the database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS contest_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    contest TEXT,
                    callsign TEXT,
                    power TEXT,
                    assisted TEXT,
                    transmitter TEXT,
                    ops TEXT,
                    bands TEXT,
                    mode TEXT,
                    overlay TEXT,
                    club TEXT,
                    section TEXT,
                    score INTEGER,
                    qsos INTEGER,
                    multipliers INTEGER,
                    points INTEGER
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS band_breakdown (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contest_score_id INTEGER,
                    band TEXT,
                    mode TEXT,
                    qsos INTEGER,
                    points INTEGER,
                    multipliers INTEGER,
                    FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
                )
            ''')
    
            conn.execute('''
                CREATE TABLE IF NOT EXISTS qth_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contest_score_id INTEGER,
                    dxcc_country TEXT,
                    cq_zone TEXT,
                    iaru_zone TEXT,
                    arrl_section TEXT,
                    state_province TEXT,
                    grid6 TEXT,
                    FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
                )
            ''')

    def parse_xml_data(self, xml_data):
        """Parse XML data and return structured contest data."""
        xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', xml_data, re.DOTALL)
        
        results = []
        for xml_doc in xml_docs:
            try:
                root = ET.fromstring(xml_doc)
                callsign = root.findtext('call', '')
                
                # Get callsign info early to use prefix for country
                callsign_info = None
                if callsign:
                    callsign_info = self.callsign_lookup.get_callsign_info(callsign)
                
                # Extract basic contest data
                contest_data = {
                    'contest': root.findtext('contest', ''),
                    'callsign': callsign,
                    'timestamp': root.findtext('timestamp', ''),
                    'club': root.findtext('club', '').strip(),
                    'section': root.find('.//qth/arrlsection').text if root.find('.//qth/arrlsection') is not None else '',
                    'score': int(root.findtext('score', 0))
                }
                
                # Extract class attributes
                class_elem = root.find('class')
                if class_elem is not None:
                    contest_data.update({
                        'power': class_elem.get('power', ''),
                        'assisted': class_elem.get('assisted', ''),
                        'transmitter': class_elem.get('transmitter', ''),
                        'ops': class_elem.get('ops', ''),
                        'bands': class_elem.get('bands', ''),
                        'mode': class_elem.get('mode', '')
                    })
                
                # Extract QTH data and use prefix instead of country name
                qth_elem = root.find('qth')
                if qth_elem is not None:
                    qth_data = {
                        'cq_zone': qth_elem.findtext('cqzone', ''),
                        'iaru_zone': qth_elem.findtext('iaruzone', ''),
                        'arrl_section': qth_elem.findtext('arrlsection', ''),
                        'state_province': qth_elem.findtext('stprvoth', ''),
                        'grid6': qth_elem.findtext('grid6', '')
                    }
                    
                    # Use prefix from callsign_info instead of country name
                    if callsign_info:
                        qth_data['dxcc_country'] = callsign_info['prefix']
                        qth_data['continent'] = callsign_info['continent']
                    else:
                        qth_data['dxcc_country'] = ''
                        qth_data['continent'] = ''
                    
                    contest_data['qth'] = qth_data
                
                # Extract breakdown totals [rest of the code remains the same]
                breakdown = root.find('breakdown')
                if breakdown is not None:
                    # Get total QSOs, points, and multipliers
                    contest_data['qsos'] = int(breakdown.findtext('qso[@band="total"][@mode="ALL"]', 0)) or sum(int(elem.text) for elem in breakdown.findall('qso[@band="total"]'))
                    contest_data['points'] = int(breakdown.findtext('point[@band="total"][@mode="ALL"]', 0)) or sum(int(elem.text) for elem in breakdown.findall('point[@band="total"]'))
                    contest_data['multipliers'] = int(breakdown.findtext('mult[@band="total"][@mode="ALL"]', 0)) or sum(int(elem.text) for elem in breakdown.findall('mult[@band="total"]'))
                    
                    # Extract per-band breakdown
                    bands = ['160', '80', '40', '20', '15', '10']
                    contest_data['band_breakdown'] = []
                    for band in bands:
                        qsos = sum(int(elem.text) for elem in breakdown.findall(f'qso[@band="{band}"]'))
                        points = sum(int(elem.text) for elem in breakdown.findall(f'point[@band="{band}"]'))
                        multipliers = sum(int(elem.text) for elem in breakdown.findall(f'mult[@band="{band}"]'))
                        
                        if qsos > 0:
                            band_data = {
                                'band': band,
                                'mode': 'ALL',
                                'qsos': qsos,
                                'points': points,
                                'multipliers': multipliers
                            }
                            contest_data['band_breakdown'].append(band_data)
                
                results.append(contest_data)
                logging.debug(f"Successfully parsed data for {contest_data['callsign']}")
            except ET.ParseError as e:
                logging.error(f"Error parsing XML: {e}")
            except Exception as e:
                logging.error(f"Error processing data: {e}")
                logging.error(traceback.format_exc())
                    
        return results


    def store_data(self, contest_data):
        """Store contest data in the database with normalized country codes (prefixes)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for data in contest_data:
                try:
                    # Insert main contest data
                    cursor.execute('''
                        INSERT INTO contest_scores (
                            timestamp, contest, callsign, power, assisted, transmitter,
                            ops, bands, mode, overlay, club, section, score, qsos,
                            multipliers, points
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        data['timestamp'], data['contest'], data['callsign'],
                        data.get('power', ''), data.get('assisted', ''),
                        data.get('transmitter', ''), data.get('ops', ''),
                        data.get('bands', ''), data.get('mode', ''),
                        data.get('overlay', ''), data['club'], data['section'],
                        data['score'], data.get('qsos', 0), data.get('multipliers', 0),
                        data.get('points', 0)
                    ))
                    
                    contest_score_id = cursor.lastrowid
                    
                    # Get callsign information - this will return the prefix
                    callsign_info = self.callsign_lookup.get_callsign_info(data['callsign'])
                    
                    # Use prefix from callsign info or fallback to empty string
                    country_prefix = callsign_info['prefix'] if callsign_info else ''
                    continent = callsign_info['continent'] if callsign_info else ''
                    
                    # Store QTH data with prefix as country code
                    cursor.execute('''
                        INSERT INTO qth_info (
                            contest_score_id, dxcc_country, continent, cq_zone, 
                            iaru_zone, arrl_section, state_province, grid6
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        contest_score_id,
                        country_prefix,  # Use prefix here, not country name
                        continent,
                        data.get('qth', {}).get('cq_zone', ''),
                        data.get('qth', {}).get('iaru_zone', ''),
                        data.get('qth', {}).get('arrl_section', ''),
                        data.get('qth', {}).get('state_province', ''),
                        data.get('qth', {}).get('grid6', '')
                    ))
                    
                    # Insert band breakdown data
                    for band_data in data.get('band_breakdown', []):
                        cursor.execute('''
                            INSERT INTO band_breakdown (
                                contest_score_id, band, mode, qsos, points, multipliers
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            contest_score_id, band_data['band'], band_data['mode'],
                            band_data['qsos'], band_data['points'],
                            band_data['multipliers']
                        ))

                    if self.mqtt_forwarder:
                        asyncio.run_coroutine_threadsafe(
                            self.mqtt_forwarder.add_message(data),
                            self.loop
                        )
                          
                except Exception as e:
                    logging.error(f"Error storing data for {data['callsign']}: {e}")
                    logging.debug("Error details:", exc_info=True)
                    raise
    
    def cleanup_old_data(self, days=3):
        """Remove data older than specified number of days."""
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # First get IDs of old records
            cursor.execute('SELECT id FROM contest_scores WHERE timestamp < ?', (cutoff_date,))
            old_ids = [row[0] for row in cursor.fetchall()]
            
            if old_ids:
                # Delete related records from all tables
                cursor.execute('DELETE FROM band_breakdown WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE timestamp < ?)', 
                             (cutoff_date,))
                cursor.execute('DELETE FROM qth_info WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE timestamp < ?)', 
                             (cutoff_date,))
                cursor.execute('DELETE FROM contest_scores WHERE timestamp < ?', (cutoff_date,))
                
                logging.debug(f"Cleaned up {len(old_ids)} old records")
                
                # Clear the callsign lookup cache periodically
                self.callsign_lookup.clear_cache()
                logging.debug("Cleared callsign lookup cache")
                
            return len(old_ids)

class ContestRequestHandler(BaseHTTPRequestHandler):
    # Standard HTTP response messages
    HTTP_RESPONSES = {
        200: "OK-Full",
        400: "Bad Request - Invalid XML format",
        403: "Forbidden - Unauthorized access",
        404: "Not Found - Invalid endpoint",
        500: "Internal Server Error - Server processing failed"
    }

    def __init__(self, *args, debug_mode=False, **kwargs):
        self.debug_mode = debug_mode
        super().__init__(*args, **kwargs)

    def debug_print(self, message, data=None):
        """Print debug information if debug mode is enabled"""
        if self.debug_mode:
            debug_info = f"[DEBUG] {message}"
            if data:
                if isinstance(data, (dict, list)):
                    debug_info += f"\n{json.dumps(data, indent=2)}"
                else:
                    debug_info += f"\n{data}"
            logging.debug(debug_info)

    def _send_response(self, status_code):
        """Helper method to send standardized HTTP response"""
        self.debug_print(f"Sending response with status code: {status_code}")
        
        response_message = self.HTTP_RESPONSES.get(status_code, "Unknown Error")
        self.debug_print("Response content:", response_message)

        self.send_response(status_code)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', len(response_message))
        self.end_headers()
        self.wfile.write(response_message.encode('utf-8'))

    def log_request_details(self):
        """Log detailed request information"""
        self.debug_print("Request Details:", {
            "path": self.path,
            "method": self.command,
            "headers": dict(self.headers),
            "client_address": self.client_address,
            "request_version": self.request_version
        })

    def validate_xml_data(self, xml_data):
        """Validate XML data format"""
        try:
            # Try to find at least one valid XML document
            xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', xml_data, re.DOTALL)
            if not xml_docs:
                return False
            # Try parsing the first document to validate XML structure
            ET.fromstring(xml_docs[0])
            return True
        except (ET.ParseError, Exception) as e:
            self.debug_print(f"XML validation error: {str(e)}")
            return False

    def do_POST(self):
        """Handle POST requests to /livescore"""
        try:
            self.log_request_details()

            # Check if path is valid
            if self.path != '/livescore':
                self.debug_print("Invalid endpoint requested")
                self._send_response(404)
                return

            # Read POST data
            content_length = int(self.headers.get('Content-Length', 0))
            self.debug_print(f"Content Length: {content_length}")

            post_data = self.rfile.read(content_length).decode('utf-8')
            self.debug_print("Received POST data:", post_data)

            # URL decode the data (using unquote_plus to handle + characters)
            decoded_data = urllib.parse.unquote_plus(post_data)
            self.debug_print("Decoded POST data:", decoded_data)

            # Validate XML data using the decoded data
            if not self.validate_xml_data(decoded_data):
                self.debug_print("Invalid XML data received")
                self._send_response(400)
                return

            # Check authorization if needed
            if not self.check_authorization():
                self.debug_print("Unauthorized access attempt")
                self._send_response(403)
                return

            # Add to batch processor instead of processing immediately
            db_handler = self.server.db_handler  # Get handler from server
            db_handler.process_submission(decoded_data)
            
            # Return success immediately
            self._send_response(200)

        except Exception as e:
            error_details = {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.debug_print("Error occurred:", error_details)
            self._send_response(500)

    def check_authorization(self):
        """
        Check if the request is authorized.
        Implement your authorization logic here.
        """
        # Example: Check for an API key in headers
        api_key = self.headers.get('X-API-Key')
        # For demonstration, returning True
        # Implement your actual authorization logic here
        return True

    def do_GET(self):
        """Handle GET requests"""
        self.log_request_details()
        
        if self.path == '/health':
            self.debug_print("Health check requested")
            self._send_response(200)
        else:
            self.debug_print(f"Invalid path requested: {self.path}")
            self._send_response(404)

def setup_logging(debug_mode, log_file):
    """Setup logging configuration"""
    log_level = logging.DEBUG if debug_mode else logging.INFO
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Contest Data Server')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='Enable debug mode')
    parser.add_argument('--host', default='127.0.0.1',
                      help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8088,
                      help='Port to bind to (default: 8088)')
    parser.add_argument('--log-file', default='contest_server.log',
                      help='Log file path (default: contest_server.log)')
    parser.add_argument('--db-file', default='contest_data.db',
                      help='Database file path (default: contest_data.db)')
    return parser.parse_args()

def run_server(host='127.0.0.1', port=8088, debug=False):
    class CustomServer(HTTPServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.db_handler = ContestDatabaseHandler()
        
        def server_close(self):
            self.db_handler.cleanup()
            super().server_close()
    
    class CustomHandler(ContestRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, debug_mode=debug, **kwargs)
    
    server_address = (host, port)
    httpd = CustomServer(server_address, CustomHandler)
    
    logging.info(f"Starting server on {host}:{port} with batch processing")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    finally:
        httpd.server_close()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging(args.debug, args.log_file)
    
    # Log startup information
    logging.info("Server starting up with configuration:")
    logging.info(f"Host: {args.host}")
    logging.info(f"Port: {args.port}")
    logging.info(f"Debug Mode: {'ON' if args.debug else 'OFF'}")
    logging.info(f"Log File: {args.log_file}")
    logging.info(f"Database File: {args.db_file}")
    
    # Run server
    run_server(args.host, args.port, args.debug)
