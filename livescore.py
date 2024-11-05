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

class ContestDatabaseHandler:
    def __init__(self, db_path='contest_data.db'):
        self.db_path = db_path
        self.setup_database()

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
            
            # Extract basic contest data
            contest_data = {
                'contest': root.findtext('contest', ''),
                'callsign': root.findtext('call', ''),
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
                    'mode': class_elem.get('mode', ''),
                    'overlay': class_elem.get('overlay', '')
                })
            
            # Extract QTH data
            qth_elem = root.find('qth')
            if qth_elem is not None:
                contest_data['qth'] = {
                    'dxcc_country': qth_elem.findtext('dxcccountry', ''),
                    'cq_zone': qth_elem.findtext('cqzone', ''),
                    'iaru_zone': qth_elem.findtext('iaruzone', ''),
                    'arrl_section': qth_elem.findtext('arrlsection', ''),
                    'state_province': qth_elem.findtext('stprvoth', ''),
                    'grid6': qth_elem.findtext('grid6', '')
                }
            
            # Extract breakdown totals
            breakdown = root.find('breakdown')
            if breakdown is not None:
                contest_data['qsos'] = int(breakdown.findtext('qso[@band="total"][@mode="ALL"]', 0))
                contest_data['points'] = int(breakdown.findtext('point[@band="total"][@mode="ALL"]', 0))
                contest_data['multipliers'] = int(breakdown.findtext('mult[@band="total"][@mode="ALL"]', 0))
                
                # Extract per-band breakdown
                bands = ['160', '80', '40', '20', '15', '10']
                contest_data['band_breakdown'] = []
                for band in bands:
                    band_data = {
                        'band': band,
                        'mode': 'ALL',
                        'qsos': int(breakdown.findtext(f'qso[@band="{band}"][@mode="ALL"]', 0)),
                        'points': int(breakdown.findtext(f'point[@band="{band}"][@mode="ALL"]', 0)),
                        'multipliers': int(breakdown.findtext(f'mult[@band="{band}"][@mode="ALL"]', 0))
                    }
                    if band_data['qsos'] > 0:
                        contest_data['band_breakdown'].append(band_data)
            
            results.append(contest_data)
            logging.debug(f"Successfully parsed data for {contest_data['callsign']}")
        except ET.ParseError as e:
            logging.error(f"Error parsing XML: {e}")
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            
    return results

    def store_data(self, contest_data):
    """Store contest data in the database."""
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
                logging.debug(f"Stored main contest data for {data['callsign']}, ID: {contest_score_id}")
                
                # Store QTH data if available
                if 'qth' in data:
                    cursor.execute('''
                        INSERT INTO qth_info (
                            contest_score_id, dxcc_country, cq_zone, iaru_zone,
                            arrl_section, state_province, grid6
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        contest_score_id,
                        data['qth'].get('dxcc_country', ''),
                        data['qth'].get('cq_zone', ''),
                        data['qth'].get('iaru_zone', ''),
                        data['qth'].get('arrl_section', ''),
                        data['qth'].get('state_province', ''),
                        data['qth'].get('grid6', '')
                    ))
                    logging.debug(f"Stored QTH data for {data['callsign']}")
                
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
                    logging.debug(f"Stored band data for {data['callsign']}, band: {band_data['band']}")
            except Exception as e:
                logging.error(f"Error storing data for {data['callsign']}: {e}")

    def cleanup_old_data(self, days=3):
        """Remove data older than specified number of days."""
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # First get IDs of old records
            cursor.execute('SELECT id FROM contest_scores WHERE timestamp < ?', (cutoff_date,))
            old_ids = [row[0] for row in cursor.fetchall()]
            
            # Delete related band breakdown records
            cursor.execute('DELETE FROM band_breakdown WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE timestamp < ?)', 
                         (cutoff_date,))
            
            # Delete old main records
            cursor.execute('DELETE FROM contest_scores WHERE timestamp < ?', (cutoff_date,))
            
            logging.debug(f"Cleaned up {len(old_ids)} old records")
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

                # Process data
                db_handler = ContestDatabaseHandler()
                
                # Parse and store with debug info
                self.debug_print("Starting XML parsing")
                contest_data = db_handler.parse_xml_data(decoded_data)
                self.debug_print("Parsed contest data:", contest_data)

                if not contest_data:
                    self.debug_print("No valid contest data found")
                    self._send_response(400)
                    return

                self.debug_print("Storing data in database")
                db_handler.store_data(contest_data)
                
                # Cleanup with debug info
                self.debug_print("Starting cleanup of old records")
                removed_count = db_handler.cleanup_old_data(days=3)
                self.debug_print(f"Cleanup completed. Removed {removed_count} records")

                # Success response
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
    """Run the HTTP server"""
    server_address = (host, port)
    
    class CustomHandler(ContestRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, debug_mode=debug, **kwargs)
    
    httpd = HTTPServer(server_address, CustomHandler)
    
    logging.info(f"Starting server on {host}:{port} (Debug mode: {'ON' if debug else 'OFF'})")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {str(e)}", exc_info=True)
    finally:
        httpd.server_close()
        logging.info("Server shutdown complete")

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
