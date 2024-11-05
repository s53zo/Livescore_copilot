#!/usr/bin/env python3
import os
import time
import hmac
import hashlib
import socket
import signal
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
import secrets
import argparse
from typing import Optional, Dict, Any, Tuple
from functools import wraps
from collections import defaultdict
from threading import Lock

class SecurityConfig:
    """Security configuration with environment variable support"""
    API_KEYS_FILE = os.getenv('LIVESCORE_API_KEYS_FILE', '/opt/livescore/api_keys.json')
    MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10MB
    LOG_FILE = os.getenv('LIVESCORE_LOG_FILE', 'livescore.log')
    DB_PATH = os.getenv('LIVESCORE_DB_PATH', 'contest_data.db')
    ALLOWED_METHODS = ['GET', 'POST']
    MAX_HEADER_SIZE = 8192
    RATE_LIMIT_WINDOWS = {
        'per_minute': 60,
        'per_hour': 3600,
        'per_day': 86400
    }
    RATE_LIMIT_COUNTS = {
        'per_minute': 30,
        'per_hour': 1000,
        'per_day': 10000
    }

class RateLimiter:
    """Thread-safe in-memory rate limiting implementation"""
    def __init__(self):
        self.rate_limits = SecurityConfig.RATE_LIMIT_WINDOWS
        self.max_requests = SecurityConfig.RATE_LIMIT_COUNTS
        self.requests = defaultdict(list)
        self.lock = Lock()

    def _cleanup_old_requests(self, api_key: str):
        """Remove expired requests from tracking"""
        current_time = time.time()
        with self.lock:
            self.requests[api_key] = [
                req_time for req_time in self.requests[api_key]
                if current_time - req_time < max(self.rate_limits.values())
            ]

    def is_rate_limited(self, api_key: str) -> bool:
        """Check if the API key has exceeded rate limits"""
        current_time = time.time()
        self._cleanup_old_requests(api_key)

        with self.lock:
            self.requests[api_key].append(current_time)
            requests = self.requests[api_key]

            # Check each time window
            for window_name, window_seconds in self.rate_limits.items():
                window_requests = len([
                    req_time for req_time in requests
                    if current_time - req_time < window_seconds
                ])
                if window_requests > self.max_requests[window_name]:
                    return True

        return False

class APIKeyAuth:
    """API key authentication handler"""
    def __init__(self, api_keys_file: str):
        self.api_keys_file = api_keys_file
        self.api_keys = self._load_api_keys()
        self.last_modified = os.path.getmtime(api_keys_file) if os.path.exists(api_keys_file) else 0
        self.lock = Lock()

    def _load_api_keys(self) -> Dict[str, str]:
        """Load API keys from file with error handling"""
        try:
            if not os.path.exists(self.api_keys_file):
                logging.error(f"API keys file not found: {self.api_keys_file}")
                return {}

            # Check file permissions
            stat = os.stat(self.api_keys_file)
            if stat.st_mode & 0o077:
                logging.error(f"Insecure permissions on API keys file: {self.api_keys_file}")
                return {}

            with open(self.api_keys_file, 'r') as f:
                keys = json.load(f)
                return {k: v for k, v in keys.items() if k and v}
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Error loading API keys: {e}")
            return {}

    def _check_and_reload_keys(self):
        """Thread-safe check and reload of API keys if file modified"""
        try:
            current_mtime = os.path.getmtime(self.api_keys_file)
            with self.lock:
                if current_mtime > self.last_modified:
                    self.api_keys = self._load_api_keys()
                    self.last_modified = current_mtime
        except OSError as e:
            logging.error(f"Error checking API keys file: {e}")

    def verify_api_key(self, api_key: str, timestamp: str, signature: str) -> bool:
        """Verify API key, timestamp, and signature"""
        self._check_and_reload_keys()

        if not api_key or api_key not in self.api_keys:
            return False

        try:
            ts = int(timestamp)
            if abs(time.time() - ts) > 300:  # 5 minute window
                return False

            secret = self.api_keys[api_key]
            message = f"{api_key}{timestamp}"
            expected_signature = hmac.new(
                secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()

            return hmac.compare_digest(signature, expected_signature)
        except (ValueError, TypeError) as e:
            logging.warning(f"Authentication error: {e}")
            return False

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
                # Validate and parse XML
                root = ET.fromstring(self._sanitize_xml(xml_doc))
                
                # Extract basic contest data with validation
                contest_data = {
                    'contest': self._validate_field(root.findtext('contest', ''), 50),
                    'callsign': self._validate_field(root.findtext('call', ''), 12),
                    'timestamp': self._validate_timestamp(root.findtext('timestamp', '')),
                    'club': self._validate_field(root.findtext('club', '').strip(), 100),
                    'section': self._validate_field(
                        root.find('.//qth/arrlsection').text if root.find('.//qth/arrlsection') is not None else '',
                        20
                    ),
                    'score': self._validate_integer(root.findtext('score', 0))
                }
                
                # Extract class attributes
                class_elem = root.find('class')
                if class_elem is not None:
                    contest_data.update({
                        'power': self._validate_field(class_elem.get('power', ''), 20),
                        'assisted': self._validate_field(class_elem.get('assisted', ''), 10),
                        'transmitter': self._validate_field(class_elem.get('transmitter', ''), 10),
                        'ops': self._validate_field(class_elem.get('ops', ''), 100),
                        'bands': self._validate_field(class_elem.get('bands', ''), 50),
                        'mode': self._validate_field(class_elem.get('mode', ''), 10),
                        'overlay': self._validate_field(class_elem.get('overlay', ''), 20)
                    })
                
                # Extract QTH data
                qth_elem = root.find('qth')
                if qth_elem is not None:
                    contest_data['qth'] = {
                        'dxcc_country': self._validate_field(qth_elem.findtext('dxcccountry', ''), 50),
                        'cq_zone': self._validate_field(qth_elem.findtext('cqzone', ''), 10),
                        'iaru_zone': self._validate_field(qth_elem.findtext('iaruzone', ''), 10),
                        'arrl_section': self._validate_field(qth_elem.findtext('arrlsection', ''), 20),
                        'state_province': self._validate_field(qth_elem.findtext('stprvoth', ''), 20),
                        'grid6': self._validate_field(qth_elem.findtext('grid6', ''), 6)
                    }
                
                # Extract breakdown totals with validation
                breakdown = root.find('breakdown')
                if breakdown is not None:
                    contest_data['qsos'] = self._validate_integer(
                        breakdown.findtext('qso[@band="total"][@mode="ALL"]', 0)
                    )
                    contest_data['points'] = self._validate_integer(
                        breakdown.findtext('point[@band="total"][@mode="ALL"]', 0)
                    )
                    contest_data['multipliers'] = self._validate_integer(
                        breakdown.findtext('mult[@band="total"][@mode="ALL"]', 0)
                    )
                    
                    # Extract per-band breakdown
                    bands = ['160', '80', '40', '20', '15', '10']
                    contest_data['band_breakdown'] = []
                    for band in bands:
                        band_data = {
                            'band': band,
                            'mode': 'ALL',
                            'qsos': self._validate_integer(
                                breakdown.findtext(f'qso[@band="{band}"][@mode="ALL"]', 0)
                            ),
                            'points': self._validate_integer(
                                breakdown.findtext(f'point[@band="{band}"][@mode="ALL"]', 0)
                            ),
                            'multipliers': self._validate_integer(
                                breakdown.findtext(f'mult[@band="{band}"][@mode="ALL"]', 0)
                            )
                        }
                        if band_data['qsos'] > 0:
                            contest_data['band_breakdown'].append(band_data)
                
                results.append(contest_data)
                logging.debug(f"Successfully parsed data for {contest_data['callsign']}")
            except ET.ParseError as e:
                logging.error(f"XML parsing error: {e}")
            except Exception as e:
                logging.error(f"Data processing error: {e}")
                
        return results

    def _sanitize_xml(self, xml_string: str) -> str:
        """Sanitize XML input to prevent XXE attacks"""
        # Remove DOCTYPE declarations
        xml_string = re.sub(r'<!DOCTYPE[^>]*>', '', xml_string)
        # Remove entity declarations
        xml_string = re.sub(r'<!ENTITY[^>]*>', '', xml_string)
        return xml_string

    def _validate_field(self, value: str, max_length: int) -> str:
        """Validate and sanitize text fields"""
        if not value:
            return ''
        # Remove any control characters
        value = ''.join(char for char in str(value) if ord(char) >= 32)
        # Trim to max length
        return value[:max_length]

    def _validate_integer(self, value: Any) -> int:
        """Validate and convert integer fields"""
        try:
            return max(0, int(value))
        except (ValueError, TypeError):
            return 0

    def _validate_timestamp(self, timestamp: str) -> str:
        """Validate and format timestamp"""
        try:
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def store_data(self, contest_data):
        """Store contest data in the database with proper error handling."""
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
                    
                    # Store band breakdown data
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
                except sqlite3.Error as e:
                    logging.error(f"Database error storing data for {data['callsign']}: {e}")
                except Exception as e:
                    logging.error(f"Error storing data for {data['callsign']}: {e}")

    def cleanup_old_data(self, days=3):
        """Remove data older than specified number of days with proper error handling."""
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            try:
                # First get IDs of old records
                cursor.execute('SELECT id FROM contest_scores WHERE timestamp < ?', (cutoff_date,))
                old_ids = [row[0] for row in cursor.fetchall()]
                
                if not old_ids:
                    return 0

                # Delete related band breakdown records
                cursor.execute('''
                    DELETE FROM band_breakdown 
                    WHERE contest_score_id IN (
                        SELECT id FROM contest_scores WHERE timestamp < ?
                    )''', (cutoff_date,))
                
                # Delete related QTH info records
                cursor.execute('''
                    DELETE FROM qth_info 
                    WHERE contest_score_id IN (
                        SELECT id FROM contest_scores WHERE timestamp < ?
                    )''', (cutoff_date,))
                
                # Delete old main records
                cursor.execute('DELETE FROM contest_scores WHERE timestamp < ?', 
                             (cutoff_date,))
                
                logging.debug(f"Cleaned up {len(old_ids)} old records")
                return len(old_ids)
                
            except sqlite3.Error as e:
                logging.error(f"Database error during cleanup: {e}")
                return 0

class SecureContestRequestHandler(BaseHTTPRequestHandler):
    """Secure request handler with proper authentication and validation"""
    
    # Standard HTTP response messages
    HTTP_RESPONSES = {
        200: "OK",
        400: "Bad Request - Invalid format",
        401: "Unauthorized - Invalid credentials",
        403: "Forbidden - Access denied",
        404: "Not Found - Invalid endpoint",
        429: "Too Many Requests - Rate limit exceeded",
        500: "Internal Server Error - Server processing failed"
    }

    def __init__(self, *args, debug_mode=False, **kwargs):
        self.debug_mode = debug_mode
        self.rate_limiter = RateLimiter()
        self.api_auth = APIKeyAuth(SecurityConfig.API_KEYS_FILE)
        super().__init__(*args, **kwargs)

    def log_message(self, format, *args):
        """Override to use application logging"""
        logging.info(f"{self.address_string()} - {format%args}")

    def log_error(self, format, *args):
        """Override to use application error logging"""
        logging.error(f"{self.address_string()} - {format%args}")

    def send_error_response(self, status_code: int, message: Optional[str] = None):
        """Send standardized error response"""
        response_message = message or self.HTTP_RESPONSES.get(status_code, "Unknown Error")
        
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        response = json.dumps({
            'error': response_message,
            'status': status_code
        })
        self.wfile.write(response.encode('utf-8'))

    def validate_request_size(self) -> bool:
        """Validate request size against configured maximum"""
        content_length = int(self.headers.get('Content-Length', 0))
        return content_length <= SecurityConfig.MAX_REQUEST_SIZE

    def validate_content(self, content: str) -> bool:
        """Validate XML content format and structure"""
        if not content:
            return False
        
        try:
            xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', content, re.DOTALL)
            if not xml_docs:
                return False
            
            for xml_doc in xml_docs:
                # Basic XML validation
                if not self._validate_xml_structure(xml_doc):
                    return False
            return True
        except Exception:
            return False

    def _validate_xml_structure(self, xml_content: str) -> bool:
        """Validate XML structure and required elements"""
        try:
            # Remove potential XXE vectors
            xml_content = re.sub(r'<!DOCTYPE[^>]*>', '', xml_content)
            xml_content = re.sub(r'<!ENTITY[^>]*>', '', xml_content)
            
            root = ET.fromstring(xml_content)
            
            # Check required elements
            required_elements = ['contest', 'call', 'timestamp']
            for element in required_elements:
                if root.find(element) is None:
                    return False
            
            return True
        except ET.ParseError:
            return False
        except Exception:
            return False

    def authenticate_request(self) -> bool:
        """Authenticate request using API key and signature"""
        api_key = self.headers.get('X-API-Key')
        timestamp = self.headers.get('X-Timestamp')
        signature = self.headers.get('X-Signature')

        if not all([api_key, timestamp, signature]):
            return False

        # Verify timestamp is recent
        try:
            ts = int(timestamp)
            if abs(time.time() - ts) > 300:  # 5 minute window
                return False
        except ValueError:
            return False

        return self.api_auth.verify_api_key(api_key, timestamp, signature)

    def do_POST(self):
        """Handle POST requests securely"""
        try:
            # 1. Validate path
            if self.path != '/livescore':
                self.send_error_response(404)
                return

            # 2. Authenticate request
            if not self.authenticate_request():
                self.send_error_response(401)
                return

            # 3. Check rate limits
            api_key = self.headers.get('X-API-Key')
            if self.rate_limiter.is_rate_limited(api_key):
                self.send_error_response(429)
                return

            # 4. Validate request size
            if not self.validate_request_size():
                self.send_error_response(413, "Request entity too large")
                return

            # 5. Read and validate content
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length).decode('utf-8')
                decoded_data = urllib.parse.unquote_plus(post_data)
            except (ValueError, UnicodeDecodeError) as e:
                logging.error(f"Error reading request data: {e}")
                self.send_error_response(400, "Invalid request data")
                return

            if not self.validate_content(decoded_data):
                self.send_error_response(400, "Invalid XML content")
                return

            # 6. Process the request
            try:
                db_handler = ContestDatabaseHandler(SecurityConfig.DB_PATH)
                contest_data = db_handler.parse_xml_data(decoded_data)
                
                if not contest_data:
                    self.send_error_response(400, "No valid contest data found")
                    return

                db_handler.store_data(contest_data)
                db_handler.cleanup_old_data()

                # 7. Send success response
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                response = json.dumps({
                    'status': 'success',
                    'message': 'Data processed successfully',
                    'records_processed': len(contest_data)
                })
                self.wfile.write(response.encode('utf-8'))

            except Exception as e:
                logging.error(f"Error processing data: {str(e)}")
                self.send_error_response(500, "Error processing data")
                return

        except Exception as e:
            logging.error(f"Error handling POST request: {str(e)}")
            logging.debug(traceback.format_exc())
            self.send_error_response(500, "Internal server error")

    def do_GET(self):
        """Handle GET requests (health check endpoint)"""
        if self.path == '/health':
            try:
                response_data = self._perform_health_check()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode('utf-8'))
            except Exception as e:
                logging.error(f"Health check error: {str(e)}")
                self.send_error_response(500, "Health check failed")
        else:
            self.send_error_response(404, "Not found")

    def _perform_health_check(self) -> Dict[str, Any]:
        """Perform system health check"""
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }

        # Check database
        try:
            with sqlite3.connect(SecurityConfig.DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM contest_scores')
                health_data['components']['database'] = {
                    'status': 'healthy',
                    'records': cursor.fetchone()[0]
                }
        except Exception as e:
            health_data['components']['database'] = {
                'status': 'unhealthy',
                'error': str(e)
            }
            health_data['status'] = 'degraded'

        return health_data

def setup_secure_logging(debug_mode):
    """Setup secure logging configuration"""
    log_level = logging.DEBUG if debug_mode else logging.INFO
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(SecurityConfig.LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o750)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    # Secure file permissions for log file
    file_handler = logging.FileHandler(SecurityConfig.LOG_FILE)
    file_handler.setFormatter(formatter)
    if os.path.exists(SecurityConfig.LOG_FILE):
        os.chmod(SecurityConfig.LOG_FILE, 0o640)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger

def create_api_key():
    """Generate a new API key and secret"""
    api_key = secrets.token_hex(16)
    api_secret = secrets.token_hex(32)
    return api_key, api_secret

def save_api_key(api_key, api_secret, filename=SecurityConfig.API_KEYS_FILE):
    """Safely save API key to file"""
    try:
        # Read existing keys
        keys = {}
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                keys = json.load(f)
        
        # Add new key
        keys[api_key] = api_secret
        
        # Write to temporary file first
        temp_file = f"{filename}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(keys, f, indent=2)
        
        # Set secure permissions
        os.chmod(temp_file, 0o600)
        
        # Atomically replace the original file
        os.replace(temp_file, filename)
        
        return True
    except Exception as e:
        logging.error(f"Error saving API key: {e}")
        return False

def run_secure_server(host='127.0.0.1', port=8088, debug=False):
    """Run the HTTP server with security configurations"""
    server_address = (host, port)
    
    class CustomSecureHandler(SecureContestRequestHandler):
        def __init__(self, *args, **kwargs):
            self.debug_mode = debug
            BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    class SecureHTTPServer(HTTPServer):
        def __init__(self, server_address, handler_class):
            HTTPServer.__init__(self, server_address, handler_class)
            # Set socket options
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.timeout = 60
            self.request_queue_size = 50

        def handle_error(self, request, client_address):
            """Override to handle errors securely"""
            logging.error(f"Error processing request from {client_address[0]}", 
                        exc_info=True)

    try:
        # Create secure server instance
        httpd = SecureHTTPServer(server_address, CustomSecureHandler)
        
        # Log startup information
        logging.info(f"Starting secure server on {host}:{port}")
        logging.info(f"Debug mode: {'ON' if debug else 'OFF'}")
        
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logging.info(f"Received signal {signum}")
            raise KeyboardInterrupt()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Main server loop with proper error handling
        while True:
            try:
                httpd.handle_request()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error(f"Error handling request: {str(e)}", exc_info=True)
                continue
                
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {str(e)}", exc_info=True)
        raise
    finally:
        try:
            logging.info("Shutting down server...")
            httpd.server_close()
            logging.info("Server shutdown complete")
        except Exception as e:
            logging.error(f"Error during shutdown: {str(e)}", exc_info=True)

def verify_security_configuration():
    """Verify security-related configurations and permissions"""
    try:
        # Check log directory permissions
        log_dir = os.path.dirname(SecurityConfig.LOG_FILE)
        if log_dir and os.path.exists(log_dir):
            log_dir_stat = os.stat(log_dir)
            if log_dir_stat.st_mode & 0o077:
                logging.warning(f"Insecure log directory permissions: {log_dir}")

        # Check database file permissions
        if os.path.exists(SecurityConfig.DB_PATH):
            db_stat = os.stat(SecurityConfig.DB_PATH)
            if db_stat.st_mode & 0o077:
                logging.warning(f"Insecure database file permissions: {SecurityConfig.DB_PATH}")

        # Check API keys file
        if not os.path.exists(SecurityConfig.API_KEYS_FILE):
            logging.error(f"API keys file not found: {SecurityConfig.API_KEYS_FILE}")
            return False

        api_keys_stat = os.stat(SecurityConfig.API_KEYS_FILE)
        if api_keys_stat.st_mode & 0o077:
            logging.error(f"Insecure API keys file permissions: {SecurityConfig.API_KEYS_FILE}")
            return False

        return True

    except Exception as e:
        logging.error(f"Error verifying security configuration: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Secure Contest Data Server')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='Enable debug mode')
    parser.add_argument('--host', default='127.0.0.1',
                      help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8088,
                      help='Port to bind to (default: 8088)')
    parser.add_argument('--generate-key', action='store_true',
                      help='Generate a new API key')
    
    args = parser.parse_args()
    
    # Setup secure logging
    logger = setup_secure_logging(args.debug)
    
    if args.generate_key:
        api_key, api_secret = create_api_key()
        if save_api_key(api_key, api_secret):
            print("\nNew API key generated:")
            print(f"API Key: {api_key}")
            print(f"API Secret: {api_secret}")
            print("\nStore these credentials securely!")
            sys.exit(0)
        else:
            print("Error generating API key")
            sys.exit(1)
    
    # Verify security configuration
    if not verify_security_configuration():
        logger.error("Security configuration verification failed")
        sys.exit(1)
    
    # Log startup information
    logger.info("Server starting with configuration:")
    logger.info(f"Host: {args.host}")
    logger.info(f"Port: {args.port}")
    logger.info(f"Debug Mode: {'ON' if args.debug else 'OFF'}")
    logger.info(f"Log File: {SecurityConfig.LOG_FILE}")
    logger.info(f"Database File: {SecurityConfig.DB_PATH}")
    
    try:
        run_secure_server(args.host, args.port, args.debug)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
