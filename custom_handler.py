#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler
import urllib.parse
import logging
import json
import xml.etree.ElementTree as ET
import re
import traceback
import time

class CustomHandler(BaseHTTPRequestHandler):
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
        self.logger = logging.getLogger('CustomHandler')
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
            self.logger.debug(debug_info)

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
            xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', xml_data, re.DOTALL)
            if not xml_docs:
                return False
            ET.fromstring(xml_docs[0])
            return True
        except (ET.ParseError, Exception) as e:
            self.debug_print(f"XML validation error: {str(e)}")
            return False

    def do_POST(self):
        """Handle POST requests to /livescore"""
        try:
            self.log_request_details()

            if self.path != '/livescore':
                self.debug_print("Invalid endpoint requested")
                self._send_response(404)
                return

            content_length = int(self.headers.get('Content-Length', 0))
            self.debug_print(f"Content Length: {content_length}")

            post_data = self.rfile.read(content_length).decode('utf-8')
            #ZO
            self.debug_print("Received POST data:", post_data)

            decoded_data = urllib.parse.unquote_plus(post_data)
            #ZO
            self.debug_print("Decoded POST data:", decoded_data)

            if not self.validate_xml_data(decoded_data):
                self.debug_print("Invalid XML data received")
                self._send_response(400)
                return

            if not self.check_authorization():
                self.debug_print("Unauthorized access attempt")
                self._send_response(403)
                return

            db_handler = self.server.db_handler
            db_handler.process_submission(decoded_data)
            
            self._send_response(200)

        except Exception as e:
            error_details = {
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.debug_print("Error occurred:", error_details)
            self._send_response(500)

    def do_GET(self):
        """Handle GET requests"""
        self.log_request_details()
        
        if self.path == '/health':
            self.debug_print("Health check requested")
            self._send_response(200)
        elif self.path.startswith('/livescore-pilot/events'):
            self.handle_sse()
        else:
            self.debug_print(f"Invalid path requested: {self.path}")
            self._send_response(404)

    def handle_sse(self):
        """Handle Server-Sent Events connection"""
        try:
            # Parse query parameters
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            
            contest = params.get('contest', [''])[0]
            callsign = params.get('callsign', [''])[0]
            filter_type = params.get('filter_type', [''])[0]
            filter_value = params.get('filter_value', [''])[0]

            if not contest or not callsign:
                self.debug_print("Missing required parameters")
                self._send_response(400)
                return

            # Set SSE headers
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.end_headers()

            # Get initial data
            db_handler = self.server.db_handler
            last_data = db_handler.get_scores(contest, callsign, filter_type, filter_value)
            last_data_json = json.dumps(last_data)  # Cache the JSON string

            # Send initial data
            self.wfile.write(f"event: init\ndata: {last_data_json}\n\n".encode('utf-8'))
            self.wfile.flush()

            self.debug_print(f"SSE connection established for {callsign} in {contest}")

            while True:
                try:
                    # Check for new data first
                    new_data = db_handler.get_scores(contest, callsign, filter_type, filter_value)
                    new_data_json = json.dumps(new_data)

                    # Only send update if data has actually changed
                    if new_data_json != last_data_json:
                        self.debug_print("Data changed, sending update")
                        self.wfile.write(f"event: update\ndata: {new_data_json}\n\n".encode('utf-8'))
                        self.wfile.flush()
                        last_data_json = new_data_json

                    # Send keep-alive after data check
                    self.wfile.write(b":keep-alive\n\n")
                    self.wfile.flush()
                    
                    self.debug_print("Keep-alive and data check completed")
                    
                    # Sleep for 30 seconds between updates
                    time.sleep(30)

                except (BrokenPipeError, ConnectionResetError) as e:
                    self.debug_print(f"Client disconnected: {str(e)}")
                    break
                except Exception as e:
                    self.debug_print(f"Error in SSE loop: {str(e)}")
                    self.debug_print(traceback.format_exc())
                    break

        except Exception as e:
            self.debug_print(f"SSE setup error: {str(e)}")
            self.debug_print(traceback.format_exc())
            self._send_response(500)
        finally:
            self.debug_print(f"SSE connection closed for {callsign} in {contest}")

    def check_authorization(self):
        """Check if the request is authorized"""
        # Example: Check for an API key in headers
        api_key = self.headers.get('X-API-Key')
        # Implement your actual authorization logic here
        return True
