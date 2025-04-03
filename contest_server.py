#!/usr/bin/env python3
import logging
import urllib.parse
from http.server import HTTPServer
from custom_handler import CustomHandler
from database_handler import ContestDatabaseHandler

class ContestServer:
    # Modified __init__ to accept socketio
    def __init__(self, host='127.0.0.1', port=8088, db_path='contest_data.db', debug=False, socketio=None):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.debug = debug
        self.socketio = socketio # Store socketio instance
        self.logger = self._setup_logging(debug)
        # Pass socketio instance to ContestDatabaseHandler
        self.db_handler = ContestDatabaseHandler(db_path, socketio=self.socketio)

    def _setup_logging(self, debug):
        logger = logging.getLogger('ContestServer')
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
                
    def start(self):
        """Start the server"""
        try:
            server_address = (self.host, self.port)
            # Pass debug mode to CustomServer if needed, but db_handler is set below
            httpd = CustomServer(server_address,
                               lambda *args, **kwargs: CustomHandler(*args,
                                                                  debug_mode=self.debug,
                                                                  **kwargs),
                               debug=self.debug) # Pass debug to CustomServer

            # Set the db_handler (which includes socketio) on the server instance
            # This makes it accessible to the CustomHandler via self.server.db_handler
            httpd.db_handler = self.db_handler

            self.logger.info(f"Starting HTTP server on {self.host}:{self.port} (Note: This doesn't start SocketIO itself)")
            # httpd.serve_forever() # This will block. SocketIO server should handle serving.
            # We assume the main application runner (e.g., using socketio.run or gunicorn)
            # will handle the combined Flask+SocketIO+HTTP serving.
            # This ContestServer might become redundant or need refactoring if
            # Flask+SocketIO handles the HTTP serving directly.
            # For now, let's assume it's still needed for the XML endpoint,
            # but it shouldn't block here. Perhaps run in a thread?
            # Or more likely, the CustomHandler logic should be integrated
            # into the Flask app itself.

            # --- Potential Refactoring Needed ---
            # If Flask/SocketIO handles HTTP, this separate HTTPServer might not be needed.
            # The XML POST logic in CustomHandler could become a Flask route.
            # Let's comment out serve_forever for now, assuming the main app runner handles it.
            self.logger.warning("ContestServer.start() called, but serve_forever() is commented out assuming Flask/SocketIO handles serving.")
            # If this server IS still needed independently, it should run in a thread:
            # import threading
            # self.server_thread = threading.Thread(target=httpd.serve_forever)
            # self.server_thread.daemon = True
            # self.server_thread.start()

        except Exception as e:
            self.logger.error(f"Error starting server: {e}")
            raise
        finally:
            self.cleanup()
            
    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'db_handler'):
                self.db_handler.cleanup()
                self.logger.info("Database handler cleaned up")
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        # Add cleanup for server thread if used
        # if hasattr(self, 'server_thread') and self.server_thread.is_alive():
        #     # Need to properly shut down httpd if running in a thread
        #     pass


class CustomServer(HTTPServer):
    # Removed redundant db_handler creation and db_path parameter
    def __init__(self, server_address, RequestHandlerClass, debug=False, **kwargs):
        self.debug = debug
        # db_handler will be set externally after initialization
        self.db_handler = None
        super().__init__(server_address, RequestHandlerClass, **kwargs)

    # server_close might not be needed if db_handler cleanup is handled elsewhere
    # def server_close(self):
    #     if hasattr(self, 'db_handler'):
    #         self.db_handler.cleanup() # Ensure cleanup is idempotent if called elsewhere too
    #     super().server_close()
