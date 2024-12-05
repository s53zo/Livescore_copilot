#!/usr/bin/env python3
import argparse
import logging
import sys
from contest_server import ContestServer

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

def main():
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
    
    # Create and start server
    server = ContestServer(args.host, args.port, args.db_file, args.debug)
    server.start()

if __name__ == "__main__":
    main()
