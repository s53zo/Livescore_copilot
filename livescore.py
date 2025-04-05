#!/usr/bin/env python3
import argparse
import logging
import sys
from contest_server import ContestServer
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from maintenance_task import perform_maintenance
import os

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

def run_maintenance(db_path, logger):
    """Run maintenance tasks with logging"""
    try:
        logger.info("Starting scheduled maintenance tasks...")
        
        # Create maintenance directories if they don't exist
        backup_dir = "./backups"
        reports_dir = "./reports"
        archive_dir = "./archive"
        for directory in [backup_dir, reports_dir, archive_dir]:
            os.makedirs(directory, exist_ok=True)

        # Run maintenance with dry_run=False
        perform_maintenance(db_path, dry_run=False)
        logger.info("Scheduled maintenance completed successfully")
        
    except Exception as e:
        logger.error(f"Error during scheduled maintenance: {e}")
        logger.exception("Maintenance task error details:")

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
    parser.add_argument('--maintenance-hour', type=int, default=2,
                      help='Hour to run maintenance (24-hour format, default: 2)')
    parser.add_argument('--maintenance-minute', type=int, default=0,
                      help='Minute to run maintenance (default: 0)')
    # Add MQTT arguments
    parser.add_argument('--mqtt-host', help='MQTT broker hostname')
    parser.add_argument('--mqtt-port', type=int, default=1883, help='MQTT broker port (default: 1883)')
    parser.add_argument('--mqtt-username', help='MQTT username')
    parser.add_argument('--mqtt-password', help='MQTT password')
    # Add client-id? Maybe later if needed. TLS? Not requested for now.
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    logger = setup_logging(args.debug, args.log_file)
    
    # Log startup information
    logger.info("Server starting up with configuration:") # Use logger instead of logging
    logger.info(f"Host: {args.host}")
    logger.info(f"Port: {args.port}")
    logger.info(f"Debug Mode: {'ON' if args.debug else 'OFF'}")
    logger.info(f"Log File: {args.log_file}")
    logger.info(f"Database File: {args.db_file}")
    logger.info(f"Maintenance Time: {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")
    # Log MQTT config if provided
    if args.mqtt_host:
        logger.info(f"MQTT Host: {args.mqtt_host}")
        logger.info(f"MQTT Port: {args.mqtt_port}")
        logger.info(f"MQTT Username: {args.mqtt_username if args.mqtt_username else 'None'}")
    else:
        logger.info("MQTT Publishing: Disabled (no --mqtt-host provided)")
    
    # Initialize scheduler
    scheduler = BackgroundScheduler()
    
    # Add maintenance job
    trigger = CronTrigger(
        hour=args.maintenance_hour,
        minute=args.maintenance_minute
    )
    
    scheduler.add_job(
        run_maintenance,
        trigger=trigger,
        args=[args.db_file, logger],
        id='maintenance_job',
        name='Database Maintenance',
        misfire_grace_time=3600  # Allow job to run up to 1 hour late
    )
    
    # Start the scheduler
    scheduler.start()
    logger.info(f"Scheduled maintenance job for {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")
    
    # Prepare MQTT config dictionary
    mqtt_config = None
    if args.mqtt_host:
        mqtt_config = {
            'host': args.mqtt_host,
            'port': args.mqtt_port,
            'username': args.mqtt_username,
            'password': args.mqtt_password,
            # 'client_id': None, # Can add later if needed
            # 'use_tls': False # Can add later if needed
        }

    try:
        # Create and start server, passing MQTT config
        server = ContestServer(
            host=args.host,
            port=args.port,
            db_path=args.db_file,
            debug=args.debug,
            mqtt_config=mqtt_config # Pass the config dict
        )
        server.start()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        logger.exception("Server error details:")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        if 'server' in locals():
            server.cleanup()
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    main()
