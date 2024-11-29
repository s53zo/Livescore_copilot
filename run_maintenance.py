#!/usr/bin/env python3
from maintenance_tasks import DatabaseMaintenance
import argparse
import time
import signal
import sys

def main():
    parser = argparse.ArgumentParser(description='Run database maintenance tasks manually')
    parser.add_argument('--db', default='/opt/livescore/contest_data.db',
                      help='Database file path')
    parser.add_argument('--log', default='/opt/livescore/logs/maintenance.log',
                      help='Log file path')
    parser.add_argument('--run-once', action='store_true',
                      help='Run maintenance once and exit')
    
    args = parser.parse_args()
    
    # Initialize maintenance
    maintenance = DatabaseMaintenance(
        db_path=args.db,
        log_path=args.log
    )
    
    def handle_shutdown(signum, frame):
        print("\nShutting down maintenance...")
        maintenance.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Start maintenance
    maintenance.start()
    
    if args.run_once:
        # Run cleanup and optimization immediately
        print("Running maintenance tasks...")
        maintenance.cleanup_scores(minutes=90)
        maintenance.perform_maintenance()
        print("Maintenance completed!")
        maintenance.stop()
    else:
        print("Maintenance running in continuous mode. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            handle_shutdown(None, None)

if __name__ == "__main__":
    main()
