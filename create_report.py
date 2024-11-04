#!/usr/bin/env python3
import argparse
import sqlite3
import os
import sys
from datetime import datetime
import logging
import time

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_minutes = rate_minutes
        self._setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}, Rate interval: {rate_minutes} minutes")

    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ScoreReporter')

    def load_template(self):
        """Load HTML template from file"""
        try:
            self.logger.debug(f"Loading template from: {self.template_path}")
            with open(self.template_path, 'r') as f:
                return f.read()
        except IOError as e:
            self.logger.error(f"Error loading template: {e}")
            return None

    # [Previous methods remain unchanged]

def run_reporter(args):
    """Run the reporter continuously"""
    reporter = ScoreReporter(args.db, args.template, args.rate)
    
    if args.debug:
        reporter.logger.setLevel(logging.DEBUG)

    while True:
        try:
            start_time = time.time()
            
            # Get and process data
            stations = reporter.get_station_details(args.callsign, args.contest)
            if stations:
                success = reporter.generate_html(args.callsign, args.contest, stations, args.output_dir)
                if not success:
                    reporter.logger.error("Failed to generate report")
            else:
                reporter.logger.error(f"No data found for {args.callsign} in {args.contest}")

            # Calculate time to next update
            elapsed_time = time.time() - start_time
            sleep_time = max(0, args.refresh * 60 - elapsed_time)
            
            reporter.logger.info(f"Next update in {sleep_time:.1f} seconds")
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            reporter.logger.info("Stopping reporter")
            break
        except Exception as e:
            reporter.logger.error(f"Error in reporter loop: {e}")
            reporter.logger.debug("Error details:", exc_info=True)
            time.sleep(args.refresh * 60)  # Wait before retrying

def main():
    parser = argparse.ArgumentParser(description='Generate contest score report')
    parser.add_argument('--callsign', required=True, help='Callsign to report')
    parser.add_argument('--contest', required=True, help='Contest name')
    parser.add_argument('--output-dir', required=True, help='Output directory for report')
    parser.add_argument('--db', default='contest_data.db', help='Database file path')
    parser.add_argument('--template', default='templates/score_template.html', 
                      help='Path to HTML template file')
    parser.add_argument('--rate', type=int, default=60,
                      help='Rate calculation interval in minutes (default: 60)')
    parser.add_argument('--refresh', type=float, default=2,
                      help='Refresh interval in minutes (default: 2)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    try:
        run_reporter(args)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
