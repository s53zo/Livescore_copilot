#!/usr/bin/env python3
import argparse
import sys
import time
import os
import logging
from score_reporter import ScoreReporter

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
        # Check if template file exists
        if not os.path.exists(args.template):
            print(f"Error: Template file not found: {args.template}")
            sys.exit(1)

        # Check if database file exists
        if not os.path.exists(args.db):
            print(f"Error: Database file not found: {args.db}")
            sys.exit(1)

        # Check if output directory is writable
        try:
            os.makedirs(args.output_dir, exist_ok=True)
            test_file = os.path.join(args.output_dir, 'test.tmp')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except OSError as e:
            print(f"Error: Output directory is not writable: {args.output_dir}")
            print(f"Details: {e}")
            sys.exit(1)

        # Print startup information
        print(f"Starting contest score reporter:")
        print(f"  Callsign: {args.callsign}")
        print(f"  Contest: {args.contest}")
        print(f"  Output directory: {args.output_dir}")
        print(f"  Rate calculation interval: {args.rate} minutes")
        print(f"  Report refresh interval: {args.refresh} minutes")
        print(f"  Debug mode: {'ON' if args.debug else 'OFF'}")
        print("\nPress Ctrl+C to stop the reporter")

        # Run the reporter
        run_reporter(args)

    except KeyboardInterrupt:
        print("\nReporter stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
    
