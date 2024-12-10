#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
import sqlite3
import logging
import sys
import traceback

class RateCalculator:
    def __init__(self, db_path, debug=False):
        self.db_path = db_path
        self.debug = debug
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('RateCalculator')
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            #self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)
            self.logger.setLevel(logging.ERROR) 

    def calculate_total_rate(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate total QSO rate using current UTC time as reference with old records discarded."""
        try:
            current_utc = datetime.utcnow()
            lookback_time = current_utc - timedelta(minutes=lookback_minutes)
    
            query = """
                WITH current_score AS (
                    SELECT 
                        cs.qsos AS current_qsos,
                        cs.timestamp AS current_ts
                    FROM contest_scores cs
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp <= ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                ),
                previous_score AS (
                    SELECT
                        cs.qsos AS prev_qsos,
                        cs.timestamp AS prev_ts
                    FROM contest_scores cs
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp < ?
                    -- Find the closest record before the lookback time
                    ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(?))
                    LIMIT 1
                )
                SELECT 
                    current_qsos,
                    prev_qsos,
                    current_ts,
                    prev_ts
                FROM current_score, previous_score
            """
    
            params = (
                callsign, contest, current_utc.strftime('%Y-%m-%d %H:%M:%S'),
                callsign, contest, lookback_time.strftime('%Y-%m-%d %H:%M:%S'),
                lookback_time.strftime('%Y-%m-%d %H:%M:%S')
            )
    
            cursor.execute(query, params)
            result = cursor.fetchone()
            if not result or None in result:
                # No valid data
                return 0
    
            current_qsos, prev_qsos, current_ts_str, prev_ts_str = result
    
            current_dt = datetime.strptime(current_ts_str, '%Y-%m-%d %H:%M:%S')
            prev_dt = datetime.strptime(prev_ts_str, '%Y-%m-%d %H:%M:%S')
    
            # Calculate actual time difference in minutes
            time_diff = (current_dt - prev_dt).total_seconds() / 60.0
    
            # If the previous record is older than the intended lookback window, discard it
            if time_diff > lookback_minutes:
                return 0
    
            qso_diff = current_qsos - prev_qsos
            if qso_diff <= 0:
                # No increase in QSOs
                return 0
    
            # Calculate the rate (QSOs per hour)
            rate = int(round((qso_diff * 60) / time_diff))
            return rate
    
        except Exception as e:
            self.logger.error(f"Error calculating total rate: {e}")
            return 0


def analyze_rates(args):
    """Analyze rates for given callsign and contest"""
    calculator = RateCalculator(args.db, args.debug)
    
    try:
        with sqlite3.connect(args.db) as conn:
            cursor = conn.cursor()
            
            # Calculate and display total rate
            total_rate = calculator.calculate_total_rate(cursor, args.call, args.contest, args.minutes)
            print(f"\nTotal QSO Rate: {total_rate}/hr")
            
            # Calculate and display band rates
            band_rates = calculator.calculate_band_rates(cursor, args.call, args.contest, args.minutes)
            if band_rates:
                print("\nPer-band QSO Rates:")
                for band in sorted(band_rates.keys()):
                    print(f"  {band}m: {band_rates[band]}/hr")
            else:
                print("\nNo band-specific data available")
                
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.debug:
            print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Calculate contest QSO rates')
    parser.add_argument('--db', default='contest_data.db',
                       help='Database file path (default: contest_data.db)')
    parser.add_argument('--call', required=True,
                       help='Callsign to analyze')
    parser.add_argument('--contest', required=True,
                       help='Contest name')
    parser.add_argument('--minutes', type=int, default=60,
                       help='Minutes to look back for rate calculation (default: 60)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug output')
    
    args = parser.parse_args()
    analyze_rates(args)

if __name__ == "__main__":
    main()
