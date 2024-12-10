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
        """Calculate total QSO rate using current UTC time as reference"""
        try:
            current_utc = datetime.utcnow()
            lookback_time = current_utc - timedelta(minutes=lookback_minutes)
            
            if self.debug:
                self.logger.debug(f"\nCalculating total rate for {callsign} in {contest}")
                self.logger.debug(f"Current UTC: {current_utc}")
                self.logger.debug(f"Looking back to: {lookback_time}")
            
            query = """
                WITH current_score AS (
                    SELECT 
                        cs.qsos as current_qsos,
                        cs.timestamp as current_ts
                    FROM contest_scores cs
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp <= ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                ),
                previous_score AS (
                    SELECT 
                        cs.qsos as prev_qsos,
                        cs.timestamp as prev_ts
                    FROM contest_scores cs
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp <= ?
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
            
            cursor.execute(query, (
                callsign, contest, current_utc.strftime('%Y-%m-%d %H:%M:%S'),
                callsign, contest, lookback_time.strftime('%Y-%m-%d %H:%M:%S'),
                lookback_time.strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            result = cursor.fetchone()
            if not result or None in result:
                if self.debug:
                    self.logger.debug("No data available for rate calculation")
                return 0
            
            current_qsos, prev_qsos, current_ts, prev_ts = result
            
            if self.debug:
                self.logger.debug("\nTotal rate analysis:")
                self.logger.debug(f"  Current QSOs: {current_qsos}")
                self.logger.debug(f"  Previous QSOs: {prev_qsos}")
                self.logger.debug(f"  Current timestamp: {current_ts}")
                self.logger.debug(f"  Previous timestamp: {prev_ts}")
            
            # Convert timestamps to datetime objects
            current_dt = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
            prev_dt = datetime.strptime(prev_ts, '%Y-%m-%d %H:%M:%S')
            
            # Calculate time difference in minutes
            time_diff = (current_dt - prev_dt).total_seconds() / 60
            
            if self.debug:
                self.logger.debug(f"  Time difference: {time_diff:.1f} minutes")
            
            if time_diff <= 0:
                if self.debug:
                    self.logger.debug("Rate calculation skipped - invalid time difference")
                return 0
            
            qso_diff = current_qsos - prev_qsos
            
            if qso_diff == 0:
                if self.debug:
                    self.logger.debug("Rate is 0 - no new QSOs")
                return 0
            
            rate = int(round((qso_diff * 60) / time_diff))
            
            if self.debug:
                self.logger.debug(f"  QSO difference: {qso_diff}")
                self.logger.debug(f"  Calculated rate: {rate}/hr")
                
            return rate
            
        except Exception as e:
            self.logger.error(f"Error calculating total rate: {e}")
            self.logger.debug(traceback.format_exc())
            return 0

    def calculate_band_rates(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate per-band QSO rates"""
        try:
            current_utc = datetime.utcnow()
            lookback_time = current_utc - timedelta(minutes=lookback_minutes)
            
            if self.debug:
                self.logger.debug(f"\nCalculating band rates for {callsign} in {contest}")
                self.logger.debug(f"Current UTC: {current_utc}")
                self.logger.debug(f"Looking back to: {lookback_time}")
            
            query = """
                WITH current_bands AS (
                    SELECT 
                        bb.band,
                        bb.qsos as current_qsos,
                        cs.timestamp as current_ts
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.callsign = ? 
                    AND cs.contest = ?
                    AND cs.timestamp <= ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                ),
                previous_bands AS (
                    SELECT 
                        bb.band,
                        bb.qsos as prev_qsos,
                        cs.timestamp as prev_ts
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp <= ?
                    ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(?))
                    LIMIT 1
                )
                SELECT 
                    cb.band,
                    cb.current_qsos,
                    pb.prev_qsos,
                    cb.current_ts,
                    pb.prev_ts
                FROM current_bands cb
                LEFT JOIN previous_bands pb ON cb.band = pb.band
                WHERE cb.current_qsos > 0
                ORDER BY cb.band
            """
            
            cursor.execute(query, (
                callsign, contest, current_utc.strftime('%Y-%m-%d %H:%M:%S'),
                callsign, contest, lookback_time.strftime('%Y-%m-%d %H:%M:%S'),
                lookback_time.strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            results = cursor.fetchall()
            band_rates = {}
            
            if self.debug:
                self.logger.debug(f"Found {len(results)} bands with activity")
            
            for row in results:
                band, current_qsos, prev_qsos, current_ts, prev_ts = row
                
                if self.debug:
                    self.logger.debug(f"\nBand {band} analysis:")
                    self.logger.debug(f"  Current QSOs: {current_qsos}")
                    self.logger.debug(f"  Previous QSOs: {prev_qsos if prev_qsos else 0}")
                    self.logger.debug(f"  Current timestamp: {current_ts}")
                    self.logger.debug(f"  Previous timestamp: {prev_ts}")
                
                if not prev_ts:
                    if self.debug:
                        self.logger.debug("  Rate calculation skipped - no previous data")
                    band_rates[band] = 0
                    continue
                
                # Convert timestamps to datetime objects
                current_dt = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
                prev_dt = datetime.strptime(prev_ts, '%Y-%m-%d %H:%M:%S')
                
                # Calculate time difference in minutes
                time_diff = (current_dt - prev_dt).total_seconds() / 60
                
                if self.debug:
                    self.logger.debug(f"  Time difference: {time_diff:.1f} minutes")
                
                if time_diff <= 0:
                    if self.debug:
                        self.logger.debug("  Rate calculation skipped - invalid time difference")
                    band_rates[band] = 0
                    continue
                
                # If previous QSOs is NULL, treat as 0
                prev_qsos = prev_qsos or 0
                qso_diff = current_qsos - prev_qsos
                
                if qso_diff == 0:
                    if self.debug:
                        self.logger.debug("  Rate is 0 - no new QSOs")
                    band_rates[band] = 0
                else:
                    rate = int(round((qso_diff * 60) / time_diff))
                    band_rates[band] = rate
                    if self.debug:
                        self.logger.debug(f"  QSO difference: {qso_diff}")
                        self.logger.debug(f"  Calculated rate: {rate}/hr")
            
            return band_rates
            
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            self.logger.debug(traceback.format_exc())
            return {}

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
