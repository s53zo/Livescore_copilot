#!/usr/bin/env python3
import sqlite3
import argparse
import sys
import logging
from datetime import datetime, timedelta

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
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def calculate_band_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate per-band QSO rates for both time windows using specified timestamp as reference"""
        # Convert window sizes to integers
        long_window = int(long_window)
        short_window = int(short_window)
        
        # Convert current_ts string to datetime if it's a string
        if isinstance(current_ts, str):
            current_ts = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
        
        # Calculate lookback times from the provided timestamp
        long_lookback = current_ts - timedelta(minutes=long_window)
        short_lookback = current_ts - timedelta(minutes=short_window)
        
        if self.debug:
            self.logger.debug(f"\nCalculating band rates for {callsign} in {contest}")
            self.logger.debug(f"Reference time: {current_ts}")
            self.logger.debug(f"Long window lookback to: {long_lookback}")
            self.logger.debug(f"Short window lookback to: {short_lookback}")
        
        query = """
            WITH current_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as current_qsos,
                    bb.multipliers,
                    cs.timestamp as current_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ? 
                AND cs.contest = ?
                AND cs.timestamp <= ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            long_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as long_window_qsos,
                    cs.timestamp as long_window_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= ?
                ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(?))
                LIMIT 1
            ),
            short_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as short_window_qsos,
                    cs.timestamp as short_window_ts
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
                cb.multipliers,
                lwb.long_window_qsos,
                swb.short_window_qsos,
                cb.current_ts,
                lwb.long_window_ts,
                swb.short_window_ts
            FROM current_bands cb
            LEFT JOIN long_window_bands lwb ON cb.band = lwb.band
            LEFT JOIN short_window_bands swb ON cb.band = swb.band
            WHERE cb.current_qsos > 0
            ORDER BY cb.band
        """
        
        cursor.execute(query, (
            callsign, contest, current_ts.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, long_lookback.strftime('%Y-%m-%d %H:%M:%S'), long_lookback.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, short_lookback.strftime('%Y-%m-%d %H:%M:%S'), short_lookback.strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        results = cursor.fetchall()
        band_data = {}
        
        if self.debug:
            self.logger.debug(f"Found {len(results)} bands with activity")
        
        for row in results:
            band, current_qsos, multipliers, long_window_qsos, short_window_qsos, current_ts, long_window_ts, short_window_ts = row
            
            if self.debug:
                self.logger.debug(f"\nBand {band} analysis:")
                self.logger.debug(f"  Current QSOs: {current_qsos}")
                self.logger.debug(f"  Current timestamp: {current_ts}")
                self.logger.debug(f"  60-min window QSOs: {long_window_qsos} at {long_window_ts}")
                self.logger.debug(f"  15-min window QSOs: {short_window_qsos} at {short_window_ts}")
            
            # Calculate long window rate (60-minute)
            long_rate = 0
            if long_window_qsos is not None and long_window_ts:
                time_diff = (datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S') - 
                           datetime.strptime(long_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
                if time_diff > 0:
                    qso_diff = current_qsos - long_window_qsos
                    if qso_diff > 0:
                        long_rate = int(round((qso_diff * 60) / time_diff))
            
            # Calculate short window rate (15-minute)
            short_rate = 0
            if short_window_qsos is not None and short_window_ts:
                time_diff = (datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S') - 
                           datetime.strptime(short_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
                if time_diff > 0:
                    qso_diff = current_qsos - short_window_qsos
                    if qso_diff > 0:
                        short_rate = int(round((qso_diff * 60) / time_diff))
            
            if self.debug:
                self.logger.debug(f"  60-minute rate: {long_rate}/hr")
                self.logger.debug(f"  15-minute rate: {short_rate}/hr")
            
            band_data[band] = [current_qsos, multipliers, long_rate, short_rate]
        
        return band_data
    
    def calculate_total_rate(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate total QSO rate with detailed debugging"""
        query = """
            WITH current_score AS (
                SELECT 
                    cs.qsos as current_qsos,
                    cs.timestamp as current_ts
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
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
                AND cs.timestamp <= datetime('now', ? || ' minutes')
                ORDER BY cs.timestamp DESC
                LIMIT 1
            )
            SELECT 
                current_qsos,
                prev_qsos,
                current_ts,
                prev_ts,
                ROUND((JULIANDAY(current_ts) - JULIANDAY(prev_ts)) * 24 * 60, 1) as minutes_diff
            FROM current_score, previous_score
        """
        
        if self.debug:
            self.logger.debug(f"\nCalculating total rate for {callsign} in {contest}")
            self.logger.debug(f"Looking back {lookback_minutes} minutes")
        
        minutes_param = f"-{lookback_minutes}"
        cursor.execute(query, (callsign, contest, callsign, contest, minutes_param))
        result = cursor.fetchone()
        
        if not result or None in result:
            if self.debug:
                self.logger.debug("No data available for rate calculation")
            return 0
        
        current_qsos, prev_qsos, current_ts, prev_ts, minutes_diff = result
        
        if self.debug:
            self.logger.debug("\nTotal rate analysis:")
            self.logger.debug(f"  Current QSOs: {current_qsos}")
            self.logger.debug(f"  Previous QSOs: {prev_qsos}")
            self.logger.debug(f"  Current timestamp: {current_ts}")
            self.logger.debug(f"  Previous timestamp: {prev_ts}")
            self.logger.debug(f"  Time difference: {minutes_diff} minutes")
        
        if not minutes_diff or minutes_diff <= 0:
            if self.debug:
                self.logger.debug("Rate calculation skipped - invalid time difference")
            return 0
        
        qso_diff = current_qsos - prev_qsos
        
        if qso_diff == 0:
            if self.debug:
                self.logger.debug("Rate is 0 - no new QSOs")
            return 0
        
        rate = int(round((qso_diff * 60) / minutes_diff))
        
        if self.debug:
            self.logger.debug(f"  QSO difference: {qso_diff}")
            self.logger.debug(f"  Calculated rate: {rate}/hr")
            
        return rate

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
                
    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Calculate contest QSO rates with detailed analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Basic rate calculation:
    %(prog)s --db contest_data.db --call W1AW --contest CQWW-CW
    
  Detailed analysis with debug info:
    %(prog)s --db contest_data.db --call W1AW --contest CQWW-CW --debug
    
  Custom time window:
    %(prog)s --db contest_data.db --call W1AW --contest CQWW-CW --minutes 30
        """
    )
    
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
