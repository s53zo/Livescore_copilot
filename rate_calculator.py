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

    def calculate_band_rates(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate per-band QSO rates prioritizing reports near the target lookback time"""
        query = """
            WITH latest_scores AS (
                SELECT cs.id, cs.timestamp
                FROM contest_scores cs
                WHERE cs.callsign = ? 
                AND cs.contest = ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            latest_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as current_qsos,
                    cs.timestamp as current_ts
                FROM latest_scores ls
                JOIN contest_scores cs ON cs.id = ls.id
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            ),
            previous_bands_all AS (
                SELECT 
                    bb.band,
                    bb.qsos as prev_qsos,
                    cs.timestamp as prev_ts,
                    ABS(ROUND((JULIANDAY(cs.timestamp) - JULIANDAY(lb.current_ts)) * 24 * 60, 1)) as time_diff,
                    -- Flag records within 5 minutes of target lookback time
                    CASE 
                        WHEN ABS(ROUND((JULIANDAY(cs.timestamp) - JULIANDAY(lb.current_ts)) * 24 * 60, 1) - ?) <= 5 
                        THEN 1 
                        ELSE 0 
                    END as is_target_window
                FROM latest_bands lb
                CROSS JOIN contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp < (SELECT current_ts FROM latest_bands LIMIT 1)
                AND cs.timestamp >= datetime((SELECT current_ts FROM latest_bands LIMIT 1), ? || ' minutes')
            ),
            prioritized_previous AS (
                -- First try to get records close to target lookback time
                SELECT band, prev_qsos, prev_ts, time_diff
                FROM previous_bands_all
                WHERE is_target_window = 1
                AND (band, time_diff) IN (
                    SELECT band, MIN(time_diff)
                    FROM previous_bands_all
                    WHERE is_target_window = 1
                    GROUP BY band
                )
                UNION ALL
                -- Fall back to closest available record for bands with no target window match
                SELECT pba.band, pba.prev_qsos, pba.prev_ts, pba.time_diff
                FROM previous_bands_all pba
                WHERE pba.band NOT IN (
                    SELECT band FROM previous_bands_all WHERE is_target_window = 1
                )
                AND (pba.band, pba.time_diff) IN (
                    SELECT band, MIN(time_diff)
                    FROM previous_bands_all pba2
                    WHERE pba2.band NOT IN (
                        SELECT band FROM previous_bands_all WHERE is_target_window = 1
                    )
                    GROUP BY band
                )
            )
            SELECT DISTINCT
                lb.band,
                lb.current_qsos,
                pp.prev_qsos,
                lb.current_ts,
                pp.prev_ts,
                pp.time_diff as minutes_diff
            FROM latest_bands lb
            LEFT JOIN prioritized_previous pp ON lb.band = pp.band
            WHERE lb.current_qsos > 0
            ORDER BY lb.band
        """
        
        minutes_param = f"-{lookback_minutes}"
        cursor.execute(query, (callsign, contest, lookback_minutes, callsign, contest, minutes_param))
        results = cursor.fetchall()
        
        if self.debug:
            self.logger.debug(f"\nCalculating band rates for {callsign} in {contest}")
            self.logger.debug(f"Looking back {lookback_minutes} minutes")
            self.logger.debug(f"Found {len(results)} bands with activity")
        
        band_rates = {}
        for row in results:
            band, current_qsos, prev_qsos, current_ts, prev_ts, minutes_diff = row
            
            if self.debug:
                self.logger.debug(f"\nBand {band} analysis:")
                self.logger.debug(f"  Current QSOs: {current_qsos}")
                self.logger.debug(f"  Previous QSOs: {prev_qsos if prev_qsos is not None else 'None'}")
                self.logger.debug(f"  Current timestamp: {current_ts}")
                self.logger.debug(f"  Previous timestamp: {prev_ts if prev_ts is not None else 'None'}")
                self.logger.debug(f"  Time difference: {minutes_diff if minutes_diff is not None else 'None'} minutes")
            
            if not prev_ts or not minutes_diff or minutes_diff <= 0:
                if self.debug:
                    self.logger.debug("  Rate calculation skipped - insufficient data")
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
                # Calculate hourly rate
                rate = int(round((qso_diff * 60) / minutes_diff))
                band_rates[band] = rate
                if self.debug:
                    self.logger.debug(f"  QSO difference: {qso_diff}")
                    self.logger.debug(f"  Calculated rate: {rate}/hr")
        
        return band_rates
    
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
