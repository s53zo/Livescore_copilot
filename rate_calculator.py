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

    def calculate_rates(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate band and total rates using nearest available QSO data"""
        query = """
            WITH latest_score AS (
                SELECT 
                    cs.id,
                    cs.timestamp,
                    cs.qsos as total_qsos
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
                    ls.timestamp as current_ts
                FROM latest_score ls
                JOIN band_breakdown bb ON bb.contest_score_id = ls.id
                WHERE bb.qsos > 0
            ),
            previous_bands AS (
                -- For each band, find the most recent previous record within the lookback window
                SELECT 
                    lb.band,
                    (
                        SELECT bb2.qsos
                        FROM contest_scores cs2
                        JOIN band_breakdown bb2 ON bb2.contest_score_id = cs2.id
                        WHERE cs2.callsign = ?
                        AND cs2.contest = ?
                        AND cs2.timestamp < lb.current_ts
                        AND cs2.timestamp >= datetime(lb.current_ts, ? || ' minutes')
                        AND bb2.band = lb.band
                        ORDER BY cs2.timestamp DESC
                        LIMIT 1
                    ) as prev_qsos,
                    (
                        SELECT cs2.timestamp
                        FROM contest_scores cs2
                        JOIN band_breakdown bb2 ON bb2.contest_score_id = cs2.id
                        WHERE cs2.callsign = ?
                        AND cs2.contest = ?
                        AND cs2.timestamp < lb.current_ts
                        AND cs2.timestamp >= datetime(lb.current_ts, ? || ' minutes')
                        AND bb2.band = lb.band
                        ORDER BY cs2.timestamp DESC
                        LIMIT 1
                    ) as prev_ts
                FROM latest_bands lb
            )
            SELECT 
                lb.band,
                lb.current_qsos,
                pb.prev_qsos,
                lb.current_ts,
                pb.prev_ts,
                ROUND((JULIANDAY(lb.current_ts) - JULIANDAY(pb.prev_ts)) * 24 * 60, 1) as minutes_diff
            FROM latest_bands lb
            LEFT JOIN previous_bands pb ON lb.band = pb.band
            ORDER BY lb.band
        """
        
        minutes_param = f"-{lookback_minutes}"
        cursor.execute(query, (callsign, contest, 
                             callsign, contest, minutes_param,
                             callsign, contest, minutes_param))
        results = cursor.fetchall()
        
        if self.debug:
            self.logger.debug(f"\nCalculating rates for {callsign} in {contest}")
            self.logger.debug(f"Looking back {lookback_minutes} minutes")
            self.logger.debug(f"Found {len(results)} active bands")
        
        band_rates = {}
        total_qso_diff = 0
        total_time_diff = 0
        band_count = 0

        for row in results:
            band, current_qsos, prev_qsos, current_ts, prev_ts, minutes_diff = row
            
            if self.debug:
                self.logger.debug(f"\nBand {band} analysis:")
                self.logger.debug(f"  Current QSOs: {current_qsos}")
                self.logger.debug(f"  Previous QSOs: {prev_qsos if prev_qsos is not None else 'No previous data'}")
                self.logger.debug(f"  Current timestamp: {current_ts}")
                self.logger.debug(f"  Previous timestamp: {prev_ts if prev_ts is not None else 'No previous timestamp'}")
                self.logger.debug(f"  Time difference: {minutes_diff if minutes_diff is not None else 'N/A'} minutes")
            
            if prev_ts is None or prev_qsos is None:
                # Try to find the earliest QSO on this band
                earliest_query = """
                    SELECT cs.timestamp, bb.qsos
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND bb.band = ?
                    AND bb.qsos > 0
                    ORDER BY cs.timestamp ASC
                    LIMIT 1
                """
                cursor.execute(earliest_query, (callsign, contest, band))
                earliest = cursor.fetchone()
                
                if earliest and earliest[0] == current_ts:
                    if self.debug:
                        self.logger.debug(f"  First QSOs on band {band}")
                    # This is the first entry for this band
                    time_diff = 60  # Assume first hour rate
                    band_rates[band] = int(round((current_qsos * 60) / time_diff))
                    total_qso_diff += current_qsos
                    total_time_diff += time_diff
                    band_count += 1
                else:
                    band_rates[band] = 0
                continue
            
            if minutes_diff <= 0:
                band_rates[band] = 0
                continue
            
            qso_diff = current_qsos - prev_qsos
            if qso_diff == 0:
                band_rates[band] = 0
            else:
                rate = int(round((qso_diff * 60) / minutes_diff))
                band_rates[band] = rate
                total_qso_diff += qso_diff
                total_time_diff += minutes_diff
                band_count += 1
                
                if self.debug:
                    self.logger.debug(f"  QSO difference: {qso_diff}")
                    self.logger.debug(f"  Calculated rate: {rate}/hr")
        
        # Calculate average time difference for total rate
        avg_time_diff = total_time_diff / band_count if band_count > 0 else 0
        total_rate = int(round((total_qso_diff * 60) / avg_time_diff)) if avg_time_diff > 0 else 0
        
        if self.debug:
            self.logger.debug(f"\nTotal statistics:")
            self.logger.debug(f"  Total QSO difference: {total_qso_diff}")
            self.logger.debug(f"  Average time difference: {avg_time_diff:.1f} minutes")
            self.logger.debug(f"  Total rate: {total_rate}/hr")
        
        return band_rates, total_rate

def analyze_rates(args):
    """Analyze rates for given callsign and contest"""
    calculator = RateCalculator(args.db, args.debug)
    
    try:
        with sqlite3.connect(args.db) as conn:
            cursor = conn.cursor()
            
            # Calculate all rates at once
            band_rates, total_rate = calculator.calculate_rates(cursor, args.call, args.contest, args.minutes)
            
            print(f"\nTotal QSO Rate: {total_rate}/hr")
            
            if band_rates:
                print("\nPer-band QSO Rates:")
                for band in sorted(band_rates.keys(), key=lambda x: int(x)):
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
