#!/usr/bin/env python3
import argparse
import sqlite3
import os
import sys
from datetime import datetime, timedelta
import logging

class ScoreReporter:
    # [Previous __init__, setup_logging, load_template, and get_station_details methods remain the same]

    def calculate_rate(self, current_qsos, previous_qsos, time_diff_hours):
        """Calculate hourly rate based on QSO difference and time difference"""
        if time_diff_hours <= 0:
            return 0
        qso_diff = current_qsos - previous_qsos
        # Interpolate to get hourly rate
        return int(round(qso_diff * (1.0 / time_diff_hours)))

    def get_band_breakdown_with_rate(self, station_id, callsign, contest):
        """Get band breakdown and calculate QSO rate for each band with interpolation"""
        current_query = """
            SELECT band, qsos, multipliers, cs.timestamp
            FROM band_breakdown bb
            JOIN contest_scores cs ON cs.id = bb.contest_score_id
            WHERE contest_score_id = ?;
        """
        
        previous_query = """
            WITH CurrentTimestamp AS (
                SELECT timestamp 
                FROM contest_scores 
                WHERE id = ?
            )
            SELECT 
                bb.band, 
                bb.qsos,
                cs.timestamp,
                ABS(ROUND((JULIANDAY(cs.timestamp) - 
                          JULIANDAY((SELECT timestamp FROM CurrentTimestamp))) * 24, 2)) as hours_diff
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = ? 
            AND cs.contest = ?
            AND cs.timestamp < (SELECT timestamp FROM CurrentTimestamp)
            ORDER BY cs.timestamp DESC;
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get current band breakdown
                cursor.execute(current_query, (station_id,))
                current_data = cursor.fetchall()
                
                if not current_data:
                    return {}
                
                # Organize current data and store current timestamp
                current_by_band = {}
                for row in current_data:
                    band, qsos, mults, current_timestamp = row
                    current_by_band[band] = {
                        'qsos': qsos,
                        'mults': mults,
                        'timestamp': current_timestamp
                    }
                
                # Get previous data points
                cursor.execute(previous_query, (station_id, callsign, contest))
                previous_data = cursor.fetchall()
                
                # Calculate rates for each band
                result = {}
                for band in current_by_band:
                    current = current_by_band[band]
                    current_qsos = current['qsos']
                    
                    # Find the closest previous data point for this band
                    previous_qsos = 0
                    rate = 0
                    found_previous = False
                    
                    for prev_row in previous_data:
                        prev_band, prev_qsos, prev_timestamp, hours_diff = prev_row
                        if prev_band == band:
                            # Found a previous data point for this band
                            found_previous = True
                            if hours_diff > 0:  # Avoid division by zero
                                rate = self.calculate_rate(current_qsos, prev_qsos, hours_diff)
                            break
                    
                    result[band] = (current_qsos, current['mults'], rate)
                    
                    # Log rate calculation details for debugging
                    self.logger.debug(f"{band} band rate calculation:")
                    self.logger.debug(f"  Current QSOs: {current_qsos}")
                    self.logger.debug(f"  Previous QSOs: {previous_qsos}")
                    self.logger.debug(f"  Calculated Rate: {rate}/hr")
                
                return result
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error in rate calculation: {e}")
            return {}

    def format_band_data(self, band_data):
        """Format band data as QSO/Mults (rate/h)"""
        if band_data:
            qsos, mults, rate = band_data
            rate_str = f"{rate:+d}" if rate != 0 else "0"  # Show + sign for positive rates
            return f"{qsos}/{mults} ({rate_str})"
        return "-/- (0)"

    # [Rest of the methods remain the same]

def main():
    parser = argparse.ArgumentParser(description='Generate contest score report')
    parser.add_argument('--callsign', required=True, help='Callsign to report')
    parser.add_argument('--contest', required=True, help='Contest name')
    parser.add_argument('--output-dir', required=True, help='Output directory for report')
    parser.add_argument('--db', default='contest_data.db', help='Database file path')
    parser.add_argument('--template', default='templates/contest_template.html', 
                      help='Path to HTML template file')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    reporter = ScoreReporter(args.db, args.template)
    if args.debug:
        reporter.logger.setLevel(logging.DEBUG)
        
    stations = reporter.get_station_details(args.callsign, args.contest)
    
    if stations:
        success = reporter.generate_html(args.callsign, args.contest, stations, args.output_dir)
        if not success:
            sys.exit(1)
    else:
        print(f"No data found for {args.callsign} in {args.contest}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
