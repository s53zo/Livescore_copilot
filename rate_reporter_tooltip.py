#!/usr/bin/env python3
import sqlite3
import logging
import json
from datetime import datetime, timedelta

class RateReporterTooltip:
    def __init__(self, db_path):
        self.db_path = db_path
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('RateReporterTooltip')
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.INFO)

    def get_rate_history(self, callsign, contest, end_time, hours=3):
        """Get rate history for all bands over the last N hours"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Convert end_time to datetime if it's a string
                if isinstance(end_time, str):
                    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                
                # Generate time windows for the query
                time_windows = []
                for hour in range(hours):
                    window_time = end_time - timedelta(hours=hour)
                    time_windows.append(window_time.strftime('%Y-%m-%d %H:00:00'))
                
                # Get band QSO counts for each time window
                rates = []
                for window_start in time_windows:
                    window_end = datetime.strptime(window_start, '%Y-%m-%d %H:%M:%S') + timedelta(hours=1)
                    
                    # Query for QSOs in this time window
                    cursor.execute("""
                        WITH band_qsos AS (
                            SELECT 
                                bb.band,
                                SUM(bb.qsos) as qsos,
                                MIN(cs.timestamp) as first_ts,
                                MAX(cs.timestamp) as last_ts
                            FROM contest_scores cs
                            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                            WHERE cs.callsign = ?
                            AND cs.contest = ?
                            AND cs.timestamp >= ?
                            AND cs.timestamp < ?
                            GROUP BY bb.band
                            HAVING qsos > 0
                        )
                        SELECT 
                            band,
                            qsos,
                            first_ts,
                            last_ts
                        FROM band_qsos
                        ORDER BY band
                    """, (callsign, contest, window_start, window_end.strftime('%Y-%m-%d %H:%M:%S')))
                    
                    results = cursor.fetchall()
                    window_rates = {}
                    
                    for band, qsos, first_ts, last_ts in results:
                        if first_ts and last_ts:
                            time_diff = (datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S') - 
                                       datetime.strptime(first_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600
                            if time_diff > 0:
                                rate = int(round(qsos / time_diff))
                                window_rates[band] = rate
                    
                    if window_rates:
                        rates.append({
                            'time': datetime.strptime(window_start, '%Y-%m-%d %H:%M:%S').strftime('%H:%M'),
                            'rates': window_rates
                        })
                
                # Also get total QSOs per band for summary
                cursor.execute("""
                    WITH latest_score AS (
                        SELECT id, timestamp
                        FROM contest_scores
                        WHERE callsign = ?
                        AND contest = ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    )
                    SELECT bb.band, bb.qsos
                    FROM latest_score ls
                    JOIN band_breakdown bb ON bb.contest_score_id = ls.id
                    WHERE bb.qsos > 0
                    ORDER BY bb.band
                """, (callsign, contest))
                
                total_qsos = {row[0]: row[1] for row in cursor.fetchall()}
                
                return {
                    'rates': list(reversed(rates)),  # Most recent first
                    'total_qsos': total_qsos
                }
                
        except Exception as e:
            self.logger.error(f"Error getting rate history: {e}")
            return None

    def get_tooltip_data(self, callsign, contest, timestamp):
        """Get formatted tooltip data including rates and summary"""
        try:
            rate_data = self.get_rate_history(callsign, contest, timestamp)
            if not rate_data:
                return None
            
            # Format data for the tooltip
            tooltip_data = {
                'callsign': callsign,
                'rateHistory': rate_data['rates'],
                'summary': {
                    'totalQsos': rate_data['total_qsos'],
                    'peakRates': {}
                }
            }
            
            # Calculate peak rates for each band
            for band in rate_data['total_qsos'].keys():
                peak_rate = max(
                    (point['rates'].get(band, 0) for point in rate_data['rates']),
                    default=0
                )
                tooltip_data['summary']['peakRates'][band] = peak_rate
            
            return tooltip_data
            
        except Exception as e:
            self.logger.error(f"Error formatting tooltip data: {e}")
            return None

    def generate_tooltip_html(self, callsign, contest, timestamp):
        """Generate HTML data attribute content for the tooltip"""
        try:
            tooltip_data = self.get_tooltip_data(callsign, contest, timestamp)
            if tooltip_data:
                return f"data-tooltip='{json.dumps(tooltip_data)}'"
            return ""
            
        except Exception as e:
            self.logger.error(f"Error generating tooltip HTML: {e}")
            return ""
