#!/usr/bin/env python3
import sqlite3
import logging
from datetime import datetime

class QsoRateCalculator:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def calculate_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate QSO rates for both long and short time windows with validation."""
        current_dt = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
    
        # Modified queries to also fetch timestamps of previous records
        query = f"""
            WITH current_score AS (
                SELECT qsos, timestamp as current_ts
                FROM contest_scores
                WHERE callsign = ? 
                AND contest = ?
                AND timestamp = ?
            ),
            long_window_score AS (
                SELECT qsos, timestamp as prev_ts
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-{long_window} minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-{long_window} minutes')))
                LIMIT 1
            ),
            short_window_score AS (
                SELECT qsos, timestamp as prev_ts
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-{short_window} minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-{short_window} minutes')))
                LIMIT 1
            )
            SELECT 
                cs.qsos as current_qsos, cs.current_ts,
                lws.qsos as long_window_qsos, lws.prev_ts as long_window_ts,
                sws.qsos as short_window_qsos, sws.prev_ts as short_window_ts
            FROM current_score cs
            LEFT JOIN long_window_score lws
            LEFT JOIN short_window_score sws
        """
    
        cursor.execute(query, (
            callsign, contest, current_ts,
            callsign, contest, current_ts, current_ts,
            callsign, contest, current_ts, current_ts
        ))
        
        result = cursor.fetchone()
        if not result:
            return 0, 0
    
        current_qsos, current_ts_str, long_window_qsos, long_window_ts_str, short_window_qsos, short_window_ts_str = result
    
        # Convert timestamps to datetime
        # current_dt is already defined above
        # Check and calculate long_rate
        long_rate = 0
        if long_window_qsos is not None and long_window_ts_str is not None:
            prev_dt = datetime.strptime(long_window_ts_str, '%Y-%m-%d %H:%M:%S')
            time_diff = (current_dt - prev_dt).total_seconds() / 60.0
            # Only proceed if within the long_window
            if time_diff <= long_window:
                qso_diff = current_qsos - long_window_qsos
                if qso_diff > 0:
                    # Calculate the hourly rate based on the actual time difference
                    long_rate = int(round((qso_diff * 60) / time_diff))
    
        # Check and calculate short_rate
        short_rate = 0
        if short_window_qsos is not None and short_window_ts_str is not None:
            prev_dt = datetime.strptime(short_window_ts_str, '%Y-%m-%d %H:%M:%S')
            time_diff = (current_dt - prev_dt).total_seconds() / 60.0
            # Only proceed if within the short_window
            if time_diff <= short_window:
                qso_diff = current_qsos - short_window_qsos
                if qso_diff > 0:
                    # Convert to hourly rate (qso_diff * 60 / short_window_minutes)
                    short_rate = int(round((qso_diff * 60) / time_diff))
    
        return long_rate, short_rate


    def calculate_band_rates(self, cursor, callsign, contest, timestamp, long_window=60, short_window=15):
        """Calculate band-specific QSO rates for both long and short windows, discarding too-old records."""
        try:
            current_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            # Fetch current band data
            current_bands = self._fetch_band_data(cursor, callsign, contest, current_dt)
            
            # For each band, we try to find a previous data point at (current_dt - long_window) and (current_dt - short_window)
            band_rates = {}
            for band, current_info in current_bands.items():
                current_qsos = current_info[0]
                multipliers = current_info[1]
    
                # 60-minute rate
                long_rate = self._calculate_band_rate_for_window(cursor, callsign, contest, band, current_dt, long_window, current_qsos)
                
                # 15-minute rate
                short_rate = self._calculate_band_rate_for_window(cursor, callsign, contest, band, current_dt, short_window, current_qsos)
    
                band_rates[band] = [current_qsos, multipliers, long_rate, short_rate]
    
            return band_rates
    
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            return {}
    
    def _fetch_band_data(self, cursor, callsign, contest, current_dt):
        """Fetch the current band QSO and multiplier data."""
        query = """
            SELECT bb.band, bb.qsos, bb.multipliers
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = ? AND cs.contest = ? AND cs.timestamp = ?
        """
        params = (callsign, contest, current_dt.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.execute(query, params)
        results = cursor.fetchall()
    
        band_data = {}
        for row in results:
            band_data[row[0]] = [row[1], row[2]]  # qsos, multipliers
        return band_data
    
    def _calculate_band_rate_for_window(self, cursor, callsign, contest, band, current_dt, window_minutes, current_qsos):
        """Calculate band QSO rate for a given time window, discarding outdated previous records."""
        lookback_time = current_dt - timedelta(minutes=window_minutes)
    
        query = """
            WITH previous_band AS (
                SELECT bb.qsos as prev_qsos, cs.timestamp as prev_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ? AND cs.contest = ? AND bb.band = ?
                AND cs.timestamp <= ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            )
            SELECT prev_qsos, prev_ts FROM previous_band
        """
    
        params = (
            callsign, contest, band,
            lookback_time.strftime('%Y-%m-%d %H:%M:%S')
        )
        cursor.execute(query, params)
        result = cursor.fetchone()
    
        if not result:
            # No previous data at all
            return 0
    
        prev_qsos, prev_ts_str = result
        prev_dt = datetime.strptime(prev_ts_str, '%Y-%m-%d %H:%M:%S')
        time_diff = (current_dt - prev_dt).total_seconds() / 60.0
    
        # If the previous record is older than the intended window, discard it
        if time_diff > window_minutes:
            return 0
    
        qso_diff = current_qsos - prev_qsos
        if qso_diff <= 0:
            return 0
    
        # Rate in QSOs per hour
        return int(round((qso_diff * 60) / time_diff))
