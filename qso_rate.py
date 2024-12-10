#!/usr/bin/env python3
import sqlite3
import logging
from datetime import datetime, timedelta


class QsoRateCalculator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger('QsoRateCalculator')
        self.logger.setLevel(logging.INFO)
        
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


    def calculate_band_rates(self, cursor, callsign, contest, lookback_minutes=60):
        """
        Calculate per-band QSO rates within the last `lookback_minutes` using the current UTC time.
        Similar logic to calculate_total_rate: we find the latest record up to now and then find a 
        record about `lookback_minutes` ago for comparison.
    
        Returns a dictionary {band: rate_per_hour}, or empty if no data.
        """
        try:
            current_utc = datetime.utcnow()
            
            # Fetch current band data from the most recent record before or at current_utc
            current_qsos_by_band, current_ts = self._fetch_band_data(cursor, callsign, contest, current_utc)
            if not current_qsos_by_band:
                # No current data means no band rates
                return {}
    
            band_rates = {}
            for band, info in current_qsos_by_band.items():
                current_qsos = info['qsos']
    
                # Find previous data for this band
                prev_qsos, prev_ts = self._fetch_previous_band_data(cursor, callsign, contest, band, current_ts, lookback_minutes)
    
                rate = 0
                if prev_qsos is not None and prev_ts:
                    # Check time difference
                    current_dt = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
                    prev_dt = datetime.strptime(prev_ts, '%Y-%m-%d %H:%M:%S')
                    time_diff = (current_dt - prev_dt).total_seconds() / 60.0
    
                    if time_diff <= lookback_minutes:
                        qso_diff = current_qsos - prev_qsos
                        if qso_diff > 0:
                            # QSOs per hour
                            rate = int(round((qso_diff * 60) / time_diff))
    
                band_rates[band] = rate
    
            return band_rates
    
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            return {}
    
    def _fetch_band_data(self, cursor, callsign, contest, current_utc):
        """
        Fetch QSO counts for all bands from the latest record before or at current_utc.
        Returns a dictionary {band: {'qsos': int, 'multipliers': int}} and the timestamp of that record.
        """
        query = """
            WITH latest AS (
                SELECT id, timestamp
                FROM contest_scores
                WHERE callsign = ? AND contest = ?
                AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
            )
            SELECT bb.band, bb.qsos, bb.multipliers, l.timestamp
            FROM latest l
            JOIN band_breakdown bb ON bb.contest_score_id = l.id
        """
        params = (callsign, contest, current_utc.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.execute(query, params)
        results = cursor.fetchall()
    
        if not results:
            return {}, None
    
        band_data = {}
        record_ts = None
        for row in results:
            band, qsos, mults, ts = row
            band_data[band] = {'qsos': qsos, 'multipliers': mults}
            record_ts = ts  # All rows have the same timestamp, since they come from the same record
    
        return band_data, record_ts
    
    def _fetch_previous_band_data(self, cursor, callsign, contest, band, current_ts, window_minutes):
        """
        Fetch a previous QSO count for the given band at about current_ts - window_minutes.
        We find the closest record before (current_ts - window_minutes).
        Returns (prev_qsos, prev_ts) or (None, None) if not found.
        """
        current_dt = datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S')
        lookback_time = current_dt - timedelta(minutes=window_minutes)
    
        query = """
            SELECT bb.qsos, cs.timestamp
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = ? AND cs.contest = ? AND bb.band = ?
            AND cs.timestamp <= ?
            ORDER BY cs.timestamp DESC
            LIMIT 1
        """
        params = (
            callsign, contest, band,
            lookback_time.strftime('%Y-%m-%d %H:%M:%S')
        )
        cursor.execute(query, params)
        result = cursor.fetchone()
    
        if not result:
            return None, None
    
        prev_qsos, prev_ts = result
        return prev_qsos, prev_ts

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
