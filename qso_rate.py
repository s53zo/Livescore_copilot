#!/usr/bin/env python3
import sqlite3
import logging
from datetime import datetime

class QsoRateCalculator:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def calculate_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate QSO rates for both long and short time windows"""
        query = """
            WITH current_score AS (
                SELECT qsos, timestamp
                FROM contest_scores
                WHERE callsign = ? 
                AND contest = ?
                AND timestamp = ?
            ),
            long_window_score AS (
                SELECT qsos
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-60 minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-60 minutes')))
                LIMIT 1
            ),
            short_window_score AS (
                SELECT qsos
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-15 minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-15 minutes')))
                LIMIT 1
            )
            SELECT 
                cs.qsos as current_qsos,
                lws.qsos as long_window_qsos,
                sws.qsos as short_window_qsos
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
            
        current_qsos, long_window_qsos, short_window_qsos = result
        
        # Calculate 60-minute rate
        long_rate = 0
        if long_window_qsos is not None:
            qso_diff = current_qsos - long_window_qsos
            if qso_diff > 0:
                long_rate = int(round((qso_diff * 60) / 60))  # 60-minute rate
                
        # Calculate 15-minute rate
        short_rate = 0
        if short_window_qsos is not None:
            qso_diff = current_qsos - short_window_qsos
            if qso_diff > 0:
                short_rate = int(round((qso_diff * 60) / 15))  # Convert 15-minute to hourly rate
                
        return long_rate, short_rate

    def calculate_band_rates(self, cursor, callsign, contest, timestamp, long_window=60, short_window=15):
        """Calculate per-band QSO rates considering current time and actual QSO increases"""
        try:
            query = """
            WITH now AS (
                SELECT datetime('now') as current_utc
            ),
            total_qsos AS (
                SELECT cs.timestamp, bb.band, bb.qsos, bb.multipliers
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                CROSS JOIN now n 
                WHERE cs.callsign = ? 
                AND cs.contest = ?
                AND cs.timestamp = ?
                AND (julianday(n.current_utc) - julianday(cs.timestamp)) * 24 * 60 <= 75
            )
            SELECT band, qsos, multipliers, 0 as long_rate, 0 as short_rate
            FROM total_qsos
            WHERE qsos > 0
            ORDER BY band
            """
                
            cursor.execute(query, (callsign, contest, timestamp))
            results = cursor.fetchall()
            
            band_data = {}
            for row in results:
                band = row[0]
                band_data[band] = [row[1], row[2], 0, 0]  # qsos, multipliers, long_rate=0, short_rate=0
    
            return band_data
                    
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            self.logger.debug(traceback.format_exc())
            return {}
