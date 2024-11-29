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
        """Calculate per-band QSO rates for both time windows"""
        try:
            current_ts = datetime.now()  # Use current time as reference
            stored_ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            # If data is too old (more than 90 minutes), return zero rates
            if (current_ts - stored_ts).total_seconds() / 60 > 90:
                # Query just to get band data without rates
                query = """
                    SELECT bb.band, bb.qsos, bb.multipliers
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp = ?
                    AND bb.qsos > 0
                    ORDER BY bb.band
                """
                cursor.execute(query, (callsign, contest, timestamp))
                results = cursor.fetchall()
                return {row[0]: [row[1], row[2], 0, 0] for row in results}
    
            # Otherwise, proceed with normal rate calculation
            query = """
                WITH current_bands AS (
                    SELECT 
                        bb.band,
                        bb.qsos as current_qsos,
                        bb.multipliers
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.callsign = ? 
                    AND cs.contest = ?
                    AND cs.timestamp = ?
                ),
                prev_time_ranges AS (
                    SELECT 
                        cs.timestamp as ts,
                        CASE 
                            WHEN (julianday(?) - julianday(cs.timestamp)) * 24 * 60 <= 90 
                            THEN 'valid'
                            ELSE 'invalid'
                        END as time_status
                    FROM contest_scores cs
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND cs.timestamp < ?
                ),
                long_window_bands AS (
                    SELECT 
                        bb.band,
                        bb.qsos as long_window_qsos
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    JOIN prev_time_ranges pt ON cs.timestamp = pt.ts
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND pt.time_status = 'valid'
                    AND (julianday(?) - julianday(cs.timestamp)) * 24 * 60 <= 60
                    ORDER BY cs.timestamp DESC
                ),
                short_window_bands AS (
                    SELECT 
                        bb.band,
                        bb.qsos as short_window_qsos
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    JOIN prev_time_ranges pt ON cs.timestamp = pt.ts
                    WHERE cs.callsign = ?
                    AND cs.contest = ?
                    AND pt.time_status = 'valid'
                    AND (julianday(?) - julianday(cs.timestamp)) * 24 * 60 <= 15
                    ORDER BY cs.timestamp DESC
                )
                SELECT 
                    cb.band,
                    cb.current_qsos,
                    cb.multipliers,
                    lwb.long_window_qsos,
                    swb.short_window_qsos
                FROM current_bands cb
                LEFT JOIN long_window_bands lwb ON cb.band = lwb.band
                LEFT JOIN short_window_bands swb ON cb.band = swb.band
                WHERE cb.current_qsos > 0
                ORDER BY cb.band
            """
            
            current_ts_str = current_ts.strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute(query, (
                callsign, contest, timestamp,
                current_ts_str,  # For time validity check
                callsign, contest, timestamp,
                callsign, contest, current_ts_str,
                callsign, contest, current_ts_str
            ))
            
            results = cursor.fetchall()
            band_data = {}
            
            for row in results:
                band, current_qsos, multipliers, long_window_qsos, short_window_qsos = row
                band_data[band] = [
                    current_qsos,
                    multipliers,
                    long_window_qsos or current_qsos,  # Use current_qsos if long_window_qsos is NULL
                    short_window_qsos or current_qsos   # Use current_qsos if short_window_qsos is NULL
                ]
            
            return band_data
                
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            self.logger.debug(traceback.format_exc())
            return {}
