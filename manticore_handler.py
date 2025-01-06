from manticore.client import Client as ManticoreClient
import sqlite3
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

class ManticoreHandler:
    def __init__(self, manticore_url: str, sqlite_db: str):
        self.manticore = ManticoreClient(manticore_url)
        self.sqlite_db = sqlite_db
        self.logger = logging.getLogger('ManticoreHandler')
        self._setup_indexes()

    def _setup_indexes(self):
        """Create required Manticore indexes if they don't exist"""
        try:
            # Real-time index for contest scores
            self.manticore.sql('CREATE TABLE IF NOT EXISTS rt_contest_scores (\
                id BIGINT, \
                callsign STRING, \
                contest STRING, \
                score BIGINT, \
                power STRING, \
                assisted STRING, \
                qsos INT, \
                multipliers INT, \
                timestamp TIMESTAMP \
            ) rt_mem')

            # Real-time index for band breakdown
            self.manticore.sql('CREATE TABLE IF NOT EXISTS rt_band_breakdown (\
                id BIGINT, \
                contest_score_id BIGINT, \
                band STRING, \
                mode STRING, \
                qsos INT, \
                points INT, \
                multipliers INT \
            ) rt_mem')

            # Real-time index for QTH info
            self.manticore.sql('CREATE TABLE IF NOT EXISTS rt_qth_info (\
                id BIGINT, \
                contest_score_id BIGINT, \
                dxcc_country STRING, \
                continent STRING, \
                cq_zone INT, \
                iaru_zone INT, \
                grid6 STRING, \
                latitude FLOAT, \
                longitude FLOAT \
            ) rt_mem')

        except Exception as e:
            self.logger.error(f"Error setting up Manticore indexes: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def sync_record(self, record_id: int) -> bool:
        """Sync a single record from SQLite to Manticore"""
        try:
            with sqlite3.connect(self.sqlite_db) as conn:
                cursor = conn.cursor()
                
                # Get contest score data
                cursor.execute("""
                    SELECT id, callsign, contest, score, power, assisted,
                           qsos, multipliers, timestamp
                    FROM contest_scores
                    WHERE id = ?
                """, (record_id,))
                score_data = cursor.fetchone()
                
                if not score_data:
                    return False
                
                # Sync to Manticore
                self.manticore.replace('rt_contest_scores', {
                    'id': score_data[0],
                    'callsign': score_data[1],
                    'contest': score_data[2],
                    'score': score_data[3],
                    'power': score_data[4],
                    'assisted': score_data[5],
                    'qsos': score_data[6],
                    'multipliers': score_data[7],
                    'timestamp': int(datetime.strptime(score_data[8], 
                                                     '%Y-%m-%d %H:%M:%S').timestamp())
                })
                
                # Sync band breakdown
                self._sync_band_breakdown(record_id)
                
                # Sync QTH info
                self._sync_qth_info(record_id)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error syncing record {record_id}: {e}")
            self.logger.error(traceback.format_exc())
            return False

    def _sync_band_breakdown(self, contest_score_id: int):
        """Sync band breakdown data for a contest score"""
        with sqlite3.connect(self.sqlite_db) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, band, mode, qsos, points, multipliers
                FROM band_breakdown
                WHERE contest_score_id = ?
            """, (contest_score_id,))
            
            for row in cursor.fetchall():
                self.manticore.replace('rt_band_breakdown', {
                    'id': row[0],
                    'contest_score_id': contest_score_id,
                    'band': row[1],
                    'mode': row[2],
                    'qsos': row[3],
                    'points': row[4],
                    'multipliers': row[5]
                })

    def _sync_qth_info(self, contest_score_id: int):
        """Sync QTH info for a contest score"""
        with sqlite3.connect(self.sqlite_db) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, dxcc_country, continent, cq_zone,
                       iaru_zone, grid6
                FROM qth_info
                WHERE contest_score_id = ?
            """, (contest_score_id,))
            
            row = cursor.fetchone()
            if row:
                self.manticore.replace('rt_qth_info', {
                    'id': row[0],
                    'contest_score_id': contest_score_id,
                    'dxcc_country': row[1],
                    'continent': row[2],
                    'cq_zone': row[3],
                    'iaru_zone': row[4],
                    'grid6': row[5]
                })

    def get_rankings(self, contest: str, filter_type: Optional[str] = None, 
                    filter_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get contest rankings using Manticore"""
        query = f"SELECT * FROM rt_contest_scores WHERE contest = '{contest}'"
        
        if filter_type and filter_value:
            if filter_type == 'Continent':
                query += f" AND id IN (SELECT contest_score_id FROM rt_qth_info \
                          WHERE continent = '{filter_value}')"
            elif filter_type == 'CQ Zone':
                query += f" AND id IN (SELECT contest_score_id FROM rt_qth_info \
                          WHERE cq_zone = {filter_value})"
        
        query += " ORDER BY score DESC"
        
        result = self.manticore.sql(query)
        return result['rows'] if 'rows' in result else []

    def get_band_activity(self, contest: str, band: str) -> List[Dict[str, Any]]:
        """Get band activity data using Manticore"""
        query = f"""
            SELECT cs.callsign, bb.qsos, bb.points, bb.multipliers
            FROM rt_contest_scores cs
            JOIN rt_band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.contest = '{contest}' AND bb.band = '{band}'
            ORDER BY bb.qsos DESC
        """
        result = self.manticore.sql(query)
        return result['rows'] if 'rows' in result else []
