#!/usr/bin/env python3
from manticoresearch import ApiClient, Configuration
from manticoresearch.api import SearchApi, IndexApi, UtilsApi
import sqlite3
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional

class ManticoreHandler:
    def __init__(self, manticore_url: str, sqlite_db: str):
        self.sqlite_db = sqlite_db
        self.logger = logging.getLogger('ManticoreHandler')
        
        # Configure Manticore API clients
        self.config = Configuration(host=manticore_url)
        self.api_client = ApiClient(self.config)
        self.search_api = SearchApi(self.api_client)
        self.index_api = IndexApi(self.api_client)
        self.utils_api = UtilsApi(self.api_client)
        
        self._setup_indexes()

    def _setup_indexes(self):
        """Create required Manticore RT indexes if they don't exist"""
        try:
            # Create RT indexes using Manticore syntax
            create_statements = [
                """
                CREATE TABLE IF NOT EXISTS rt_contest_scores(
                    id bigint,
                    callsign text,
                    contest text,
                    score bigint,
                    power text,
                    assisted text,
                    qsos integer,
                    multipliers integer,
                    timestamp timestamp
                ) type='rt'
                """,
                """
                CREATE TABLE IF NOT EXISTS rt_band_breakdown(
                    id bigint,
                    contest_score_id bigint,
                    band text,
                    mode text,
                    qsos integer,
                    points integer,
                    multipliers integer
                ) type='rt'
                """,
                """
                CREATE TABLE IF NOT EXISTS rt_qth_info(
                    id bigint,
                    contest_score_id bigint,
                    dxcc_country text,
                    continent text,
                    cq_zone integer,
                    iaru_zone integer,
                    grid6 text,
                    latitude float,
                    longitude float
                ) type='rt'
                """
            ]
            
            # Execute each create statement
            for statement in create_statements:
                try:
                    self.utils_api.sql(statement)
                    self.logger.debug(f"Successfully created index with statement: {statement}")
                except Exception as e:
                    # Log the error but continue - the index might already exist
                    self.logger.warning(f"Error creating index (might already exist): {e}")
                    continue

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

                # Convert timestamp to Unix timestamp
                timestamp = int(datetime.strptime(score_data[8], '%Y-%m-%d %H:%M:%S').timestamp())

                # Prepare document for insertion
                insert_document = {
                    'index': 'rt_contest_scores',
                    'id': score_data[0],
                    'doc': {
                        'callsign': score_data[1],
                        'contest': score_data[2],
                        'score': score_data[3],
                        'power': score_data[4] or '',
                        'assisted': score_data[5] or '',
                        'qsos': score_data[6],
                        'multipliers': score_data[7],
                        'timestamp': timestamp
                    }
                }
                
                # Insert/update in Manticore
                self.index_api.replace(insert_document_request=insert_document)
                
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
                insert_document = {
                    'index': 'rt_band_breakdown',
                    'id': row[0],
                    'doc': {
                        'contest_score_id': contest_score_id,
                        'band': row[1] or '',
                        'mode': row[2] or '',
                        'qsos': row[3],
                        'points': row[4],
                        'multipliers': row[5]
                    }
                }
                self.index_api.replace(insert_document_request=insert_document)

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
                insert_document = {
                    'index': 'rt_qth_info',
                    'id': row[0],
                    'doc': {
                        'contest_score_id': contest_score_id,
                        'dxcc_country': row[1] or '',
                        'continent': row[2] or '',
                        'cq_zone': row[3] or 0,
                        'iaru_zone': row[4] or 0,
                        'grid6': row[5] or ''
                    }
                }
                self.index_api.replace(insert_document_request=insert_document)

    def get_rankings(self, contest: str, filter_type: Optional[str] = None, 
                    filter_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get contest rankings using Manticore"""
        try:
            # Base query using proper escaping
            base_query = f'SELECT callsign, score, qsos, multipliers, power, assisted, timestamp ' \
                        f'FROM rt_contest_scores WHERE contest = "{contest}"'

            # Add filters if specified
            if filter_type and filter_value:
                if filter_type == 'Continent':
                    base_query += f' AND continent = "{filter_value}"'
                elif filter_type == 'CQ Zone':
                    base_query += f' AND cq_zone = {filter_value}'
                elif filter_type == 'DXCC':
                    base_query += f' AND dxcc_country = "{filter_value}"'

            # Add ordering
            base_query += ' ORDER BY score DESC'

            # Execute query through SQL API
            response = self.utils_api.sql(base_query)
            if not response or not hasattr(response, 'hits'):
                return []

            return [
                {
                    'callsign': hit['_source']['callsign'],
                    'score': hit['_source']['score'],
                    'qsos': hit['_source'].get('qsos', 0),
                    'multipliers': hit['_source'].get('multipliers', 0),
                    'power': hit['_source'].get('power', ''),
                    'assisted': hit['_source'].get('assisted', ''),
                    'timestamp': hit['_source'].get('timestamp', '')
                }
                for hit in response.hits.hits
            ]

        except Exception as e:
            self.logger.error(f"Error getting rankings: {e}")
            self.logger.error(traceback.format_exc())
            return []

    def get_band_activity(self, contest: str, band: str) -> List[Dict[str, Any]]:
        """Get band activity data using Manticore"""
        try:
            query = f'SELECT callsign, qsos, points, multipliers ' \
                    f'FROM rt_band_breakdown ' \
                    f'WHERE contest = "{contest}" AND band = "{band}" ' \
                    f'ORDER BY qsos DESC'

            response = self.utils_api.sql(query)
            if not response or not hasattr(response, 'hits'):
                return []

            return [
                {
                    'callsign': hit['_source']['callsign'],
                    'qsos': hit['_source'].get('qsos', 0),
                    'points': hit['_source'].get('points', 0),
                    'multipliers': hit['_source'].get('multipliers', 0)
                }
                for hit in response.hits.hits
            ]

        except Exception as e:
            self.logger.error(f"Error getting band activity: {e}")
            self.logger.error(traceback.format_exc())
            return []

    def get_contest_summary(self, contest: str) -> Dict[str, Any]:
        """Get summary statistics for a contest"""
        try:
            query = f'SELECT COUNT(*) as participants, ' \
                    f'MAX(score) as top_score, ' \
                    f'SUM(qsos) as total_qsos, ' \
                    f'AVG(score) as avg_score ' \
                    f'FROM rt_contest_scores ' \
                    f'WHERE contest = "{contest}"'

            response = self.utils_api.sql(query)
            if not response or not hasattr(response, 'hits') or not response.hits.hits:
                return {}

            stats = response.hits.hits[0]['_source']
            return {
                'participants': stats.get('participants', 0),
                'top_score': stats.get('top_score', 0),
                'total_qsos': stats.get('total_qsos', 0),
                'avg_score': int(stats.get('avg_score', 0))
            }

        except Exception as e:
            self.logger.error(f"Error getting contest summary: {e}")
            self.logger.error(traceback.format_exc())
            return {}

    def get_callsign_history(self, callsign: str) -> List[Dict[str, Any]]:
        """Get contest history for a specific callsign"""
        try:
            query = f'SELECT contest, score, qsos, timestamp ' \
                    f'FROM rt_contest_scores ' \
                    f'WHERE callsign = "{callsign}" ' \
                    f'ORDER BY timestamp DESC'

            response = self.utils_api.sql(query)
            if not response or not hasattr(response, 'hits'):
                return []

            return [
                {
                    'contest': hit['_source']['contest'],
                    'score': hit['_source']['score'],
                    'qsos': hit['_source'].get('qsos', 0),
                    'timestamp': hit['_source'].get('timestamp', '')
                }
                for hit in response.hits.hits
            ]

        except Exception as e:
            self.logger.error(f"Error getting callsign history: {e}")
            self.logger.error(traceback.format_exc())
            return []
