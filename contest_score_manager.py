# contest_score_manager.py
import sqlite3
import logging
from callsign_utils import CallsignLookup
from sql_queries import (INSERT_SCORE, INSERT_SCORE_QTH, INSERT_SCORE_BAND)

class ContestScoreManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.callsign_lookup = CallsignLookup()
        self.logger = logging.getLogger('ContestScoreManager')

    def insert_score(self, score_data):
        """
        Insert contest score with QTH info and band breakdown
        score_data: dictionary containing all score information
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Insert main score data
                cursor.execute(INSERT_SCORE, (
                    score_data['timestamp'],
                    score_data['contest'],
                    score_data['callsign'],
                    score_data['power'],
                    score_data['assisted'],
                    score_data['transmitter'],
                    score_data['ops'],
                    score_data['bands'],
                    score_data['mode'],
                    score_data['overlay'],
                    score_data['club'],
                    score_data['section'],
                    score_data['score'],
                    score_data['qsos'],
                    score_data['multipliers'],
                    score_data['points']
                ))
                
                contest_score_id = cursor.lastrowid
                
                # Insert QTH info
                callsign_info = self.callsign_lookup.get_callsign_info(score_data['callsign'])
                if callsign_info:
                    cursor.execute(INSERT_SCORE_QTH, (
                        contest_score_id,
                        callsign_info['prefix'],
                        callsign_info['continent'],
                        str(callsign_info['cq_zone']),
                        str(callsign_info['itu_zone']),
                        score_data.get('arrl_section', ''),
                        score_data.get('state_province', ''),
                        score_data.get('grid6', '')
                    ))

                # Insert band breakdown if present
                if 'band_breakdown' in score_data:
                    for band_data in score_data['band_breakdown']:
                        cursor.execute(INSERT_SCORE_BAND, (
                            contest_score_id,
                            band_data['band'],
                            band_data['mode'],
                            band_data['qsos'],
                            band_data['points'],
                            band_data['multipliers']
                        ))
                
                conn.commit()
                return contest_score_id
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error in insert_score: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error in insert_score: {e}")
            raise
