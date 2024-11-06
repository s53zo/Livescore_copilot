#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback  # Add this import
from datetime import datetime

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_minutes = rate_minutes
        self._setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}, Rate interval: {rate_minutes} minutes")

    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None, category_filter='same'):
        """Get station details and nearby competitors with optional filtering"""
        self.logger.debug(f"get_station_details called with: callsign={callsign}, contest={contest}")
        self.logger.debug(f"Filters: type={filter_type}, value={filter_value}, category={category_filter}")

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # First verify the station exists
                cursor.execute("""
                    SELECT COUNT(*), MAX(timestamp)
                    FROM contest_scores
                    WHERE callsign = ? AND contest = ?
                """, (callsign, contest))
                count, last_update = cursor.fetchone()
                self.logger.debug(f"Initial check: Found {count} records, last update: {last_update}")
                
                if count == 0:
                    self.logger.error(f"No records found for {callsign} in {contest}")
                    return None

                # Base query structure
                query = """
                    WITH StationScore AS (
                        SELECT 
                            cs.id, 
                            cs.callsign, 
                            cs.score, 
                            cs.power, 
                            cs.assisted,
                            cs.timestamp, 
                            cs.qsos, 
                            cs.multipliers,
                            'current' as position,
                            1 as rn
                        FROM contest_scores cs
                        WHERE cs.callsign = ? 
                        AND cs.contest = ?
                        ORDER BY cs.timestamp DESC
                        LIMIT 1
                    ),
                    LatestScores AS (
                        SELECT cs.id, cs.callsign, cs.score, cs.power, cs.assisted,
                               cs.timestamp, cs.qsos, cs.multipliers,
                               qi.dxcc_country, qi.cq_zone, qi.iaru_zone
                        FROM contest_scores cs
                        JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
                        WHERE cs.contest = ?
                        {filter_clause}
                    )
                    SELECT 
                        id,
                        callsign, 
                        score, 
                        power, 
                        assisted,
                        timestamp, 
                        qsos, 
                        multipliers,
                        position,
                        rn
                    FROM (
                        SELECT * FROM StationScore
                        UNION ALL
                        SELECT 
                            ls.id,
                            ls.callsign, 
                            ls.score, 
                            ls.power, 
                            ls.assisted,
                            ls.timestamp, 
                            ls.qsos, 
                            ls.multipliers,
                            CASE
                                WHEN ls.score > ss.score THEN 'above'
                                WHEN ls.score < ss.score THEN 'below'
                            END as position,
                            ROW_NUMBER() OVER (
                                PARTITION BY 
                                    CASE
                                        WHEN ls.score > ss.score THEN 'above'
                                        WHEN ls.score < ss.score THEN 'below'
                                    END
                                ORDER BY 
                                    CASE
                                        WHEN ls.score > ss.score THEN score END ASC,
                                    CASE
                                        WHEN ls.score < ss.score THEN score END DESC
                            ) as rn
                        FROM LatestScores ls
                        CROSS JOIN StationScore ss
                        WHERE ls.callsign != ss.callsign
                        {category_clause}
                    )
                    ORDER BY score DESC;
                """
                
                params = [callsign, contest, contest, contest]
                filter_clause = ""
                category_clause = ""
                
                # Add category filtering if requested
                if category_filter == 'same':
                    category_clause = """
                        AND ls.power = ss.power
                        AND ls.assisted = ss.assisted
                    """
                
                # Add location filtering if specified
                if filter_type and filter_value and str(filter_value).strip():
                    if filter_type == 'dxcc':
                        filter_clause = "AND qi.dxcc_country = ?"
                        params.append(filter_value.strip())
                    elif filter_type == 'cq_zone':
                        filter_clause = "AND CAST(qi.cq_zone AS TEXT) = ?"
                        params.append(str(filter_value).strip())
                    elif filter_type == 'iaru_zone':
                        filter_clause = "AND CAST(qi.iaru_zone AS TEXT) = ?"
                        params.append(str(filter_value).strip())
                
                # Format the query with the appropriate clauses
                formatted_query = query.format(
                    filter_clause=filter_clause,
                    category_clause=category_clause
                )
                
                self.logger.debug(f"Executing query with params: {params}")
                self.logger.debug(f"Category filter: {category_filter}")
                self.logger.debug(f"Category clause: {category_clause}")
                
                cursor.execute(formatted_query, params)
                stations = cursor.fetchall()
                
                if not stations:
                    self.logger.error("Main query returned no results")
                    self.logger.debug("Query used:")
                    self.logger.debug(formatted_query)
                    self.logger.debug("Parameters:")
                    self.logger.debug(params)
                    return None
                
                self.logger.debug(f"Found {len(stations)} stations")
                for station in stations:
                    self.logger.debug(f"Station: {station}")
                
                return stations
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.logger.error(traceback.format_exc())
            return None
