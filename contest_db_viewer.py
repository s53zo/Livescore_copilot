#!/usr/bin/env python3
import sqlite3
import logging
import sys
from datetime import datetime
from tabulate import tabulate
from display_utils import format_band_stats, format_scores, format_band_breakdown

class ContestDatabaseViewer:
    def __init__(self, db_path, debug=False):
        self.db_path = db_path
        self.debug = debug
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration """
        log_level = logging.DEBUG if self.debug else logging.INFO
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger = logging.getLogger('ContestDBViewer')
        self.logger.setLevel(log_level)
        self.logger.addHandler(console_handler)

    def connect_db(self):
        """Connect to the database"""
        try:
            self.logger.debug(f"Connecting to database: {self.db_path}")
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            sys.exit(1)

    def check_callsign_exists(self, callsign):
        """Check if callsign exists in database"""
        query = "SELECT COUNT(*) FROM contest_scores WHERE callsign = ?"
        
        self.logger.debug(f"Checking if callsign exists: {callsign}")
        
        with self.connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (callsign,))
            count = cursor.fetchone()[0]
            
            if count == 0:
                cursor.execute("SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
                available_calls = [row[0] for row in cursor.fetchall()]
                
                print(f"No records found for callsign: {callsign}")
                print("\nAvailable callsigns in database:")
                col_width = max(len(call) for call in available_calls) + 2
                cols = max(1, 80 // col_width)
                for i in range(0, len(available_calls), cols):
                    print("".join(call.ljust(col_width) for call in available_calls[i:i+cols]))
                return False
            return True

    def get_available_contests(self):
        """Get list of all contests in database"""
        query = """
            SELECT DISTINCT contest 
            FROM contest_scores 
            ORDER BY contest
        """
        with self.connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]

    def get_contest_stats(self):
        """Get various statistics from the database"""
        stats_queries = {
            "total_stats": """
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest, cs.qsos
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
                SELECT 
                    COUNT(DISTINCT callsign) as unique_stations,
                    COUNT(DISTINCT contest) as contests,
                    (SELECT COUNT(*) FROM contest_scores) as total_uploads,
                    SUM(qsos) as total_qsos
                FROM latest_scores
            """,
            "contest_counts": """
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest, cs.score, cs.qsos, cs.timestamp
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
                SELECT 
                    contest,
                    COUNT(DISTINCT callsign) as participants,
                    COUNT(*) as total_uploads,
                    MIN(timestamp) as first_upload,
                    MAX(timestamp) as last_upload,
                    MAX(score) as highest_score,
                    SUM(qsos) as total_qsos
                FROM latest_scores
                GROUP BY contest
                ORDER BY last_upload DESC
            """,
            "band_stats": """
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
                SELECT 
                    cs.contest,
                    COUNT(DISTINCT cs.callsign) as stations,
                    SUM(CASE WHEN bb.band = '160' THEN bb.qsos ELSE 0 END) as '160_qsos',
                    SUM(CASE WHEN bb.band = '160' THEN bb.points ELSE 0 END) as '160_points',
                    SUM(CASE WHEN bb.band = '160' THEN bb.multipliers ELSE 0 END) as '160_mults',
                    SUM(CASE WHEN bb.band = '80' THEN bb.qsos ELSE 0 END) as '80_qsos',
                    SUM(CASE WHEN bb.band = '80' THEN bb.points ELSE 0 END) as '80_points',
                    SUM(CASE WHEN bb.band = '80' THEN bb.multipliers ELSE 0 END) as '80_mults',
                    SUM(CASE WHEN bb.band = '40' THEN bb.qsos ELSE 0 END) as '40_qsos',
                    SUM(CASE WHEN bb.band = '40' THEN bb.points ELSE 0 END) as '40_points',
                    SUM(CASE WHEN bb.band = '40' THEN bb.multipliers ELSE 0 END) as '40_mults',
                    SUM(CASE WHEN bb.band = '20' THEN bb.qsos ELSE 0 END) as '20_qsos',
                    SUM(CASE WHEN bb.band = '20' THEN bb.points ELSE 0 END) as '20_points',
                    SUM(CASE WHEN bb.band = '20' THEN bb.multipliers ELSE 0 END) as '20_mults',
                    SUM(CASE WHEN bb.band = '15' THEN bb.qsos ELSE 0 END) as '15_qsos',
                    SUM(CASE WHEN bb.band = '15' THEN bb.points ELSE 0 END) as '15_points',
                    SUM(CASE WHEN bb.band = '15' THEN bb.multipliers ELSE 0 END) as '15_mults',
                    SUM(CASE WHEN bb.band = '10' THEN bb.qsos ELSE 0 END) as '10_qsos',
                    SUM(CASE WHEN bb.band = '10' THEN bb.points ELSE 0 END) as '10_points',
                    SUM(CASE WHEN bb.band = '10' THEN bb.multipliers ELSE 0 END) as '10_mults'
                FROM latest_scores ls
                JOIN contest_scores cs ON cs.id = ls.id
                LEFT JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                GROUP BY cs.contest
                ORDER BY cs.contest
            """
        }

        stats = {}
        with self.connect_db() as conn:
            cursor = conn.cursor()
            for stat_name, query in stats_queries.items():
                cursor.execute(query)
                if stat_name == "total_stats":
                    stats[stat_name] = cursor.fetchone()
                else:
                    stats[stat_name] = cursor.fetchall()
        return stats

    def get_contest_scores(self, sort_by='t', sort_order='DESC', limit=None, latest=False, contest=None):
        """Retrieve contest scores from database"""
        valid_sort_fields = {
            't': 'timestamp',
            'c': 'callsign',
            'n': 'contest',
            's': 'score',
            'q': 'qsos',
            'u': 'club',
            'e': 'section',
            'p': 'power'
        }

        if sort_by not in valid_sort_fields:
            self.logger.error(f"Invalid sort field: {sort_by}")
            self.logger.info(f"Valid sort fields are: {', '.join(valid_sort_fields.keys())}")
            sys.exit(1)

        sort_order = sort_order.upper()
        if sort_order not in ['ASC', 'DESC']:
            self.logger.error(f"Invalid sort order: {sort_order}")
            self.logger.info("Valid sort orders are: ASC, DESC")
            sys.exit(1)

        # Base query using CTE to get latest scores
        base_query = """
            WITH latest_scores AS (
                SELECT DISTINCT ON (cs.callsign, cs.contest) 
                    cs.timestamp,
                    cs.contest,
                    cs.callsign,
                    cs.power,
                    cs.score,
                    cs.qsos,
                    cs.multipliers,
                    cs.club,
                    cs.section,
                    cs.assisted,
                    cs.mode
                FROM contest_scores cs
                WHERE 1=1
                {contest_filter}
                ORDER BY cs.callsign, cs.contest, cs.timestamp DESC
            )
            SELECT * FROM latest_scores
            ORDER BY {sort_field} {sort_order}
            {limit_clause}
        """

        contest_filter = " AND contest = ?" if contest else ""
        limit_clause = f" LIMIT {limit}" if limit else ""
        
        query = base_query.format(
            contest_filter=contest_filter,
            sort_field=valid_sort_fields[sort_by],
            sort_order=sort_order,
            limit_clause=limit_clause
        )
        
        self.logger.debug(f"Executing query: {query}")
        
        with self.connect_db() as conn:
            cursor = conn.cursor()
            if contest:
                cursor.execute(query, (contest,))
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            self.logger.debug(f"Retrieved {len(results)} records")
            return results 

    def get_band_breakdown(self, callsign=None, contest=None):
        """Retrieve band breakdown for specific callsign or all"""
        if callsign and not self.check_callsign_exists(callsign):
            return None

        # Main query using the latest timestamps and DISTINCT values
        query = """
            WITH latest_scores AS (
                SELECT cs.id, cs.callsign, cs.contest, cs.timestamp
                FROM contest_scores cs
                INNER JOIN (
                    SELECT callsign, contest, MAX(timestamp) as max_ts
                    FROM contest_scores
                    GROUP BY callsign, contest
                ) latest ON cs.callsign = latest.callsign 
                    AND cs.contest = latest.contest
                    AND cs.timestamp = latest.max_ts
            )
            SELECT DISTINCT
                cs.callsign,
                cs.contest,
                cs.timestamp,
                bb.band,
                bb.mode,
                bb.qsos,
                bb.points,
                bb.multipliers
            FROM contest_scores cs
            JOIN latest_scores ls ON cs.id = ls.id
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE bb.band IS NOT NULL
            AND bb.qsos > 0  -- Only include bands with QSOs
        """
        
        params = []
        if callsign:
            query += " AND cs.callsign = ?"
            params.append(callsign)
        if contest:
            query += " AND cs.contest = ?"
            params.append(contest)
        
        query += " ORDER BY cs.callsign, bb.band"
        
        self.logger.debug(f"Executing band breakdown query: {query}")
        self.logger.debug(f"Query parameters: {params}")
        
        with self.connect_db() as conn:
            cursor = conn.cursor()
            
            if callsign:
                contests = self.get_callsign_contests(callsign)
                if contests:
                    print("\nAvailable contests for", callsign + ":")
                    for contest, timestamp in contests:
                        ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                        print(f"{contest} ({ts})")
                    print()

            cursor.execute(query, params)
            results = cursor.fetchall()
            self.logger.debug(f"Retrieved {len(results)} band breakdown records")
            return results
    
    
    def display_stats(self, stats):
        format_band_stats(stats)

    def display_scores(self, data, show_all=False):
        format_scores(data, show_all)

    def display_band_breakdown(self, data):
        format_band_breakdown(data)

    def get_callsign_contests(self, callsign):
        """Get list of contests for a callsign"""
        query = """
            SELECT DISTINCT contest, timestamp 
            FROM contest_scores 
            WHERE callsign = ?
            ORDER BY timestamp DESC
        """
        with self.connect_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (callsign,))
            return cursor.fetchall()
