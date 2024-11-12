# qso_diagnostics.py

import sqlite3
import argparse
import logging
import sys
from tabulate import tabulate
from datetime import datetime

class QsoDiagnostics:
    def __init__(self, db_path, log_path=None):
        self.db_path = db_path
        self.setup_logging(log_path)
        
    def setup_logging(self, log_path=None):
        """Configure logging to both file and console"""
        self.logger = logging.getLogger('QsoDiagnostics')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        if log_path:
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def check_duplicate_entries(self):
        """Check for duplicate entries in band_breakdown"""
        self.logger.info("Checking for duplicate band_breakdown entries...")
        
        query = """
        WITH duplicates AS (
            SELECT 
                cs.id,
                cs.callsign,
                cs.contest,
                bb.band,
                bb.mode,
                COUNT(*) as entry_count,
                SUM(bb.qsos) as total_qsos
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            GROUP BY cs.id, cs.callsign, cs.contest, bb.band, bb.mode
            HAVING COUNT(*) > 1
        )
        SELECT * FROM duplicates
        ORDER BY entry_count DESC, callsign, contest
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    self.logger.info(f"Found {len(results)} duplicate entries")
                    headers = ['ID', 'Callsign', 'Contest', 'Band', 'Mode', 'Entry Count', 'Total QSOs']
                    print("\nDuplicate Band/Mode Entries:")
                    print(tabulate(results, headers=headers, tablefmt='grid'))
                else:
                    self.logger.info("No duplicate band/mode entries found")
                    
        except Exception as e:
            self.logger.error(f"Error checking duplicates: {e}")

    def analyze_band_distribution(self):
        """Analyze QSO distribution across bands for inconsistent records"""
        self.logger.info("Analyzing QSO distribution across bands...")
        
        query = """
        WITH inconsistent_scores AS (
            SELECT cs.id, cs.callsign, cs.contest, cs.qsos as total_qsos,
                   (SELECT SUM(bb2.qsos) FROM band_breakdown bb2 
                    WHERE bb2.contest_score_id = cs.id) as band_total
            FROM contest_scores cs
            WHERE cs.qsos != (
                SELECT SUM(bb1.qsos) FROM band_breakdown bb1 
                WHERE bb1.contest_score_id = cs.id
            )
        )
        SELECT 
            is2.id,
            is2.callsign,
            is2.contest,
            is2.total_qsos,
            bb.band,
            bb.mode,
            bb.qsos as band_qsos,
            ROUND(CAST(bb.qsos AS FLOAT) / is2.total_qsos * 100, 2) as percentage
        FROM inconsistent_scores is2
        JOIN band_breakdown bb ON bb.contest_score_id = is2.id
        ORDER BY is2.id, bb.band
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    headers = ['ID', 'Callsign', 'Contest', 'Total QSOs', 
                             'Band', 'Mode', 'Band QSOs', '% of Total']
                    print("\nBand Distribution for Inconsistent Records:")
                    print(tabulate(results, headers=headers, tablefmt='grid'))
                    
                    # Calculate statistics
                    contests = set((r[2] for r in results))
                    print(f"\nFound inconsistencies in {len(contests)} contests")
                    
        except Exception as e:
            self.logger.error(f"Error analyzing band distribution: {e}")

    def analyze_contest_patterns(self):
        """Look for patterns in specific contests"""
        self.logger.info("Analyzing contest-specific patterns...")
        
        query = """
        WITH contest_stats AS (
            SELECT 
                cs.contest,
                COUNT(DISTINCT cs.id) as total_entries,
                COUNT(DISTINCT CASE 
                    WHEN cs.qsos != (SELECT SUM(bb1.qsos) FROM band_breakdown bb1 
                                   WHERE bb1.contest_score_id = cs.id)
                    THEN cs.id 
                    END) as inconsistent_entries,
                AVG(CASE 
                    WHEN cs.qsos != (SELECT SUM(bb1.qsos) FROM band_breakdown bb1 
                                   WHERE bb1.contest_score_id = cs.id)
                    THEN CAST((SELECT SUM(bb1.qsos) FROM band_breakdown bb1 
                             WHERE bb1.contest_score_id = cs.id) AS FLOAT) / cs.qsos 
                    END) as avg_ratio
            FROM contest_scores cs
            GROUP BY cs.contest
            HAVING inconsistent_entries > 0
        )
        SELECT 
            contest,
            total_entries,
            inconsistent_entries,
            ROUND(CAST(inconsistent_entries AS FLOAT) / total_entries * 100, 2) as pct_inconsistent,
            ROUND(avg_ratio, 2) as avg_qso_ratio
        FROM contest_stats
        ORDER BY inconsistent_entries DESC
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    headers = ['Contest', 'Total Entries', 'Inconsistent', '% Inconsistent', 'Avg QSO Ratio']
                    print("\nContest Pattern Analysis:")
                    print(tabulate(results, headers=headers, tablefmt='grid'))
                    
                    # Analyze patterns
                    high_ratio_contests = [r for r in results if r[4] >= 2.0]
                    if high_ratio_contests:
                        print("\nContests with QSO ratios >= 2.0 (possible double counting):")
                        print(tabulate(high_ratio_contests, headers=headers, tablefmt='grid'))
                        
        except Exception as e:
            self.logger.error(f"Error analyzing contest patterns: {e}")

    def check_logging_software(self):
        """Analyze if inconsistencies are related to specific logging software"""
        self.logger.info("Checking for logging software patterns...")
        
        # Note: This assumes there's some way to identify the logging software
        # You might need to modify this based on your actual data structure
        query = """
        WITH inconsistent_scores AS (
            SELECT cs.id, cs.callsign, cs.contest,
                   cs.qsos as total_qsos,
                   (SELECT SUM(bb.qsos) FROM band_breakdown bb 
                    WHERE bb.contest_score_id = cs.id) as band_total
            FROM contest_scores cs
            WHERE cs.qsos != (
                SELECT SUM(bb.qsos) FROM band_breakdown bb 
                WHERE bb.contest_score_id = cs.id
            )
        )
        SELECT DISTINCT
            cs.callsign,
            cs.contest,
            cs.timestamp,
            cs.qsos as reported_qsos,
            (SELECT SUM(bb.qsos) FROM band_breakdown bb 
             WHERE bb.contest_score_id = cs.id) as actual_qsos
        FROM inconsistent_scores is2
        JOIN contest_scores cs ON cs.id = is2.id
        ORDER BY cs.timestamp DESC
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    headers = ['Callsign', 'Contest', 'Timestamp', 'Reported QSOs', 'Actual QSOs']
                    print("\nInconsistent Score Timeline:")
                    print(tabulate(results, headers=headers, tablefmt='grid'))
                    
                    # Analyze submission patterns
                    callsign_counts = {}
                    for r in results:
                        callsign_counts[r[0]] = callsign_counts.get(r[0], 0) + 1
                    
                    print("\nCallsigns with Multiple Inconsistencies:")
                    frequent_issues = [(call, count) for call, count in callsign_counts.items() 
                                     if count > 1]
                    if frequent_issues:
                        print(tabulate(frequent_issues, 
                                     headers=['Callsign', 'Issue Count'], 
                                     tablefmt='grid'))
                    
        except Exception as e:
            self.logger.error(f"Error checking logging software patterns: {e}")

def main():
    parser = argparse.ArgumentParser(
        description='Diagnose QSO count inconsistencies in contest database'
    )
    parser.add_argument('--db', required=True,
                      help='Database file path')
    parser.add_argument('--log',
                      help='Log file path')
    parser.add_argument('--all', action='store_true',
                      help='Run all diagnostics')
    parser.add_argument('--duplicates', action='store_true',
                      help='Check for duplicate band_breakdown entries')
    parser.add_argument('--bands', action='store_true',
                      help='Analyze QSO distribution across bands')
    parser.add_argument('--contests', action='store_true',
                      help='Analyze contest-specific patterns')
    parser.add_argument('--logging', action='store_true',
                      help='Check for logging software patterns')
    
    args = parser.parse_args()
    
    try:
        diagnostics = QsoDiagnostics(args.db, args.log)
        
        if args.all or args.duplicates:
            diagnostics.check_duplicate_entries()
        
        if args.all or args.bands:
            diagnostics.analyze_band_distribution()
            
        if args.all or args.contests:
            diagnostics.analyze_contest_patterns()
            
        if args.all or args.logging:
            diagnostics.check_logging_software()
            
    except Exception as e:
        print(f"Error running diagnostics: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())
