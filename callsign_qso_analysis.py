# callsign_qso_analysis.py

import sqlite3
import argparse
import logging
from tabulate import tabulate
from datetime import datetime

def analyze_callsign_patterns(db_path, callsign=None):
    """Analyze patterns for specific callsigns with QSO count issues"""
    query = """
    WITH inconsistent_records AS (
        -- Find records where total QSOs don't match band breakdown
        SELECT 
            cs.id,
            cs.callsign,
            cs.contest,
            cs.timestamp,
            cs.qsos as reported_total,
            (SELECT SUM(bb.qsos) FROM band_breakdown bb 
             WHERE bb.contest_score_id = cs.id) as band_total,
            GROUP_CONCAT(bb.band || ':' || bb.qsos) as band_breakdown
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        WHERE cs.qsos != (
            SELECT SUM(bb2.qsos) FROM band_breakdown bb2 
            WHERE bb2.contest_score_id = cs.id
        )
        {callsign_filter}
        GROUP BY cs.id, cs.callsign, cs.contest, cs.timestamp, cs.qsos
        ORDER BY cs.timestamp DESC
    )
    SELECT 
        callsign,
        contest,
        timestamp,
        reported_total,
        band_total,
        ROUND(CAST(band_total AS FLOAT) / reported_total, 2) as ratio,
        band_breakdown
    FROM inconsistent_records
    ORDER BY callsign, timestamp DESC
    """
    
    callsign_filter = ""
    params = ()
    if callsign:
        callsign_filter = "AND cs.callsign = ?"
        params = (callsign,)
    
    query = query.format(callsign_filter=callsign_filter)
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            print("\nAnalyzing QSO Count Patterns:")
            headers = ['Callsign', 'Contest', 'Timestamp', 'Reported QSOs', 
                      'Band Total', 'Ratio', 'Band Breakdown']
            print(tabulate(results[:50], headers=headers, tablefmt='grid'))
            
            # Analyze ratios
            ratios = {}
            for row in results:
                ratio = row[5]  # Ratio column
                ratios[ratio] = ratios.get(ratio, 0) + 1
            
            print("\nRatio Distribution:")
            ratio_data = [(ratio, count) for ratio, count in ratios.items()]
            ratio_data.sort(key=lambda x: x[1], reverse=True)
            print(tabulate(ratio_data, headers=['Ratio', 'Count'], tablefmt='grid'))
            
            # Most common case
            most_common_ratio = ratio_data[0][0] if ratio_data else None
            if most_common_ratio:
                print(f"\nMost common ratio: {most_common_ratio}")
                
            return results
            
    except Exception as e:
        print(f"Error analyzing patterns: {e}")
        return None

def analyze_band_patterns(db_path, callsign=None):
    """Analyze how QSOs are distributed across bands"""
    query = """
    WITH inconsistent_records AS (
        SELECT cs.id, cs.callsign, cs.contest, cs.qsos as total_qsos
        FROM contest_scores cs
        WHERE cs.qsos != (
            SELECT SUM(bb.qsos) FROM band_breakdown bb 
            WHERE bb.contest_score_id = cs.id
        )
        {callsign_filter}
    )
    SELECT 
        ir.callsign,
        ir.contest,
        bb.band,
        COUNT(*) as entry_count,
        SUM(bb.qsos) as total_band_qsos,
        GROUP_CONCAT(DISTINCT bb.mode) as modes
    FROM inconsistent_records ir
    JOIN band_breakdown bb ON bb.contest_score_id = ir.id
    GROUP BY ir.callsign, ir.contest, bb.band
    ORDER BY entry_count DESC
    """
    
    callsign_filter = ""
    params = ()
    if callsign:
        callsign_filter = "AND cs.callsign = ?"
        params = (callsign,)
    
    query = query.format(callsign_filter=callsign_filter)
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            print("\nBand Distribution Analysis:")
            headers = ['Callsign', 'Contest', 'Band', 'Entry Count', 
                      'Total QSOs', 'Modes']
            print(tabulate(results, headers=headers, tablefmt='grid'))
            
            return results
            
    except Exception as e:
        print(f"Error analyzing band patterns: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(
        description='Analyze QSO inconsistencies for specific callsigns'
    )
    parser.add_argument('--db', required=True, help='Database file path')
    parser.add_argument('--call', help='Specific callsign to analyze')
    
    args = parser.parse_args()
    
    print(f"Analyzing {'all problematic callsigns' if not args.call else args.call}")
    analyze_callsign_patterns(args.db, args.call)
    analyze_band_patterns(args.db, args.call)

if __name__ == "__main__":
    main()
