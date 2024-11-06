#!/usr/bin/env python3
import sqlite3
import argparse
from datetime import datetime, timedelta

def calculate_rates(db_path, callsign, contest):
    """Calculate 1-hour and 15-minute QSO rates for a given station"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get all submissions for this station in chronological order
            cursor.execute("""
                SELECT 
                    timestamp,
                    qsos,
                    band_data.band_breakdown
                FROM contest_scores cs
                LEFT JOIN (
                    SELECT 
                        contest_score_id,
                        GROUP_CONCAT(band || ':' || qsos) as band_breakdown
                    FROM band_breakdown
                    GROUP BY contest_score_id
                ) band_data ON band_data.contest_score_id = cs.id
                WHERE callsign = ? 
                AND contest = ?
                ORDER BY timestamp
            """, (callsign, contest))
            
            records = cursor.fetchall()
            
            if not records:
                print(f"No records found for {callsign} in {contest}")
                return
            
            print(f"\nRate analysis for {callsign} in {contest}")
            print("-" * 50)
            
            # Process each record
            for i in range(len(records)):
                current = records[i]
                current_ts = datetime.strptime(current[0], '%Y-%m-%d %H:%M:%S')
                current_qsos = current[1]
                current_bands = parse_band_breakdown(current[2]) if current[2] else {}
                
                print(f"\nTimestamp: {current_ts}")
                print(f"Total QSOs: {current_qsos}")
                print("Band breakdown:", current_bands)
                
                # Find records within last hour and 15 minutes
                hour_ago = current_ts - timedelta(hours=1)
                quarter_ago = current_ts - timedelta(minutes=15)
                
                # Find closest previous record within these intervals
                hour_record = find_previous_record(records, i, hour_ago)
                quarter_record = find_previous_record(records, i, quarter_ago)
                
                # Calculate rates
                if hour_record:
                    hour_ts = datetime.strptime(hour_record[0], '%Y-%m-%d %H:%M:%S')
                    hour_diff = (current_ts - hour_ts).total_seconds() / 3600  # in hours
                    hour_qso_diff = current_qsos - hour_record[1]
                    hour_rate = int(round(hour_qso_diff / hour_diff))
                    
                    # Calculate band differences
                    hour_bands = parse_band_breakdown(hour_record[2]) if hour_record[2] else {}
                    print(f"\n1-hour rate: {hour_rate}/hr")
                    print(f"Time span: {hour_diff:.2f} hours")
                    print(f"QSOs in span: {hour_qso_diff}")
                    print("Band changes:")
                    for band in sorted(set(current_bands.keys()) | set(hour_bands.keys())):
                        diff = current_bands.get(band, 0) - hour_bands.get(band, 0)
                        if diff != 0:
                            print(f"  {band}m: +{diff}")
                
                if quarter_record:
                    quarter_ts = datetime.strptime(quarter_record[0], '%Y-%m-%d %H:%M:%S')
                    quarter_diff = (current_ts - quarter_ts).total_seconds() / 3600  # in hours
                    quarter_qso_diff = current_qsos - quarter_record[1]
                    quarter_rate = int(round(quarter_qso_diff / quarter_diff))
                    
                    # Calculate band differences
                    quarter_bands = parse_band_breakdown(quarter_record[2]) if quarter_record[2] else {}
                    print(f"\n15-minute rate: {quarter_rate}/hr")
                    print(f"Time span: {quarter_diff:.2f} hours")
                    print(f"QSOs in span: {quarter_qso_diff}")
                    print("Band changes:")
                    for band in sorted(set(current_bands.keys()) | set(quarter_bands.keys())):
                        diff = current_bands.get(band, 0) - quarter_bands.get(band, 0)
                        if diff != 0:
                            print(f"  {band}m: +{diff}")
                
                print("-" * 50)
                
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def find_previous_record(records, current_idx, target_time):
    """Find the closest previous record that's after target_time"""
    current_ts = datetime.strptime(records[current_idx][0], '%Y-%m-%d %H:%M:%S')
    
    for i in range(current_idx - 1, -1, -1):
        record_ts = datetime.strptime(records[i][0], '%Y-%m-%d %H:%M:%S')
        if record_ts >= target_time:
            return records[i]
    return None

def parse_band_breakdown(band_str):
    """Parse band breakdown string into a dictionary"""
    if not band_str:
        return {}
        
    bands = {}
    for item in band_str.split(','):
        try:
            band, qsos = item.split(':')
            bands[band] = int(qsos)
        except ValueError:
            continue
    return bands

def main():
    parser = argparse.ArgumentParser(description='Calculate contest QSO rates')
    parser.add_argument('--db', default='contest_data.db',
                      help='Database file path (default: contest_data.db)')
    parser.add_argument('--call', required=True,
                      help='Callsign to analyze')
    parser.add_argument('--contest', required=True,
                      help='Contest name')
    
    args = parser.parse_args()
    
    calculate_rates(args.db, args.call.upper(), args.contest)

if __name__ == "__main__":
    main()
  
