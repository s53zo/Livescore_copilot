#!/usr/bin/env python3
import sqlite3
import argparse
from datetime import datetime, timedelta

def calculate_rates(db_path, callsign, contest):
    """Calculate 1-hour and 15-minute QSO rates for a given station"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get entries in the last hour and 15 minutes relative to now
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
                AND timestamp >= datetime('now', '-65 minutes')  -- Get a bit more for accurate closest match
                ORDER BY timestamp DESC
            """, (callsign, contest))
            
            records = cursor.fetchall()
            
            if not records:
                print(f"No recent records found for {callsign} in {contest} in the last hour")
                return

            now = datetime.utcnow()
            hour_ago = now - timedelta(hours=1)
            quarter_ago = now - timedelta(minutes=15)

            # Find records closest to now, hour ago, and quarter ago
            latest = None
            hour_record = None
            quarter_record = None
            min_hour_diff = float('inf')
            min_quarter_diff = float('inf')

            for record in records:
                record_ts = datetime.strptime(record[0], '%Y-%m-%d %H:%M:%S')
                
                # Always update latest if it's more recent
                if not latest or record_ts > datetime.strptime(latest[0], '%Y-%m-%d %H:%M:%S'):
                    latest = record

                # Find closest to an hour ago
                hour_diff = abs((record_ts - hour_ago).total_seconds())
                if hour_diff < min_hour_diff:
                    hour_record = record
                    min_hour_diff = hour_diff

                # Find closest to 15 minutes ago
                quarter_diff = abs((record_ts - quarter_ago).total_seconds())
                if quarter_diff < min_quarter_diff:
                    quarter_record = record
                    min_quarter_diff = quarter_diff

            if not latest:
                print("No records found")
                return

            latest_ts = datetime.strptime(latest[0], '%Y-%m-%d %H:%M:%S')
            latest_qsos = latest[1]
            latest_bands = parse_band_breakdown(latest[2]) if latest[2] else {}

            print(f"\nRate analysis for {callsign} in {contest}")
            print(f"Latest entry: {latest_ts} with {latest_qsos} QSOs")
            print(f"Current time: {now}")
            print(f"Time since last update: {(now - latest_ts).total_seconds() / 60:.1f} minutes")
            print("-" * 50)

            # If latest entry is more than 65 minutes old, rates should be 0
            if (now - latest_ts).total_seconds() / 3600 > 1.1:
                print("\nNo recent activity (last entry more than an hour old)")
                return

            # Calculate 1-hour rate
            if hour_record:
                hour_ts = datetime.strptime(hour_record[0], '%Y-%m-%d %H:%M:%S')
                hour_qsos = hour_record[1]
                hour_bands = parse_band_breakdown(hour_record[2]) if hour_record[2] else {}
                
                time_diff = (latest_ts - hour_ts).total_seconds() / 60
                qso_diff = latest_qsos - hour_qsos
                
                print("\n1-hour rate calculation:")
                print(f"From: {hour_ts}")
                print(f"To:   {latest_ts}")
                print(f"Time span: {time_diff:.1f} minutes")
                print(f"QSOs in span: {qso_diff}")
                
                if qso_diff == 0:
                    print("No QSO changes - rate is 0/hr")
                else:
                    hour_rate = int(round((qso_diff * 60) / time_diff))
                    print(f"Interpolated 60-minute rate: {hour_rate}/hr")

            # Calculate 15-minute rate (similar to 1-hour rate)
            if quarter_record:
                # Similar calculation for 15-minute rate...
                quarter_ts = datetime.strptime(quarter_record[0], '%Y-%m-%d %H:%M:%S')
                quarter_qsos = quarter_record[1]
                
                time_diff = (latest_ts - quarter_ts).total_seconds() / 60
                qso_diff = latest_qsos - quarter_qsos
                
                print("\n15-minute rate calculation:")
                print(f"From: {quarter_ts}")
                print(f"To:   {latest_ts}")
                print(f"Time span: {time_diff:.1f} minutes")
                print(f"QSOs in span: {qso_diff}")
                
                if qso_diff == 0:
                    print("No QSO changes - rate is 0/hr")
                else:
                    quarter_rate = int(round((qso_diff * 60) / time_diff))
                    print(f"Interpolated 60-minute rate: {quarter_rate}/hr")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

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
    
