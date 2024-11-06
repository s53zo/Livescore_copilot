#!/usr/bin/env python3
import sqlite3
import argparse
from datetime import datetime, timedelta

def calculate_rates(db_path, callsign, contest):
    """Calculate 1-hour and 15-minute QSO rates for a given station"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get latest submission first
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
                ORDER BY timestamp DESC
                LIMIT 1
            """, (callsign, contest))
            
            latest = cursor.fetchone()
            if not latest:
                print(f"No records found for {callsign} in {contest}")
                return

            latest_ts = datetime.strptime(latest[0], '%Y-%m-%d %H:%M:%S')
            latest_qsos = latest[1]
            latest_bands = parse_band_breakdown(latest[2]) if latest[2] else {}

            print(f"\nRate analysis for {callsign} in {contest}")
            print(f"Latest entry: {latest_ts} with {latest_qsos} QSOs")
            print("-" * 50)

            # Calculate target timestamps
            hour_target = latest_ts - timedelta(hours=1)
            quarter_target = latest_ts - timedelta(minutes=15)

            # Get closest records to target times
            cursor.execute("""
                SELECT 
                    timestamp,
                    qsos,
                    band_data.band_breakdown,
                    ABS(JULIANDAY(timestamp) - JULIANDAY(?)) as hour_diff,
                    ABS(JULIANDAY(timestamp) - JULIANDAY(?)) as quarter_diff
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
                AND timestamp < ?
                ORDER BY timestamp DESC
            """, (hour_target, quarter_target, callsign, contest, latest[0]))

            records = cursor.fetchall()
            
            # Find closest records to target times
            hour_record = None
            quarter_record = None
            min_hour_diff = float('inf')
            min_quarter_diff = float('inf')

            for record in records:
                if record[3] < min_hour_diff:
                    hour_record = record
                    min_hour_diff = record[3]
                if record[4] < min_quarter_diff:
                    quarter_record = record
                    min_quarter_diff = record[4]

            # Calculate 1-hour rate
            if hour_record:
                hour_ts = datetime.strptime(hour_record[0], '%Y-%m-%d %H:%M:%S')
                hour_qsos = hour_record[1]
                hour_bands = parse_band_breakdown(hour_record[2]) if hour_record[2] else {}
                
                # Calculate actual time difference in minutes
                time_diff = (latest_ts - hour_ts).total_seconds() / 60
                qso_diff = latest_qsos - hour_qsos
                
                print("\n1-hour rate calculation:")
                print(f"Time span: {time_diff:.1f} minutes")
                print(f"QSOs in span: {qso_diff}")
                
                if qso_diff == 0:
                    print("No QSO changes - rate is 0/hr")
                else:
                    # Interpolate to 60 minutes
                    hour_rate = int(round((qso_diff * 60) / time_diff))
                    print(f"Interpolated 60-minute rate: {hour_rate}/hr")
                
                print("Band changes:")
                for band in sorted(set(latest_bands.keys()) | set(hour_bands.keys())):
                    diff = latest_bands.get(band, 0) - hour_bands.get(band, 0)
                    if diff != 0:
                        print(f"  {band}m: +{diff}")

            # Calculate 15-minute rate
            if quarter_record:
                quarter_ts = datetime.strptime(quarter_record[0], '%Y-%m-%d %H:%M:%S')
                quarter_qsos = quarter_record[1]
                quarter_bands = parse_band_breakdown(quarter_record[2]) if quarter_record[2] else {}
                
                # Calculate actual time difference in minutes
                time_diff = (latest_ts - quarter_ts).total_seconds() / 60
                qso_diff = latest_qsos - quarter_qsos
                
                print("\n15-minute rate calculation:")
                print(f"Time span: {time_diff:.1f} minutes")
                print(f"QSOs in span: {qso_diff}")
                
                if qso_diff == 0:
                    print("No QSO changes - rate is 0/hr")
                else:
                    # Interpolate to 60 minutes
                    quarter_rate = int(round((qso_diff * 60) / time_diff))
                    print(f"Interpolated 60-minute rate: {quarter_rate}/hr")
                
                print("Band changes:")
                for band in sorted(set(latest_bands.keys()) | set(quarter_bands.keys())):
                    diff = latest_bands.get(band, 0) - quarter_bands.get(band, 0)
                    if diff != 0:
                        print(f"  {band}m: +{diff}")

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
    
