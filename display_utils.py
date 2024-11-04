#!/usr/bin/env python3
from datetime import datetime
from tabulate import tabulate

def format_band_stats(stats):
    """Format and display database statistics"""
    # Display total stats
    total_stats = stats["total_stats"]
    print("\n=== Overall Statistics ===")
    print(f"Unique Stations: {total_stats[0]}")
    print(f"Number of Contests: {total_stats[1]}")
    print(f"Total Uploads: {total_stats[2]}")
    print(f"Total QSOs: {total_stats[3]}")

    # Display contest stats
    print("\n=== Contest Statistics ===")
    headers = ['Contest', 'Participants', 'Uploads', 'First Upload', 'Last Upload', 'High Score', 'Total QSOs']
    contest_data = []
    for row in stats["contest_counts"]:
        formatted_row = list(row)
        # Format timestamps
        formatted_row[3] = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
        formatted_row[4] = datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
        contest_data.append(formatted_row)
    print(tabulate(contest_data, headers=headers, tablefmt='grid'))

    # Display band statistics by contest
    print("\n=== Band Statistics by Contest ===")
    headers = ['Contest', 'Stations', 
              '160m Q/P/M', '80m Q/P/M', '40m Q/P/M', 
              '20m Q/P/M', '15m Q/P/M', '10m Q/P/M']
    
    band_data = []
    for row in stats["band_stats"]:
        formatted_row = [
            row[0],  # Contest
            row[1],  # Stations
            f"{row[2]}/{row[3]}/{row[4]}" if row[2] or row[3] or row[4] else "-",  # 160m
            f"{row[5]}/{row[6]}/{row[7]}" if row[5] or row[6] or row[7] else "-",  # 80m
            f"{row[8]}/{row[9]}/{row[10]}" if row[8] or row[9] or row[10] else "-",  # 40m
            f"{row[11]}/{row[12]}/{row[13]}" if row[11] or row[12] or row[13] else "-",  # 20m
            f"{row[14]}/{row[15]}/{row[16]}" if row[14] or row[15] or row[16] else "-",  # 15m
            f"{row[17]}/{row[18]}/{row[19]}" if row[17] or row[18] or row[19] else "-",  # 10m
        ]
        band_data.append(formatted_row)
    
    print(tabulate(band_data, headers=headers, tablefmt='grid'))

def format_scores(data, show_all=False):
    """Format and display contest scores"""
    headers = ['Timestamp', 'Contest', 'Callsign', 'Power', 'Score', 'QSOs', 
              'Mults', 'Club', 'Section', 'Assisted', 'Mode']

    if not data:
        print("No records found.")
        return

    formatted_data = []
    for row in data:
        formatted_row = list(row)
        # Format timestamp
        formatted_row[0] = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
        
        # Truncate long fields if not show_all
        if not show_all:
            for i, field in enumerate(formatted_row):
                if isinstance(field, str) and len(field) > 20:
                    formatted_row[i] = field[:17] + '...'
        
        formatted_data.append(formatted_row)

    print(tabulate(formatted_data, headers=headers, tablefmt='grid'))

def format_band_breakdown(data):
    """Format and display band breakdown data"""
    if data is None:
        return

    headers = ['Callsign', 'Contest', 'Timestamp', 'Band', 'Mode', 'QSOs', 'Points', 'Multipliers']
    
    if not data:
        print("No band breakdown records found.")
        return
    
    # Format the timestamp in the data
    formatted_data = []
    current_callsign = None
    current_timestamp = None
    
    for row in data:
        formatted_row = list(row)
        
        # Add a blank line between different callsigns or timestamps
        if (current_callsign is not None and current_callsign != row[0]) or \
           (current_timestamp is not None and current_timestamp != row[2]):
            formatted_data.append(['-'*10, '-'*10, '-'*16, '-'*4, '-'*4, '-'*4, '-'*6, '-'*11])
        
        current_callsign = row[0]
        current_timestamp = row[2]
        
        # Format timestamp (index 2)
        formatted_row[2] = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
        formatted_data.append(formatted_row)
    
    print(tabulate(formatted_data, headers=headers, tablefmt='grid'))

def format_number(num):
    """Format numbers with thousands separator"""
    return f"{num:,}" if num else "0"
