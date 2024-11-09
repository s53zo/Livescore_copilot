#!/usr/bin/env python3
from datetime import datetime
from tabulate import tabulate

def format_qth_details(data):
    """Format and display enhanced QTH information"""
    if not data:
        print("No QTH records found.")
        return

    headers = [
        'Callsign', 'Contest', 'Timestamp', 
        'DXCC', 'Prefix', 'Continent',  # New location fields first
        'CQ Zone', 'IARU Zone', 
        'ARRL Section', 'State/Province', 'Grid'
    ]
    
    formatted_data = []
    current_call = None
    
    for row in data:
        # Format timestamp
        formatted_row = [
            row[0],                    # Callsign
            row[1],                    # Contest
            datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M'),  # Timestamp
            row[3] or '-',             # DXCC Country
            row[10] or '-',            # DXCC Prefix
            row[9] or '-',             # Continent
            row[4] or '-',             # CQ Zone
            row[5] or '-',             # IARU Zone
            row[6] or '-',             # ARRL Section
            row[7] or '-',             # State/Province
            row[8] or '-',             # Grid6
        ]
        
        # Add separator line between different callsigns
        if current_call is not None and current_call != row[0]:
            formatted_data.append(['-' * 10] * len(headers))
        
        current_call = row[0]
        formatted_data.append(formatted_row)
    
    print("\nDetailed QTH Information:")
    print(tabulate(formatted_data, headers=headers, tablefmt='grid'))

def format_qth_statistics(stats):
    """Format and display QTH statistics"""
    if not stats:
        print("No QTH statistics available.")
        return

    print("\n=== Location Statistics ===")
    
    # Process each category
    for category, count, items in stats:
        # Enhanced display for Continents
        if category == 'Continents':
            print(f"\n{category}:")
            print(f"Total: {count}")
            if items:
                continents = items.split(',')
                # Sort continents in a logical order
                continent_order = ['EU', 'NA', 'SA', 'AS', 'AF', 'OC', 'AN']
                sorted_continents = sorted(continents, 
                                        key=lambda x: continent_order.index(x) 
                                        if x in continent_order else len(continent_order))
                for continent in sorted_continents:
                    print(f"  • {continent}")

        # Enhanced display for DXCC Countries
        elif category == 'DXCC Countries':
            print(f"\n{category}:")
            print(f"Total unique: {count}")
            if items:
                countries = sorted(items.split(','))  # Alphabetical sort
                # Format in columns
                col_width = max(len(country) for country in countries) + 2
                cols = max(1, 80 // col_width)  # Calculate number of columns
                for i in range(0, len(countries), cols):
                    print("  " + "".join(country.ljust(col_width) 
                          for country in countries[i:i+cols]))

        # Standard display for other categories
        else:
            print(f"\n{category}:")
            print(f"Total unique: {count}")
            if items:
                item_list = sorted(items.split(','))  # Sort items alphabetically
                # Format items in columns
                col_width = max(len(item) for item in item_list) + 2
                cols = max(1, 80 // col_width)
                for i in range(0, len(item_list), cols):
                    print("  " + "".join(item.ljust(col_width) 
                          for item in item_list[i:i+cols]))

def format_scores(data, show_all=False):
    """Format and display contest scores"""
    headers = [
        'Timestamp', 'Contest', 'Callsign', 
        'DXCC', 'Cont.',  # Added location columns
        'Power', 'Score', 'QSOs', 
        'Mults', 'Club', 'Section', 'Assisted', 'Mode'
    ]

    if not data:
        print("No records found.")
        return

    formatted_data = []
    for row in data:
        formatted_row = [
            datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M'),
            row[1],     # Contest
            row[2],     # Callsign
            row[12],    # DXCC Country
            row[11],    # Continent
            row[3],     # Power
            format_number(row[4]),  # Score with thousands separator
            row[5],     # QSOs
            row[6],     # Multipliers
            row[7],     # Club
            row[8],     # Section
            row[9],     # Assisted
            row[10]     # Mode
        ]
        
        # Truncate long fields if not show_all
        if not show_all:
            for i, field in enumerate(formatted_row):
                if isinstance(field, str) and len(field) > 20:
                    formatted_row[i] = field[:17] + '...'
        
        formatted_data.append(formatted_row)

    print("\nContest Scores:")
    print(tabulate(formatted_data, headers=headers, tablefmt='grid'))

def format_band_stats(stats):
    """Format and display database statistics"""
    # Display total stats
    total_stats = stats["total_stats"]
    print("\n=== Overall Statistics ===")
    print(f"Unique Stations: {format_number(total_stats[0])}")
    print(f"Number of Contests: {format_number(total_stats[1])}")
    print(f"Total Uploads: {format_number(total_stats[2])}")
    print(f"Total QSOs: {format_number(total_stats[3])}")

    # Display contest stats
    print("\n=== Contest Statistics ===")
    headers = [
        'Contest', 'Participants', 'Countries', 'Continents',  # Added location columns
        'Uploads', 'First Upload', 'Last Upload', 'High Score', 'Total QSOs'
    ]
    contest_data = []
    for row in stats["contest_counts"]:
        formatted_row = [
            row[0],  # Contest
            format_number(row[1]),  # Participants
            format_number(row[2]),  # Countries count
            format_number(row[3]),  # Continents count
            format_number(row[4]),  # Uploads
            datetime.strptime(row[5], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M'),  # First upload
            datetime.strptime(row[6], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M'),  # Last upload
            format_number(row[7]),  # High score
            format_number(row[8])   # Total QSOs
        ]
        contest_data.append(formatted_row)

    print(tabulate(contest_data, headers=headers, tablefmt='grid'))

    # Display band statistics by contest
    print("\n=== Band Statistics by Contest ===")
    headers = [
        'Contest', 'Stations', 
        '160m Q/P/M', '80m Q/P/M', '40m Q/P/M', 
        '20m Q/P/M', '15m Q/P/M', '10m Q/P/M'
    ]
    
    band_data = []
    for row in stats["band_stats"]:
        formatted_row = [
            row[0],  # Contest
            format_number(row[1]),  # Stations
            f"{format_number(row[2])}/{format_number(row[3])}/{format_number(row[4])}" if any([row[2], row[3], row[4]]) else "-",  # 160m
            f"{format_number(row[5])}/{format_number(row[6])}/{format_number(row[7])}" if any([row[5], row[6], row[7]]) else "-",  # 80m
            f"{format_number(row[8])}/{format_number(row[9])}/{format_number(row[10])}" if any([row[8], row[9], row[10]]) else "-",  # 40m
            f"{format_number(row[11])}/{format_number(row[12])}/{format_number(row[13])}" if any([row[11], row[12], row[13]]) else "-",  # 20m
            f"{format_number(row[14])}/{format_number(row[15])}/{format_number(row[16])}" if any([row[14], row[15], row[16]]) else "-",  # 15m
            f"{format_number(row[17])}/{format_number(row[18])}/{format_number(row[19])}" if any([row[17], row[18], row[19]]) else "-",  # 10m
        ]
        band_data.append(formatted_row)
    
    print(tabulate(band_data, headers=headers, tablefmt='grid'))

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
            formatted_data.append(['-'*10] * len(headers))
        
        current_callsign = row[0]
        current_timestamp = row[2]
        
        # Format timestamp (index 2)
        formatted_row[2] = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
        
        # Format numeric values
        formatted_row[5] = format_number(row[5])  # QSOs
        formatted_row[6] = format_number(row[6])  # Points
        formatted_row[7] = format_number(row[7])  # Multipliers
        
        formatted_data.append(formatted_row)
    
    print(tabulate(formatted_data, headers=headers, tablefmt='grid'))

def format_number(num):
    """Format numbers with thousands separator"""
    try:
        if num is None:
            return "0"
        return f"{int(num):,}"
    except (ValueError, TypeError):
        return str(num)
