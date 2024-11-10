#!/usr/bin/env python3
import argparse
import sqlite3
from contest_db_viewer import ContestDatabaseViewer
from display_utils import format_qth_statistics, format_qth_details
from tabulate import tabulate
import logging

def show_operating_categories(db_path, contest=None):
    """Display operating category statistics including all category fields"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Base query for category statistics - now including mode
            query = """
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest, cs.timestamp,
                           cs.power, cs.assisted, cs.transmitter, cs.ops, cs.bands, cs.mode,
                           cs.qsos, cs.score
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                    {where_clause}
                )
            """
            
            where_clause = "WHERE contest = ?" if contest else ""
            base_query = query.format(where_clause=where_clause)
            params = (contest,) if contest else ()

            # Function to get category statistics
            def get_category_stats(category_field):
                cursor.execute(base_query + f"""
                    SELECT 
                        contest,
                        {category_field},
                        COUNT(*) as count,
                        GROUP_CONCAT(callsign) as stations,
                        SUM(qsos) as total_qsos,
                        MAX(score) as high_score,
                        MIN(score) as low_score,
                        AVG(score) as avg_score
                    FROM latest_scores
                    WHERE {category_field} IS NOT NULL AND {category_field} != ''
                    GROUP BY contest, {category_field}
                    ORDER BY contest, {category_field}
                """, params)
                return cursor.fetchall()

            # Get statistics for all categories
            categories = {
                'Power': get_category_stats('power'),
                'Assisted': get_category_stats('assisted'),
                'Transmitter': get_category_stats('transmitter'),
                'Operator': get_category_stats('ops'),
                'Bands': get_category_stats('bands'),
                'Mode': get_category_stats('mode')
            }
            
            # Display results
            contest_str = f" for {contest}" if contest else ""
            print(f"\n=== Operating Category Statistics{contest_str} ===\n")
            
            def format_category_stats(stats, category_name):
                if not stats:
                    return
                
                print(f"\n{category_name} Categories:")
                print("=" * (len(category_name) + 11))
                
                current_contest = None
                table_data = []
                
                for row in stats:
                    if not contest and current_contest != row[0]:
                        if table_data:
                            print(tabulate(table_data, headers=headers, 
                                        tablefmt='grid', floatfmt=".0f"))
                            table_data = []
                        current_contest = row[0]
                        print(f"\nContest: {current_contest}")
                        headers = ['Category', 'Count', 'Total QSOs', 'Avg Score', 'High Score', 'Example Stations']
                    
                    # Format scores
                    avg_score = int(row[7]) if row[7] else 0
                    high_score = int(row[5]) if row[5] else 0
                    
                    # Format station list
                    stations = row[3].split(',')
                    station_str = ', '.join(stations[:3])
                    if len(stations) > 3:
                        station_str += f" (+{len(stations)-3})"
                    
                    category_value = row[1] or 'Unknown'
                    table_data.append([
                        category_value,
                        row[2],
                        row[4],
                        format(avg_score, ",d"),
                        format(high_score, ",d"),
                        station_str
                    ])
                
                if table_data:
                    headers = ['Category', 'Count', 'Total QSOs', 'Avg Score', 'High Score', 'Example Stations']
                    print(tabulate(table_data, headers=headers, 
                                tablefmt='grid', floatfmt=".0f"))
            
            # Display statistics for each category type
            for category_name, stats in categories.items():
                format_category_stats(stats, category_name)
            
            # Get combined category breakdown
            if contest:
                print(f"\nDetailed Category Combinations for {contest}:")
                print("=" * 40)
                cursor.execute("""
                    WITH latest_scores AS (
                        SELECT cs.id, cs.callsign, cs.power, cs.assisted, 
                               cs.transmitter, cs.ops, cs.bands, cs.mode
                        FROM contest_scores cs
                        INNER JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        WHERE cs.contest = ?
                    )
                    SELECT 
                        COALESCE(power, 'Unknown') as power,
                        COALESCE(assisted, 'Unknown') as assisted,
                        COALESCE(transmitter, 'Unknown') as transmitter,
                        COALESCE(ops, 'Unknown') as ops,
                        COALESCE(bands, 'Unknown') as bands,
                        COALESCE(mode, 'Unknown') as mode,
                        COUNT(*) as count,
                        GROUP_CONCAT(callsign) as stations
                    FROM latest_scores
                    GROUP BY power, assisted, transmitter, ops, bands, mode
                    ORDER BY count DESC
                    LIMIT 10
                """, (contest, contest))
                
                combo_data = []
                for row in cursor.fetchall():
                    stations = row[7].split(',')
                    station_str = ', '.join(stations[:2])
                    if len(stations) > 2:
                        station_str += f" (+{len(stations)-2})"
                    
                    combo_data.append([
                        row[0], row[1], row[2], row[3], 
                        row[4], row[5], row[6], station_str
                    ])
                
                if combo_data:
                    print("\nTop Category Combinations:")
                    headers = ['Power', 'Assisted', 'Transmitter', 'Operator', 
                             'Bands', 'Mode', 'Count', 'Example Stations']
                    print(tabulate(combo_data, headers=headers, tablefmt='grid'))
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    return True

[Previous code remains the same...]

def main():
    parser = argparse.ArgumentParser(description='Contest Database Viewer')
    [Previous arguments remain the same...]
    parser.add_argument('--categories', action='store_true',
                      help='Show operating category statistics (power, assisted, transmitter, ops, bands, mode)')

    args = parser.parse_args()

    if args.categories:
        show_operating_categories(args.db, args.contest)
        return

    [Rest of the main function remains the same...]

if __name__ == "__main__":
    main()
