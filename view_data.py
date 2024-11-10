#!/usr/bin/env python3
import argparse
import sqlite3
from contest_db_viewer import ContestDatabaseViewer
from display_utils import format_qth_statistics, format_qth_details
from tabulate import tabulate
import logging

def show_operating_categories(db_path, contest=None):
    """Display operating category statistics"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Base query for category statistics
            query = """
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest, cs.timestamp,
                           cs.power, cs.assisted, cs.transmitter, cs.ops, cs.bands
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
            """
            
            if contest:
                query += " WHERE contest = ?"
                params = (contest,)
            else:
                params = ()
            
            # Power category statistics
            cursor.execute(query + """
                SELECT 
                    contest,
                    power,
                    COUNT(*) as count,
                    GROUP_CONCAT(callsign) as stations
                FROM latest_scores
                WHERE power IS NOT NULL
                GROUP BY contest, power
                ORDER BY contest, power
            """, params)
            power_stats = cursor.fetchall()
            
            # Assisted category statistics
            cursor.execute(query + """
                SELECT 
                    contest,
                    assisted,
                    COUNT(*) as count,
                    GROUP_CONCAT(callsign) as stations
                FROM latest_scores
                WHERE assisted IS NOT NULL
                GROUP BY contest, assisted
                ORDER BY contest, assisted
            """, params)
            assisted_stats = cursor.fetchall()
            
            # Transmitter category statistics
            cursor.execute(query + """
                SELECT 
                    contest,
                    transmitter,
                    COUNT(*) as count,
                    GROUP_CONCAT(callsign) as stations
                FROM latest_scores
                WHERE transmitter IS NOT NULL
                GROUP BY contest, transmitter
                ORDER BY contest, transmitter
            """, params)
            transmitter_stats = cursor.fetchall()
            
            # Operator category statistics
            cursor.execute(query + """
                SELECT 
                    contest,
                    ops,
                    COUNT(*) as count,
                    GROUP_CONCAT(callsign) as stations
                FROM latest_scores
                WHERE ops IS NOT NULL
                GROUP BY contest, ops
                ORDER BY contest, ops
            """, params)
            operator_stats = cursor.fetchall()
            
            # Band category statistics
            cursor.execute(query + """
                SELECT 
                    contest,
                    bands,
                    COUNT(*) as count,
                    GROUP_CONCAT(callsign) as stations
                FROM latest_scores
                WHERE bands IS NOT NULL
                GROUP BY contest, bands
                ORDER BY contest, bands
            """, params)
            band_stats = cursor.fetchall()
            
            # Display results
            contest_str = f" for {contest}" if contest else ""
            print(f"\n=== Operating Category Statistics{contest_str} ===\n")
            
            # Function to format statistics
            def format_category_stats(stats, category_name):
                if not stats:
                    return
                
                print(f"\n{category_name} Categories:")
                print("-" * (len(category_name) + 11))
                
                current_contest = None
                data = []
                
                for row in stats:
                    if not contest and current_contest != row[0]:
                        if data:
                            print(tabulate(data, headers=['Category', 'Count', 'Stations'], 
                                        tablefmt='grid'))
                            data = []
                        current_contest = row[0]
                        print(f"\nContest: {current_contest}")
                    
                    # Truncate station list if too long
                    stations = row[3].split(',')
                    station_str = ', '.join(stations[:5])
                    if len(stations) > 5:
                        station_str += f" (and {len(stations)-5} more)"
                    
                    data.append([row[1] or 'Unknown', row[2], station_str])
                
                if data:
                    print(tabulate(data, headers=['Category', 'Count', 'Stations'], 
                                tablefmt='grid'))
            
            # Display all category statistics
            format_category_stats(power_stats, "Power")
            format_category_stats(assisted_stats, "Assisted")
            format_category_stats(transmitter_stats, "Transmitter")
            format_category_stats(operator_stats, "Operator")
            format_category_stats(band_stats, "Band")
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    return True

def show_database_structure(db_path):
    """Display the database structure including tables, columns, and indexes"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
            tables = cursor.fetchall()
            
            print("\n=== Database Structure ===\n")
            
            for table in tables:
                table_name = table[0]
                print(f"\nTable: {table_name}")
                print("-" * (len(table_name) + 7))
                
                # Get column information
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                column_data = []
                for col in columns:
                    col_info = {
                        'Name': col[1],
                        'Type': col[2],
                        'NotNull': 'NOT NULL' if col[3] else '',
                        'DefaultValue': col[4] if col[4] is not None else '',
                        'PrimaryKey': 'PRIMARY KEY' if col[5] else ''
                    }
                    column_data.append(col_info)
                
                print(tabulate(
                    column_data, 
                    headers='keys',
                    tablefmt='grid'
                ))
                
                # Get foreign key information
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                foreign_keys = cursor.fetchall()
                
                if foreign_keys:
                    print("\nForeign Keys:")
                    fk_data = []
                    for fk in foreign_keys:
                        fk_info = {
                            'Column': fk[3],
                            'References': f"{fk[2]}({fk[4]})",
                            'OnUpdate': fk[5],
                            'OnDelete': fk[6]
                        }
                        fk_data.append(fk_info)
                    print(tabulate(
                        fk_data,
                        headers='keys',
                        tablefmt='grid'
                    ))
                
                # Get index information
                cursor.execute(f"""
                    SELECT name, sql 
                    FROM sqlite_master 
                    WHERE type='index' 
                    AND tbl_name=?
                    AND name IS NOT NULL
                """, (table_name,))
                indexes = cursor.fetchall()
                
                if indexes:
                    print("\nIndexes:")
                    index_data = []
                    for idx in indexes:
                        index_data.append({
                            'Name': idx[0],
                            'Definition': idx[1]
                        })
                    print(tabulate(
                        index_data,
                        headers='keys',
                        tablefmt='grid'
                    ))
                print("\n" + "="*50)

    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description='Contest Database Viewer')
    parser.add_argument('--db', default='contest_data.db',
                      help='Database file path (default: contest_data.db)')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='Enable debug mode')
    parser.add_argument('-s', '--sort', choices=['t', 'c', 'n', 's', 'q', 'u', 'e', 'p'],
                      default='t',
                      help='''Sort by: 
                      t=timestamp (default), 
                      c=callsign, 
                      n=contest name, 
                      s=score, 
                      q=QSOs, 
                      u=club, 
                      e=section, 
                      p=power''')
    parser.add_argument('-o', '--order', choices=['a', 'd'],
                      default='d', help='Sort order: a=ascending, d=descending (default)')
    parser.add_argument('-l', '--limit', type=int,
                      help='Limit number of records displayed')
    parser.add_argument('-b', '--bands', action='store_true',
                      help='Show band breakdown')
    parser.add_argument('-c', '--call',
                      help='Show data for specific callsign')
    parser.add_argument('-a', '--all', action='store_true',
                      help='Show full content of all fields')
    parser.add_argument('-t', '--latest', action='store_true',
                      help='Show only latest record for each callsign')
    parser.add_argument('-n', '--contest',
                      help='Show data for specific contest')
    parser.add_argument('--list-contests', action='store_true',
                      help='List all available contests')
    parser.add_argument('--stats', action='store_true',
                      help='Show database statistics')
    parser.add_argument('--qth', action='store_true',
                      help='Show QTH information for stations')
    parser.add_argument('--qth-stats', action='store_true',
                      help='Show QTH statistics')
    parser.add_argument('--structure', action='store_true',
                      help='Show database structure')
    parser.add_argument('--categories', action='store_true',
                      help='Show operating category statistics')

    args = parser.parse_args()

    if args.categories:
        show_operating_categories(args.db, args.contest)
        return

    if args.structure:
        show_database_structure(args.db)
        return

    viewer = ContestDatabaseViewer(args.db, args.debug)

    if args.list_contests:
        contests = viewer.get_available_contests()
        print("\nAvailable contests:")
        for contest in contests:
            print(contest)
        return

    if args.stats:
        stats = viewer.get_contest_stats()
        viewer.display_stats(stats)
        return

    if args.qth_stats:
        stats = viewer.get_qth_statistics(args.contest)
        format_qth_statistics(stats)
        return
        
    if args.qth:
        data = viewer.get_qth_details(args.call, args.contest)
        format_qth_details(data)
        return

    if args.bands or args.call:
        data = viewer.get_band_breakdown(args.call, args.contest)
        viewer.display_band_breakdown(data)
    else:
        sort_order = 'DESC' if args.order == 'd' else 'ASC'
        data = viewer.get_contest_scores(args.sort, sort_order, args.limit, args.latest, args.contest)
        viewer.display_scores(data, args.all)

if __name__ == "__main__":
    main()
