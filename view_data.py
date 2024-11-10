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
            
            # Base query for category statistics - now with properly qualified column names
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
            
            where_clause = "WHERE cs.contest = ?" if contest else ""
            base_query = query.format(where_clause=where_clause)
            params = (contest,) if contest else ()

            # Function to get category statistics with qualified column names
            def get_category_stats(category_field):
                category_query = base_query + f"""
                    SELECT 
                        ls.contest,
                        ls.{category_field},
                        COUNT(*) as count,
                        GROUP_CONCAT(ls.callsign) as stations,
                        SUM(ls.qsos) as total_qsos,
                        MAX(ls.score) as high_score,
                        MIN(ls.score) as low_score,
                        AVG(ls.score) as avg_score
                    FROM latest_scores ls
                    WHERE ls.{category_field} IS NOT NULL AND ls.{category_field} != ''
                    GROUP BY ls.contest, ls.{category_field}
                    ORDER BY ls.contest, ls.{category_field}
                """
                cursor.execute(category_query, params)
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
                headers = ['Category', 'Count', 'Total QSOs', 'Avg Score', 'High Score', 'Example Stations']
                
                for row in stats:
                    if not contest and current_contest != row[0]:
                        if table_data:
                            print(tabulate(table_data, headers=headers, 
                                        tablefmt='grid', floatfmt=".0f"))
                            table_data = []
                        current_contest = row[0]
                        print(f"\nContest: {current_contest}")
                    
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
                    print(tabulate(table_data, headers=headers, 
                                tablefmt='grid', floatfmt=".0f"))
            
            # Display statistics for each category type
            for category_name, stats in categories.items():
                format_category_stats(stats, category_name)
            
            # Get combined category breakdown with properly qualified column names
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
                            FROM contest_scores cs2
                            WHERE cs2.contest = ?
                            GROUP BY cs2.callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        WHERE cs.contest = ?
                    )
                    SELECT 
                        COALESCE(ls.power, 'Unknown') as power,
                        COALESCE(ls.assisted, 'Unknown') as assisted,
                        COALESCE(ls.transmitter, 'Unknown') as transmitter,
                        COALESCE(ls.ops, 'Unknown') as ops,
                        COALESCE(ls.bands, 'Unknown') as bands,
                        COALESCE(ls.mode, 'Unknown') as mode,
                        COUNT(*) as count,
                        GROUP_CONCAT(ls.callsign) as stations
                    FROM latest_scores ls
                    GROUP BY ls.power, ls.assisted, ls.transmitter, 
                             ls.ops, ls.bands, ls.mode
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
                      help='Show operating category statistics (power, assisted, transmitter, ops, bands, mode)')

    args = parser.parse_args()

    if args.categories:
        show_operating_categories(args.db, args.contest)
        return

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
