#!/usr/bin/env python3
import argparse
import sqlite3
from contest_db_viewer import ContestDatabaseViewer
from display_utils import format_qth_statistics, format_qth_details
from tabulate import tabulate
import logging

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

    args = parser.parse_args()

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
