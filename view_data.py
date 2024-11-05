#!/usr/bin/env python3
import argparse
from contest_db_viewer import ContestDatabaseViewer
from display_utils import format_qth_statistics, format_qth_details

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

    args = parser.parse_args()

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
    
