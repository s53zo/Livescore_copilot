#!/usr/bin/env python3

import sqlite3
import argparse
from datetime import datetime, timedelta


def cleanup_scores(db_path, dry_run, callsign=None, contest=None, minutes=90):
    """
    Retains only the latest `minutes` of scores for specified callsign and contest.

    :param db_path: Path to the SQLite database.
    :param dry_run: If True, no changes are made to the database; just prints the deletions.
    :param callsign: Filter by callsign (optional).
    :param contest: Filter by contest (optional).
    :param minutes: Time period (in minutes) to retain scores.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Build query to fetch latest timestamps
            query = """
                SELECT callsign, contest, MAX(timestamp) AS latest_timestamp
                FROM contest_scores
                WHERE 1=1
            """
            params = []

            # Apply filters
            if callsign:
                query += " AND callsign = ?"
                params.append(callsign)
            if contest:
                query += " AND contest = ?"
                params.append(contest)

            query += " GROUP BY callsign, contest"
            cursor.execute(query, params)
            latest_entries = cursor.fetchall()

            print(f"Found {len(latest_entries)} callsign-contest combinations to process.\n")

            total_deleted = 0
            for entry_callsign, entry_contest, latest_timestamp in latest_entries:
                latest_time = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S')
                cutoff_time = latest_time - timedelta(minutes=minutes)

                # Find entries older than the cutoff
                cursor.execute("""
                    SELECT id, timestamp FROM contest_scores
                    WHERE callsign = ? AND contest = ? AND timestamp < ?
                """, (entry_callsign, entry_contest, cutoff_time.strftime('%Y-%m-%d %H:%M:%S')))
                old_entries = cursor.fetchall()

                if old_entries:
                    print(f"Callsign: {entry_callsign}, Contest: {entry_contest}")
                    print(f"  Latest entry: {latest_time}")
                    print(f"  Retaining scores from the last {minutes} minutes.")
                    print(f"  Removing entries older than: {cutoff_time}")
                    print(f"  Entries to delete: {len(old_entries)}")
                    if dry_run:
                        for entry in old_entries:
                            print(f"    Would delete: ID={entry[0]}, Timestamp={entry[1]}")
                    else:
                        # Delete the old entries
                        cursor.executemany("""
                            DELETE FROM contest_scores WHERE id = ?
                        """, [(entry[0],) for entry in old_entries])
                        print(f"  Deleted {len(old_entries)} entries.")

                    total_deleted += len(old_entries)

            if not dry_run:
                conn.commit()
                print(f"\nCleanup complete. Total entries deleted: {total_deleted}")
            else:
                print(f"\nDry-run complete. Total entries that would be deleted: {total_deleted}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Clean up old scores from contest database."
    )
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the changes without making any deletions.",
    )
    parser.add_argument(
        "--callsign",
        help="Filter by callsign (optional). Only process scores for this callsign.",
    )
    parser.add_argument(
        "--contest",
        help="Filter by contest (optional). Only process scores for this contest.",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=90,
        help="Time period (in minutes) to retain scores. Default: 90 minutes.",
    )

    args = parser.parse_args()

    print(f"Starting cleanup process on database: {args.db}")
    print(f"Dry-run mode: {'ON' if args.dry_run else 'OFF'}")
    if args.callsign:
        print(f"Filter: Callsign = {args.callsign}")
    if args.contest:
        print(f"Filter: Contest = {args.contest}")
    print(f"Time period to retain: {args.minutes} minutes\n")

    cleanup_scores(args.db, args.dry_run, args.callsign, args.contest, args.minutes)


if __name__ == "__main__":
    main()
