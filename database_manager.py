#!/usr/bin/env python3
import sqlite3
import argparse
import sys
from tabulate import tabulate

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def setup_indexes(self, analyze=True):
        """Create indexes on the contest database"""
        print(f"Setting up indexes on database: {self.db_path}")
        
        index_commands = [
            # ... (keep existing index commands)
        ]

        try:
            with sqlite3.connect(self.db_path) as conn:
                for cmd in index_commands:
                    print(f"Creating index...")
                    print(cmd.replace('\n', ' ').strip())
                    conn.execute(cmd)
                    print("Success!")
                    print()
                
                if analyze:
                    print("Analyzing database...")
                    conn.execute("ANALYZE contest_scores")
                    conn.execute("ANALYZE band_breakdown")
                    print("Analysis complete!")

            print("\nAll indexes created successfully!")
            
        except sqlite3.Error as e:
            print(f"Error creating indexes: {e}", file=sys.stderr)
            sys.exit(1)

    def cleanup_small_contests(self, min_participants):
        """Remove contests that have fewer than specified number of participants"""
        print(f"\nLooking for contests with fewer than {min_participants} participants...")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # First, get a list of contests and their participant counts
                cursor.execute("""
                    WITH UniqueCalls AS (
                        SELECT DISTINCT contest, callsign
                        FROM contest_scores
                    )
                    SELECT 
                        contest,
                        COUNT(DISTINCT callsign) as participant_count
                    FROM UniqueCalls
                    GROUP BY contest
                    HAVING participant_count < ?
                    ORDER BY participant_count DESC, contest
                """, (min_participants,))
                
                small_contests = cursor.fetchall()
                
                if not small_contests:
                    print("No contests found with fewer than " +
                          f"{min_participants} participants.")
                    return
                
                # Display contests that will be removed
                headers = ['Contest', 'Participants']
                print("\nContests to be removed:")
                print(tabulate(small_contests, headers=headers, tablefmt='grid'))
                
                # Ask for confirmation
                response = input("\nDo you want to remove these contests? (yes/no): ")
                if response.lower() != 'yes':
                    print("Operation cancelled.")
                    return
                
                # Delete process
                print("\nRemoving contests...")
                for contest, _ in small_contests:
                    print(f"Processing {contest}...")
                    
                    # Get contest_score_ids for this contest
                    cursor.execute("""
                        SELECT id FROM contest_scores WHERE contest = ?
                    """, (contest,))
                    score_ids = [row[0] for row in cursor.fetchall()]
                    
                    if score_ids:
                        # Delete from band_breakdown
                        cursor.execute("""
                            DELETE FROM band_breakdown 
                            WHERE contest_score_id IN (
                                SELECT id FROM contest_scores WHERE contest = ?
                            )
                        """, (contest,))
                        bb_count = cursor.rowcount
                        
                        # Delete from qth_info
                        cursor.execute("""
                            DELETE FROM qth_info 
                            WHERE contest_score_id IN (
                                SELECT id FROM contest_scores WHERE contest = ?
                            )
                        """, (contest,))
                        qth_count = cursor.rowcount
                        
                        # Delete from contest_scores
                        cursor.execute("""
                            DELETE FROM contest_scores WHERE contest = ?
                        """, (contest,))
                        cs_count = cursor.rowcount
                        
                        print(f"Removed {cs_count} score entries, " +
                              f"{bb_count} band breakdown entries, " +
                              f"{qth_count} QTH info entries")
                    
                conn.commit()
                print("\nCleanup complete!")
                
        except sqlite3.Error as e:
            print(f"Database error: {e}", file=sys.stderr)
            sys.exit(1)

    def reindex_database(self):
        """Rebuild all indexes on the database"""
        print(f"Rebuilding indexes on database: {self.db_path}")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                print("Reindexing contest_scores table...")
                conn.execute("REINDEX contest_scores")
                
                print("Reindexing band_breakdown table...")
                conn.execute("REINDEX band_breakdown")
                
                print("Reindexing qth_info table...")
                conn.execute("REINDEX qth_info")
                
            print("\nAll indexes rebuilt successfully!")
            
        except sqlite3.Error as e:
            print(f"Error rebuilding indexes: {e}", file=sys.stderr)
            sys.exit(1)

    def list_indexes(self):
        """List all indexes in the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        m.tbl_name as table_name,
                        m.name as index_name,
                        GROUP_CONCAT(ii.name) as columns
                    FROM sqlite_master m
                    LEFT JOIN pragma_index_info(m.name) ii
                    WHERE m.type = 'index'
                    GROUP BY m.name
                    ORDER BY m.tbl_name, m.name
                """)
                
                indexes = cursor.fetchall()
                
                if indexes:
                    headers = ["Table", "Index Name", "Columns"]
                    print("\nDatabase Indexes:")
                    print(tabulate(indexes, headers=headers, tablefmt='grid'))
                else:
                    print("\nNo indexes found in the database.")
                
        except sqlite3.Error as e:
            print(f"Error listing indexes: {e}", file=sys.stderr)
            sys.exit(1)

def get_example_queries():
    """Return a dictionary of example queries to analyze"""
    return {
        "latest_scores": """
            WITH latest_records AS (
                SELECT MAX(id) as latest_id
                FROM contest_scores cs1
                GROUP BY callsign, contest
            )
            SELECT cs.callsign, cs.contest, cs.timestamp, cs.score
            FROM contest_scores cs
            INNER JOIN latest_records lr ON cs.id = lr.latest_id
            ORDER BY cs.timestamp DESC
            LIMIT 10
        """,
        
        "band_breakdown": """
            SELECT cs.callsign, bb.band, bb.qsos, bb.points
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = 'W1AW'
            ORDER BY cs.timestamp DESC
        """,
        
        "contest_summary": """
            SELECT 
                contest,
                COUNT(DISTINCT callsign) as participants,
                SUM(qsos) as total_qsos,
                MAX(score) as top_score
            FROM contest_scores
            GROUP BY contest
            ORDER BY COUNT(DISTINCT callsign) DESC
        """
    }

def explain_query(self, query):
    """Analyze and explain query execution plan"""
    try:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # First get the explain plan
            print("\nExecution Plan:")
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            plan_rows = cursor.fetchall()
            for row in plan_rows:
                print(f"id: {row[0]}, parent: {row[1]}, notused: {row[2]}, detail: {row[3]}")
            
            # Then run the query with actual execution statistics
            print("\nQuery Statistics:")
            cursor.execute(f"EXPLAIN {query}")
            stats_rows = cursor.fetchall()
            for row in stats_rows:
                print(row[0])
            
            # Execute the query to show results
            print("\nQuery Results:")
            cursor.execute(query)
            results = cursor.fetchall()
            if results:
                print(f"Found {len(results)} rows")
                # Display first few results
                for row in results[:5]:
                    print(row)
                if len(results) > 5:
                    print("...")
            else:
                print("No results found")
                
    except sqlite3.Error as e:
        print(f"Error analyzing query: {e}", file=sys.stderr)
        sys.exit(1)
        
def main():
    parser = argparse.ArgumentParser(
        description='Contest Database Management Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Create new indexes:
    %(prog)s --db contest_data.db --create-indexes
  
  Rebuild existing indexes:
    %(prog)s --db contest_data.db --reindex
  
  List existing indexes:
    %(prog)s --db contest_data.db --list
  
  Analyze query performance:
    %(prog)s --db contest_data.db --explain latest_scores
    
  Remove small contests:
    %(prog)s --db contest_data.db --cleanup-contests 5
        """)
    
    parser.add_argument('--db', default='contest_data.db',
                      help='Database file path (default: contest_data.db)')
    
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--create-indexes', action='store_true',
                          help='Create new indexes on the database')
    action_group.add_argument('--reindex', action='store_true',
                          help='Rebuild existing indexes')
    action_group.add_argument('--list', action='store_true',
                          help='List existing indexes')
    action_group.add_argument('--explain',
                          choices=list(get_example_queries().keys()),
                          help='Analyze query execution plan for example queries')
    action_group.add_argument('--cleanup-contests', type=int, metavar='MIN_PARTICIPANTS',
                          help='Remove contests with fewer than MIN_PARTICIPANTS participants')
    
    parser.add_argument('--no-analyze', action='store_true',
                      help='Skip analyzing the database after creating indexes')
    
    args = parser.parse_args()
    
    db_manager = DatabaseManager(args.db)
    
    if args.cleanup_contests is not None:
        if args.cleanup_contests < 1:
            print("Error: Minimum participants must be at least 1", file=sys.stderr)
            sys.exit(1)
        db_manager.cleanup_small_contests(args.cleanup_contests)
    elif args.create_indexes:
        db_manager.setup_indexes(not args.no_analyze)
    elif args.reindex:
        db_manager.reindex_database()
    elif args.list:
        db_manager.list_indexes()
    elif args.explain:
        query = get_example_queries()[args.explain]
        print(f"\nAnalyzing query: {args.explain}")
        print("Query text:")
        print(query)
        db_manager.explain_query(query)

if __name__ == "__main__":
    main()
