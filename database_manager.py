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
            # Primary table indexes
            """CREATE INDEX IF NOT EXISTS idx_contest_scores_callsign_contest 
               ON contest_scores(callsign, contest)""",

            """CREATE INDEX IF NOT EXISTS idx_qth_contest_score_id ON qth_info(contest_score_id)""",
            
            """CREATE INDEX IF NOT EXISTS idx_contest_scores_timestamp 
               ON contest_scores(timestamp)""",
            
            """CREATE INDEX IF NOT EXISTS idx_contest_scores_contest_timestamp 
               ON contest_scores(contest, timestamp)""",
            
            # Band breakdown indexes
            """CREATE INDEX IF NOT EXISTS idx_band_breakdown_contest_score_id 
               ON band_breakdown(contest_score_id)""",
            
            """CREATE INDEX IF NOT EXISTS idx_band_breakdown_band 
               ON band_breakdown(band, qsos)""",
            
            # Composite indexes
            """CREATE INDEX IF NOT EXISTS idx_contest_scores_full 
               ON contest_scores(callsign, contest, timestamp, score)""",
            
            # Covering index
            """CREATE INDEX IF NOT EXISTS idx_contest_scores_common 
               ON contest_scores(callsign, contest, timestamp, score, qsos, power, club, section)"""
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

    def reindex_database(self):
        """Rebuild all indexes on the database"""
        print(f"Rebuilding indexes on database: {self.db_path}")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                print("Reindexing contest_scores table...")
                conn.execute("REINDEX contest_scores")
                
                print("Reindexing band_breakdown table...")
                conn.execute("REINDEX band_breakdown")
                
            print("\nAll indexes rebuilt successfully!")
            
        except sqlite3.Error as e:
            print(f"Error rebuilding indexes: {e}", file=sys.stderr)
            sys.exit(1)

    def explain_query(self, query):
        """Show the query execution plan"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"EXPLAIN QUERY PLAN {query}")
                plan = cursor.fetchall()
                
                print("\nQuery Execution Plan:")
                print("-" * 60)
                for step in plan:
                    print(f"Step {step[0]}: {step[3]}")
                print("-" * 60)
                
        except sqlite3.Error as e:
            print(f"Error explaining query: {e}", file=sys.stderr)
            sys.exit(1)

    def list_indexes(self):
        """List all indexes in the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get indexes from sqlite_master
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
    
Available example queries for --explain:
  - latest_scores: Shows most recent scores for all callsigns
  - band_breakdown: Shows band breakdown for W1AW
  - contest_summary: Shows summary statistics for each contest
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
    
    parser.add_argument('--no-analyze', action='store_true',
                      help='Skip analyzing the database after creating indexes')
    
    args = parser.parse_args()
    
    db_manager = DatabaseManager(args.db)
    
    if args.create_indexes:
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
  
