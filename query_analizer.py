import sqlite3
import time
from datetime import datetime
import argparse

def analyze_query_performance(db_path, contest, callsign, filter_type, filter_value):
    """Analyze query performance and execution plan"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n=== Breaking Down Query Performance ===")
    
    # Part 1: Analyze latest scores lookup
    print("\n1. Latest Scores Lookup Performance:")
    latest_scores_query = """
    SELECT callsign, MAX(timestamp) as max_ts
    FROM contest_scores
    WHERE contest = ?
    GROUP BY callsign
    """
    
    start_time = time.time()
    cursor.execute("EXPLAIN QUERY PLAN " + latest_scores_query, (contest,))
    plan = cursor.fetchall()
    print("\nExecution Plan:")
    for row in plan:
        print(f"id: {row[0]}, parent: {row[1]}, notused: {row[2]}, detail: {row[3]}")
    
    cursor.execute(latest_scores_query, (contest,))
    results = cursor.fetchall()
    time_taken = time.time() - start_time
    print(f"Time taken: {time_taken:.3f} seconds")
    print(f"Rows returned: {len(results)}")
    
    # Part 2: Analyze QTH info join performance
    print("\n2. QTH Info Join Performance:")
    qth_query = """
    SELECT COUNT(*), AVG(time_taken) 
    FROM (
        SELECT cs.id, cs.timestamp,
               (julianday(cs.timestamp) - julianday(cs2.timestamp)) * 86400 as time_taken
        FROM contest_scores cs
        LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
        LEFT JOIN contest_scores cs2 
            ON cs2.contest = cs.contest 
            AND cs2.callsign = cs.callsign 
            AND cs2.timestamp < cs.timestamp
        WHERE cs.contest = ?
        AND qi.continent = ?
        GROUP BY cs.id
    )
    """
    
    start_time = time.time()
    cursor.execute("EXPLAIN QUERY PLAN " + qth_query, (contest, filter_value))
    plan = cursor.fetchall()
    print("\nExecution Plan:")
    for row in plan:
        print(f"id: {row[0]}, parent: {row[1]}, notused: {row[2]}, detail: {row[3]}")
    
    cursor.execute(qth_query, (contest, filter_value))
    result = cursor.fetchone()
    time_taken = time.time() - start_time
    print(f"Time taken: {time_taken:.3f} seconds")
    if result[0]:
        print(f"Records found: {result[0]}")
        print(f"Average time between updates: {result[1]:.1f} seconds")
    
    # Analyze index usage statistics
    print("\n=== Index Usage Analysis ===")
    cursor.execute("ANALYZE")
    
    index_queries = [
        ("contest_scores by contest", 
         "SELECT * FROM contest_scores WHERE contest = ?", 
         (contest,)),
        ("qth_info by continent", 
         "SELECT * FROM qth_info WHERE continent = ?", 
         (filter_value,)),
        ("contest_scores by callsign and contest", 
         "SELECT * FROM contest_scores WHERE callsign = ? AND contest = ?", 
         (callsign, contest))
    ]
    
    for description, query, params in index_queries:
        print(f"\nAnalyzing {description}:")
        cursor.execute("EXPLAIN QUERY PLAN " + query, params)
        plan = cursor.fetchall()
        for row in plan:
            print(f"Using index: {row[3]}")
    
    # Check index sizes
    print("\n=== Index Size Analysis ===")
    cursor.execute("""
        SELECT name, 
               (SELECT COUNT(*) FROM contest_scores) as total_rows,
               (SELECT COUNT(DISTINCT contest) FROM contest_scores) as unique_contests,
               (SELECT COUNT(DISTINCT callsign) FROM contest_scores) as unique_calls
        FROM sqlite_master 
        WHERE type='index' 
        AND tbl_name='contest_scores'
    """)
    indexes = cursor.fetchall()
    for idx in indexes:
        print(f"\nIndex: {idx[0]}")
        print(f"Total rows indexed: {idx[1]:,}")
        print(f"Unique contests: {idx[2]:,}")
        print(f"Unique callsigns: {idx[3]:,}")
    
    # Analyze data distribution
    print("\n=== Data Distribution Analysis ===")
    cursor.execute("""
        SELECT COUNT(*) as record_count,
               COUNT(DISTINCT callsign) as unique_calls,
               COUNT(DISTINCT timestamp) as unique_timestamps,
               MIN(timestamp) as first_record,
               MAX(timestamp) as last_record
        FROM contest_scores 
        WHERE contest = ?
    """, (contest,))
    
    stats = cursor.fetchone()
    print(f"\nContest: {contest}")
    print(f"Total records: {stats[0]:,}")
    print(f"Unique callsigns: {stats[1]:,}")
    print(f"Unique timestamps: {stats[2]:,}")
    print(f"Time span: {stats[3]} to {stats[4]}")
    
    # Check for potential optimizations
    print("\n=== Optimization Suggestions ===")
    
    # Check if we need a compound index for the filtering
    cursor.execute("""
        SELECT COUNT(*) 
        FROM contest_scores cs
        JOIN qth_info qi ON qi.contest_score_id = cs.id
        WHERE cs.contest = ? AND qi.continent = ?
    """, (contest, filter_value))
    filtered_count = cursor.fetchone()[0]
    
    if filtered_count > 1000:
        print("\nSuggested optimizations:")
        print("1. Consider adding compound index:")
        print("   CREATE INDEX idx_opt_contest_continent ON contest_scores(contest), qth_info(continent)")
    
    # Check for data sparsity
    cursor.execute("""
        SELECT COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM contest_scores WHERE contest = ?
        ) as coverage
        FROM contest_scores cs
        JOIN qth_info qi ON qi.contest_score_id = cs.id
        WHERE cs.contest = ?
    """, (contest, contest))
    
    coverage = cursor.fetchone()[0]
    if coverage < 90:
        print("\n2. Data coverage warning:")
        print(f"   Only {coverage:.1f}% of contest records have QTH info")
        print("   Consider updating missing QTH data")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze SQLite query performance')
    parser.add_argument('--db', required=True, help='Database file path')
    parser.add_argument('--contest', required=True, help='Contest name')
    parser.add_argument('--callsign', required=True, help='Callsign')
    parser.add_argument('--filter-type', required=True, help='Filter type')
    parser.add_argument('--filter-value', required=True, help='Filter value')
    
    args = parser.parse_args()
    analyze_query_performance(
        args.db,
        args.contest,
        args.callsign,
        args.filter_type,
        args.filter_value
    )
  
