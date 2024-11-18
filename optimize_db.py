import sqlite3
import time
import argparse

def optimize_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        print("Creating materialized view...")
        
        # Create the materialized table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS latest_contest_scores AS
        WITH latest_scores AS (
            SELECT callsign, contest, MAX(timestamp) as max_ts
            FROM contest_scores
            GROUP BY callsign, contest
        )
        SELECT 
            cs.id,
            cs.callsign,
            cs.contest,
            cs.timestamp,
            cs.score,
            cs.power,
            cs.assisted,
            cs.qsos,
            cs.multipliers,
            qi.continent
        FROM contest_scores cs
        JOIN latest_scores ls 
            ON cs.callsign = ls.callsign 
            AND cs.contest = ls.contest
            AND cs.timestamp = ls.max_ts
        LEFT JOIN qth_info qi 
            ON qi.contest_score_id = cs.id
        """)

        # Create indexes
        print("Creating indexes...")
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_latest_contest_scores_contest 
        ON latest_contest_scores(contest)
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_latest_contest_scores_continent 
        ON latest_contest_scores(contest, continent)
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_latest_contest_scores_callsign 
        ON latest_contest_scores(callsign, contest)
        """)

        # Create update trigger
        print("Creating maintenance trigger...")
        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_latest_scores 
        AFTER INSERT ON contest_scores
        BEGIN
            DELETE FROM latest_contest_scores 
            WHERE callsign = NEW.callsign 
            AND contest = NEW.contest 
            AND timestamp < NEW.timestamp;
            
            INSERT INTO latest_contest_scores
            SELECT 
                NEW.id,
                NEW.callsign,
                NEW.contest,
                NEW.timestamp,
                NEW.score,
                NEW.power,
                NEW.assisted,
                NEW.qsos,
                NEW.multipliers,
                (SELECT continent FROM qth_info WHERE contest_score_id = NEW.id)
            WHERE NOT EXISTS (
                SELECT 1 FROM contest_scores 
                WHERE callsign = NEW.callsign 
                AND contest = NEW.contest 
                AND timestamp > NEW.timestamp
            );
        END
        """)

        # Test the performance
        def test_query(contest, callsign, continent):
            start_time = time.time()
            cursor.execute("""
                SELECT 
                    id, callsign, score, power, assisted, timestamp, qsos, multipliers,
                    CASE 
                        WHEN callsign = ? THEN 'current'
                        WHEN score > (SELECT score FROM latest_contest_scores 
                                    WHERE callsign = ? AND contest = ?) 
                        THEN 'above'
                        ELSE 'below'
                    END as position,
                    ROW_NUMBER() OVER (ORDER BY score DESC) as rn
                FROM latest_contest_scores
                WHERE contest = ? 
                AND continent = ?
                ORDER BY score DESC
            """, (callsign, callsign, contest, contest, continent))
            
            results = cursor.fetchall()
            end_time = time.time()
            return len(results), end_time - start_time

        # Test with ARRL-SS-SSB
        rows, duration = test_query("ARRL-SS-SSB", "AA3B", "NA")
        print(f"\nPerformance test results:")
        print(f"Rows returned: {rows}")
        print(f"Query duration: {duration:.3f} seconds")

        conn.commit()
        print("\nOptimization completed successfully!")
        
    except Exception as e:
        print(f"Error during optimization: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Optimize contest database')
    parser.add_argument('--db', required=True, help='Database file path')
    args = parser.parse_args()
    optimize_database(args.db)
