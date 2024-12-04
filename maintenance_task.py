import sqlite3
import argparse
import logging
from datetime import datetime, timedelta
import os
import shutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

      
def perform_enhanced_maintenance(db_path, dry_run):
    """
    Enhanced maintenance tasks for contest database including data integrity checks,
    performance optimizations, and statistics gathering.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("Starting enhanced maintenance tasks")

            # 1. Data Integrity Checks
            logger.info("Performing data integrity checks...")
            
            # Check for orphaned band_breakdown records
            cursor.execute("""
                SELECT COUNT(*) FROM band_breakdown bb 
                WHERE NOT EXISTS (
                    SELECT 1 FROM contest_scores cs 
                    WHERE cs.id = bb.contest_score_id
                )
            """)
            orphaned_bb = cursor.fetchone()[0]
            
            # Check for orphaned qth_info records
            cursor.execute("""
                SELECT COUNT(*) FROM qth_info qi 
                WHERE NOT EXISTS (
                    SELECT 1 FROM contest_scores cs 
                    WHERE cs.id = qi.contest_score_id
                )
            """)
            orphaned_qth = cursor.fetchone()[0]

            if not dry_run and (orphaned_bb > 0 or orphaned_qth > 0):
                logger.info(f"Removing {orphaned_bb} orphaned band_breakdown records")
                logger.info(f"Removing {orphaned_qth} orphaned qth_info records")
                cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                cursor.execute("DELETE FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")

            # 3. Performance Optimization
            logger.info("Analyzing table statistics...")
            
            # Get table statistics before optimization
            cursor.execute("SELECT COUNT(*) FROM contest_scores")
            total_scores = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT contest) FROM contest_scores")
            total_contests = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT callsign) FROM contest_scores")
            total_stations = cursor.fetchone()[0]

            if not dry_run:
                # Rebuild indexes if needed
                logger.info("Rebuilding indexes...")
                cursor.execute("REINDEX contest_scores")
                cursor.execute("REINDEX band_breakdown")
                cursor.execute("REINDEX qth_info")

                # Update table statistics
                logger.info("Updating table statistics...")
                cursor.execute("ANALYZE contest_scores")
                cursor.execute("ANALYZE band_breakdown")
                cursor.execute("ANALYZE qth_info")

                # Optimize database file
                logger.info("Optimizing database file...")
                cursor.execute("VACUUM")

            # 4. Storage Management
            cursor.execute("""
                SELECT contest, 
                       COUNT(*) as record_count,
                       MIN(timestamp) as oldest_record,
                       MAX(timestamp) as newest_record,
                       SUM(qsos) as total_qsos
                FROM contest_scores
                GROUP BY contest
                ORDER BY newest_record DESC
            """)
            contest_stats = cursor.fetchall()

            logger.info("\nDatabase Statistics:")
            logger.info(f"Total Contests: {total_contests}")
            logger.info(f"Total Stations: {total_stations}")
            logger.info(f"Total Score Records: {total_scores}")
            logger.info("\nPer-Contest Statistics:")
            
            for stat in contest_stats:
                logger.info(f"\nContest: {stat[0]}")
                logger.info(f"Records: {stat[1]}")
                logger.info(f"Date Range: {stat[2]} to {stat[3]}")
                logger.info(f"Total QSOs: {stat[4]}")

            # 5. Identify Potential Issues
            # Check for stations with abnormal QSO rates
            cursor.execute("""
                WITH rate_calc AS (
                    SELECT cs.callsign, 
                           cs.contest,
                           cs.qsos - LAG(cs.qsos) OVER (
                               PARTITION BY cs.callsign, cs.contest 
                               ORDER BY cs.timestamp
                           ) as qso_diff,
                           (JULIANDAY(cs.timestamp) - JULIANDAY(LAG(cs.timestamp) OVER (
                               PARTITION BY cs.callsign, cs.contest 
                               ORDER BY cs.timestamp
                           ))) * 24 * 60 as minute_diff
                    FROM contest_scores cs
                )
                SELECT callsign, 
                       contest,
                       ROUND(CAST(qso_diff as FLOAT) / NULLIF(minute_diff, 0) * 60, 2) as hourly_rate
                FROM rate_calc
                WHERE qso_diff > 0 
                AND minute_diff > 0
                AND (CAST(qso_diff as FLOAT) / NULLIF(minute_diff, 0) * 60) > 400
                ORDER BY hourly_rate DESC
                LIMIT 10
            """)
            high_rates = cursor.fetchall()
            
            if high_rates:
                logger.warning("\nPotentially problematic QSO rates detected:")
                for rate in high_rates:
                    logger.warning(f"Station: {rate[0]}, Contest: {rate[1]}, Rate: {rate[2]} QSOs/hour")

            return {
                'total_contests': total_contests,
                'total_stations': total_stations,
                'total_scores': total_scores,
                'orphaned_records_removed': orphaned_bb + orphaned_qth if not dry_run else 0,
                'inconsistent_qsos': len(inconsistent_qsos),
                'high_rate_stations': len(high_rates)
            }

    except sqlite3.Error as e:
        logger.error(f"Database error during maintenance: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during maintenance: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Maintenance Script for Contest Database.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without making modifications.")
    args = parser.parse_args()

    logger.info(f"Starting maintenance script on database: {args.db}")
    logger.info(f"Dry-run mode: {'ON' if args.dry_run else 'OFF'}")
    
    try:
        stats = perform_enhanced_maintenance(args.db, args.dry_run)
        logger.info("\nMaintenance Summary:")
        logger.info(f"Total Contests: {stats['total_contests']}")
        logger.info(f"Total Stations: {stats['total_stations']}")
        logger.info(f"Total Score Records: {stats['total_scores']}")
        logger.info(f"Orphaned Records Removed: {stats['orphaned_records_removed']}")
        logger.info(f"Inconsistent QSO Records: {stats['inconsistent_qsos']}")
        logger.info(f"High Rate Stations Found: {stats['high_rate_stations']}")
    except Exception as e:
        logger.error(f"Maintenance failed: {e}")
        sys.exit(1)
