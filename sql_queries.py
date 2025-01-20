# SQL Queries for LiveScore Contest Reporting System

# Contest queries
GET_CONTESTS = """
    SELECT contest, COUNT(DISTINCT callsign) AS active_stations
    FROM contest_scores
    GROUP BY contest
    ORDER BY contest
"""

GET_CALLSIGNS = """
    WITH latest_scores AS (
        SELECT cs.callsign, cs.qsos, cs.timestamp
        FROM contest_scores cs
        INNER JOIN (
            SELECT callsign, MAX(timestamp) as max_ts
            FROM contest_scores
            WHERE contest = ?
            GROUP BY callsign
        ) latest ON cs.callsign = latest.callsign 
            AND cs.timestamp = latest.max_ts
        WHERE cs.contest = ?
        AND cs.qsos > 0
    )
    SELECT DISTINCT callsign, qsos as qso_count
    FROM latest_scores
    ORDER BY callsign
"""

API_GET_CALLSIGNS = """
    WITH latest_scores AS (
        SELECT cs.callsign, cs.qsos, cs.timestamp
        FROM contest_scores cs
        INNER JOIN (
            SELECT callsign, MAX(timestamp) as max_ts
            FROM contest_scores
            WHERE contest = ?
            GROUP BY callsign
        ) latest ON cs.callsign = latest.callsign 
            AND cs.timestamp = latest.max_ts
        WHERE cs.contest = ?
        AND cs.qsos > 0
    )
    SELECT DISTINCT callsign, qsos as qso_count
    FROM latest_scores
    ORDER BY callsign
"""

VERIFY_STATION = """
    SELECT COUNT(*) 
    FROM contest_scores
    WHERE contest = ?
    AND callsign = ?
"""

GET_FILTERS = """
    SELECT qi.dxcc_country, qi.cq_zone, qi.iaru_zone, 
           qi.arrl_section, qi.state_province, qi.continent
    FROM contest_scores cs
    JOIN qth_info qi ON qi.contest_score_id = cs.id
    WHERE cs.contest = ? AND cs.callsign = ?
    ORDER BY cs.timestamp DESC
    LIMIT 1
"""

# Rate calculation queries
CALCULATE_RATES = """
    WITH now AS (
        SELECT datetime('now') as current_utc
    ),
    total_qsos AS (
        SELECT cs.timestamp, SUM(bb.qsos) as total
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        CROSS JOIN now n 
        WHERE cs.callsign = ? 
        AND cs.contest = ?
        AND cs.timestamp >= ?
        AND cs.timestamp <= ?
        AND (julianday(n.current_utc) - julianday(cs.timestamp)) * 24 * 60 <= 75
        GROUP BY cs.timestamp
        ORDER BY cs.timestamp DESC
    )
    SELECT 
        MAX(total) - MIN(total) as qso_diff,
        COUNT(*) as samples,
        MAX(timestamp) as latest,
        MIN(timestamp) as earliest
    FROM total_qsos
    WHERE timestamp >= ?
"""

CALCULATE_BAND_RATES = """
    WITH now AS (
        SELECT datetime('now') as current_utc
    ),
    band_qsos AS (
        SELECT cs.timestamp, bb.band, bb.qsos
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        CROSS JOIN now n
        WHERE cs.callsign = ? 
        AND cs.contest = ?
        AND cs.timestamp >= ?
        AND cs.timestamp <= ?
        AND (julianday(n.current_utc) - julianday(cs.timestamp)) * 24 * 60 <= 75
        ORDER BY cs.timestamp DESC
    )
    SELECT 
        band,
        MAX(qsos) - MIN(qsos) as qso_diff,
        COUNT(*) as samples,
        MAX(timestamp) as latest,
        MIN(timestamp) as earliest
    FROM band_qsos
    WHERE timestamp >= ?
    GROUP BY band
    HAVING qso_diff > 0
"""

# Station details query
# GET_STATION_DETAILS query has been moved to score_reporter.py
# where it is now dynamically built based on filter parameters

# Band breakdown queries
GET_BAND_BREAKDOWN = """
    SELECT bb.band, bb.qsos, bb.multipliers
    FROM contest_scores cs
    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
    WHERE cs.callsign = ?
    AND cs.contest = ?
    AND cs.timestamp = ?
    AND bb.qsos > 0
    ORDER BY bb.band
"""

GET_BAND_BREAKDOWN_WITH_RATES = """
    WITH current_score AS (
        SELECT cs.id, cs.timestamp, bb.band, bb.qsos, bb.multipliers
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        WHERE cs.callsign = ? 
        AND cs.contest = ?
        AND cs.timestamp = ?
    ),
    long_window_score AS (
        SELECT bb.band, bb.qsos
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        WHERE cs.callsign = ?
        AND cs.contest = ?
        AND cs.timestamp <= datetime(?, '-60 minutes')
        AND cs.timestamp >= datetime(?, '-65 minutes')
        ORDER BY cs.timestamp DESC
    ),
    short_window_score AS (
        SELECT bb.band, bb.qsos
        FROM contest_scores cs
        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        WHERE cs.callsign = ?
        AND cs.contest = ?
        AND cs.timestamp <= datetime(?, '-15 minutes')
        AND cs.timestamp >= datetime(?, '-20 minutes')
        ORDER BY cs.timestamp DESC
    )
    SELECT 
        cs.band,
        cs.qsos as current_qsos,
        cs.multipliers,
        lws.qsos as long_window_qsos,
        sws.qsos as short_window_qsos
    FROM current_score cs
    LEFT JOIN long_window_score lws ON cs.band = lws.band
    LEFT JOIN short_window_score sws ON cs.band = sws.band
    WHERE cs.qsos > 0
    ORDER BY cs.band
"""

# Database schema queries
CREATE_CONTEST_SCORES_TABLE = """
    CREATE TABLE IF NOT EXISTS contest_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        contest TEXT,
        callsign TEXT,
        power TEXT,
        assisted TEXT,
        transmitter TEXT,
        ops TEXT,
        bands TEXT,
        mode TEXT,
        overlay TEXT,
        club TEXT,
        section TEXT,
        score INTEGER,
        qsos INTEGER,
        multipliers INTEGER,
        points INTEGER
    )
"""

CREATE_BAND_BREAKDOWN_TABLE = """
    CREATE TABLE IF NOT EXISTS band_breakdown (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contest_score_id INTEGER,
        band TEXT,
        mode TEXT,
        qsos INTEGER,
        points INTEGER,
        multipliers INTEGER,
        FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
    )
"""

CREATE_QTH_INFO_TABLE = """
    CREATE TABLE IF NOT EXISTS qth_info (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contest_score_id INTEGER,
        dxcc_country TEXT,
        cq_zone TEXT,
        iaru_zone TEXT,
        arrl_section TEXT,
        state_province TEXT,
        grid6 TEXT,
        FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
    )
"""

# Data insertion queries
INSERT_QTH_INFO = """
    INSERT INTO qth_info (
        contest_score_id, dxcc_country, continent, cq_zone, 
        iaru_zone, arrl_section, state_province, grid6
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_BAND_BREAKDOWN = """
    INSERT INTO band_breakdown (
        contest_score_id, band, mode, qsos, points, multipliers
    ) VALUES (?, ?, ?, ?, ?, ?)
"""

INSERT_CONTEST_DATA = """
    INSERT INTO contest_scores (
        timestamp, contest, callsign, power, assisted, transmitter,
        ops, bands, mode, overlay, club, section, score, qsos,
        multipliers, points
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Data consistency queries
CHECK_QSO_CONSISTENCY = """
    SELECT cs.id, cs.callsign, cs.qsos, SUM(bb.qsos) as total_band_qsos
    FROM contest_scores cs
    LEFT JOIN band_breakdown bb ON bb.contest_score_id = cs.id
    GROUP BY cs.id
    HAVING cs.qsos != total_band_qsos
    AND total_band_qsos IS NOT NULL
"""

COUNT_ORPHANED_BAND_BREAKDOWN = """
    SELECT COUNT(*) 
    FROM band_breakdown bb
    LEFT JOIN contest_scores cs ON cs.id = bb.contest_score_id
    WHERE cs.id IS NULL
"""

COUNT_ORPHANED_QTH_INFO = """
    SELECT COUNT(*) 
    FROM qth_info qi
    LEFT JOIN contest_scores cs ON cs.id = qi.contest_score_id
    WHERE cs.id IS NULL
"""

ANALYZE_ORPHANED_BAND_BREAKDOWN = """
    SELECT 
        bb.contest_score_id,
        COUNT(*) as record_count,
        SUM(bb.qsos) as total_qsos,
        GROUP_CONCAT(DISTINCT bb.band) as bands,
        MIN(bb.qsos) as min_qsos,
        MAX(bb.qsos) as max_qsos
    FROM band_breakdown bb
    LEFT JOIN contest_scores cs ON cs.id = bb.contest_score_id
    WHERE cs.id IS NULL
    GROUP BY bb.contest_score_id
    ORDER BY record_count DESC
    LIMIT 10
"""

ANALYZE_ORPHANED_QTH_INFO = """
    SELECT 
        qi.contest_score_id,
        qi.dxcc_country,
        qi.cq_zone,
        qi.iaru_zone,
        qi.arrl_section,
        qi.state_province
    FROM qth_info qi
    LEFT JOIN contest_scores cs ON cs.id = qi.contest_score_id
    WHERE cs.id IS NULL
    ORDER BY qi.contest_score_id DESC
    LIMIT 10
"""

DELETE_ORPHANED_BAND_BREAKDOWN = """
    DELETE FROM band_breakdown
    WHERE contest_score_id IN (
        SELECT bb.contest_score_id
        FROM band_breakdown bb
        LEFT JOIN contest_scores cs ON cs.id = bb.contest_score_id
        WHERE cs.id IS NULL
    )
"""

DELETE_ORPHANED_QTH_INFO = """
    DELETE FROM qth_info
    WHERE contest_score_id IN (
        SELECT qi.contest_score_id
        FROM qth_info qi
        LEFT JOIN contest_scores cs ON cs.id = qi.contest_score_id
        WHERE cs.id IS NULL
    )
"""

FIND_SMALL_CONTESTS = """
    SELECT contest, COUNT(DISTINCT callsign) as num_callsigns
    FROM contest_scores
    GROUP BY contest
    HAVING num_callsigns < 5
"""

GET_OLD_CONTESTS = """
    SELECT DISTINCT contest
    FROM contest_scores
    WHERE timestamp < ?
    ORDER BY timestamp DESC
"""

DELETE_OLD_CONTEST_DATA = """
    BEGIN TRANSACTION;
    
    -- Delete band breakdowns
    DELETE FROM band_breakdown
    WHERE contest_score_id IN (
        SELECT id FROM contest_scores WHERE contest = ?
    );
    
    -- Delete QTH info
    DELETE FROM qth_info
    WHERE contest_score_id IN (
        SELECT id FROM contest_scores WHERE contest = ?
    );
    
    -- Delete contest scores
    DELETE FROM contest_scores
    WHERE contest = ?;
    
    COMMIT;
"""

GET_OLD_RECORDS = """
    SELECT id
    FROM contest_scores
    WHERE timestamp < ?
"""

GET_ARCHIVE_RECORDS = """
    SELECT id, contest, timestamp
    FROM contest_scores
    WHERE timestamp < ?
    ORDER BY timestamp DESC
"""

# Data deletion queries
DELETE_BAND_BREAKDOWN_BY_CONTEST_SCORE_ID = """
    DELETE FROM band_breakdown
    WHERE contest_score_id IN (
        SELECT id FROM contest_scores WHERE contest = ?
    )
"""

DELETE_QTH_INFO_BY_CONTEST_SCORE_ID = """
    DELETE FROM qth_info
    WHERE contest_score_id IN (
        SELECT id FROM contest_scores WHERE contest = ?
    )
"""

DELETE_CONTEST_SCORES_BY_CONTEST = """
    DELETE FROM contest_scores
    WHERE contest = ?
"""

# Maintenance queries
GET_REDUNDANT_INDEXES = """
    SELECT name FROM sqlite_master 
    WHERE type = 'index' 
    AND name NOT LIKE 'sqlite_autoindex%'
    AND name NOT IN (
        SELECT name FROM sqlite_master 
        WHERE type = 'index' 
        AND sql LIKE '%latest_contest_scores%'
    )
"""

GET_INDEX_USAGE = """
    SELECT name, sql FROM sqlite_master 
    WHERE type='index' AND sql IS NOT NULL
    AND name NOT LIKE 'sqlite_autoindex%'
"""

GET_INDEX_STATS = """
    SELECT COUNT(*) as usage_count
    FROM sqlite_stat1
    WHERE idx = ?
"""

GET_DB_METRICS = """
    PRAGMA page_size;
    PRAGMA page_count;
"""

GET_INDEXES_TO_REBUILD = """
    SELECT name FROM sqlite_master
    WHERE type = 'index'
    AND name NOT LIKE 'sqlite_autoindex%'
    AND name IN (
        'idx_contest_scores_contest_timestamp',
        'idx_contest_scores_callsign_contest_timestamp', 
        'idx_band_breakdown_contest_score_id_band',
        'idx_qth_info_contest_score_id'
    )
"""

# Add these to sql_queries.py

# Base query for contest standings using latest_contest_scores
GET_CONTEST_STANDINGS_BASE = """
    WITH ranked_stations AS (
        SELECT 
            ls.id,
            ls.callsign,
            ls.score,
            ls.power,
            ls.assisted,
            ls.timestamp,
            ls.qsos,
            ls.multipliers,
            ROW_NUMBER() OVER (ORDER BY ls.score DESC) as position
        FROM latest_contest_scores ls
        WHERE ls.contest = ?
"""

# Extension for QTH filters
GET_CONTEST_STANDINGS_QTH_FILTER = """
        AND {field} = ?
"""

# Close CTE and add position range filter
GET_CONTEST_STANDINGS_RANGE = """
    )
    SELECT rs.*, 
        CASE WHEN rs.callsign = ? THEN 'current'
             WHEN rs.score > (SELECT score FROM ranked_stations WHERE callsign = ?) 
             THEN 'above' ELSE 'below' END as rel_pos
    FROM ranked_stations rs
    WHERE EXISTS (
        SELECT 1 FROM ranked_stations ref 
        WHERE ref.callsign = ? 
        AND ABS(rs.position - ref.position) <= 5
    )
    ORDER BY rs.score DESC
"""

# Close CTE and add standard ordering
GET_CONTEST_STANDINGS_ALL = """
    )
    SELECT rs.*, 
        CASE WHEN rs.callsign = ? THEN 'current'
             WHEN rs.score > (SELECT score FROM ranked_stations WHERE callsign = ?) 
             THEN 'above' ELSE 'below' END as rel_pos
    FROM ranked_stations rs
    ORDER BY rs.score DESC
"""

# Map of filter types to their column names in latest_contest_scores
FILTER_MAP = {
    'DXCC': 'ls.dxcc_country',
    'CQ Zone': 'ls.cq_zone',
    'IARU Zone': 'ls.iaru_zone',
    'ARRL Section': 'ls.arrl_section',
    'State/Province': 'ls.state_province',
    'Continent': 'ls.continent'
}

# Verify station exists in contest
VERIFY_STATION_LATEST = """
    SELECT COUNT(*) 
    FROM latest_contest_scores
    WHERE contest = ?
    AND callsign = ?
"""

# Get filter options for a station
GET_FILTERS_LATEST = """
    SELECT dxcc_country, cq_zone, iaru_zone, 
           arrl_section, state_province, continent
    FROM latest_contest_scores
    WHERE contest = ? AND callsign = ?
    LIMIT 1
"""

