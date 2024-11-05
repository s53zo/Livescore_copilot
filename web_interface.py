@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    logger.debug(f"Request received: {request.method}")
    logger.debug(f"Request form data: {request.form}")
    logger.debug(f"Request headers: {request.headers}")

    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Get contests
            logger.debug("Fetching contests")
            cursor.execute("""
                SELECT DISTINCT contest 
                FROM contest_scores 
                ORDER BY contest
            """)
            contests = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found contests: {contests}")
            
            # If contest is selected (either via POST or GET parameter)
            selected_contest = request.form.get('contest') or request.args.get('contest')
            if selected_contest:
                # Get callsigns for this contest only
                logger.debug(f"Fetching callsigns for contest: {selected_contest}")
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    )
                    SELECT cs.callsign
                    FROM contest_scores cs
                    JOIN LatestScores ls ON cs.callsign = ls.callsign 
                        AND cs.timestamp = ls.max_ts
                    WHERE cs.contest = ?
                    ORDER BY cs.callsign
                """, (selected_contest, selected_contest))
                callsigns = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Found callsigns: {callsigns}")
                
                # Get available DXCC countries for this contest
                cursor.execute("""
                    SELECT DISTINCT qi.dxcc_country
                    FROM contest_scores cs
                    JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.contest = ?
                    AND qi.dxcc_country IS NOT NULL 
                    AND qi.dxcc_country != ''
                    ORDER BY qi.dxcc_country
                """, (selected_contest,))
                countries = [row[0] for row in cursor.fetchall()]
                
                # Get available CQ zones for this contest
                cursor.execute("""
                    SELECT DISTINCT qi.cq_zone
                    FROM contest_scores cs
                    JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.contest = ?
                    AND qi.cq_zone IS NOT NULL 
                    AND qi.cq_zone != ''
                    ORDER BY qi.cq_zone
                """, (selected_contest,))
                cq_zones = [row[0] for row in cursor.fetchall()]
                
                # Get available IARU zones for this contest
                cursor.execute("""
                    SELECT DISTINCT qi.iaru_zone
                    FROM contest_scores cs
                    JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.contest = ?
                    AND qi.iaru_zone IS NOT NULL 
                    AND qi.iaru_zone != ''
                    ORDER BY qi.iaru_zone
                """, (selected_contest,))
                iaru_zones = [row[0] for row in cursor.fetchall()]
            else:
                callsigns = []
                countries = []
                cq_zones = []
                iaru_zones = []
        
        if request.method == 'POST' and request.form.get('callsign'):
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            filter_type = request.form.get('filter_type')
            filter_value = request.form.get('filter_value')
            
            logger.info(f"POST request with callsign={callsign}, contest={contest}, "
                       f"filter_type={filter_type}, filter_value={filter_value}")
            
            # Create reporter instance
            logger.debug("Creating ScoreReporter instance")
            reporter = ScoreReporter(Config.DB_PATH)
            
            # Get station details with filters
            logger.debug("Getting station details")
            stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)
            logger.debug(f"Station details result: {stations}")
            
            if stations:
                logger.debug("Generating HTML report")
                success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                logger.debug(f"HTML generation result: {success}")
                
                if success:
                    logger.info("Redirecting to report")
                    return redirect('/reports/live.html')
                else:
                    logger.error("Failed to generate report")
                    return render_template('error.html', error="Failed to generate report")
            else:
                logger.warning("No stations found")
                return render_template('error.html', error="No data found for the selected criteria")
        
        logger.debug("Rendering template")
        return render_template('select_form.html', 
                             contests=contests,
                             selected_contest=selected_contest,
                             callsigns=callsigns,
                             countries=countries,
                             cq_zones=cq_zones,
                             iaru_zones=iaru_zones)
    
    except Exception as e:
        logger.error("Exception occurred:")
        logger.error(traceback.format_exc())
        return render_template('error.html', error=f"Error: {str(e)}")
