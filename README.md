# Livescore Pilot - Live Contest Scoring Application

## Project Overview

**Livescore Pilot** is a web application designed to facilitate real-time contest scoring for amateur radio contests. It enables participants to monitor their performance, compare scores with nearby competitors, and apply filters based on geographical or operational criteria. The application comprises server-side scripts for data ingestion and processing, a SQLite database for storing contest data, and a web interface for user interaction.

## Key Features

- **Real-Time Scoring**: Provides up-to-date contest scores and standings for participants.
- **Filtering Options**: Allows users to filter competitors by DXCC country, CQ zone, or IARU zone.
- **Band Breakdown**: Offers a detailed view of QSOs, points, and multipliers for each band.
- **QSO Rate Calculation**: Displays the rate of QSOs over a specified time interval to help users assess their performance trends.
- **Automatic Refresh**: The report page refreshes automatically to display the latest data.
- **User-Friendly Interface**: Provides an intuitive web interface for selecting contests, callsigns, and applying filters.

## Architecture

### 1. Data Ingestion

- **XML Data Reception**: The `livescore.py` script runs an HTTP server that listens for POST requests containing XML-formatted contest data.
- **XML Parsing and Validation**: Parses the received XML data, extracts contest information, and validates the data format.
- **Data Storage**: Stores the parsed data in a SQLite database (`contest_data.db`), including contest scores and band breakdowns.

### 2. Database Management

- **Index Creation**: The `database_manager.py` script can create indexes to optimize database queries.
- **Cleanup Operations**: Removes contests with fewer participants than a specified threshold to maintain database efficiency.
- **Reindexing**: Provides functionality to rebuild indexes for performance optimization.

### 3. Data Analysis and Reporting

- **Score Retrieval**: The `ContestDatabaseViewer` class in `contest_db_viewer.py` retrieves scores, band breakdowns, and statistics from the database.
- **Report Generation**: `ScoreReporter` in `score_reporter.py` generates HTML reports using data from the database and HTML templates.
- **Rate Calculations**: Calculates QSO rates over a specified interval to provide insights into performance trends.

### 4. Web Interface

- **User Interaction**: The Flask app (`web_interface.py`) allows users to select contests and callsigns, and apply filters.
- **Live Reports**: Generates and serves live contest progress reports, updating at regular intervals.
- **Error Handling**: Provides user-friendly error messages via the `error.html` template.

### 5. Service Deployment

- **Systemd Service**: The `livescore-pilot.service` file allows the application to run as a service managed by systemd.
- **Gunicorn Configuration**: `gunicorn_config.py` specifies how Gunicorn should run the Flask app, including worker settings and logging.

## Installation

### Prerequisites

- Python 3.6 or higher
- SQLite3
- Flask
- Gunicorn

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/yourusername/livescore-pilot.git
   cd livescore-pilot
   ```

2. **Install Dependencies**

  Python will tell you about missing dependencies.

3. **Set Up the Database**

   The database will be automatically created when the application runs for the first time.

4. **Configure the Application**

   - Update `gunicorn_config.py` and `livescore-pilot.service` files with appropriate paths and settings.
   - Ensure that the `Config` class in `web_interface.py` points to the correct database and output directories.

5. **Run the Application**

   - For development:

     ```bash
     python web_interface.py
     ```

   - For production using Gunicorn and systemd:

     - Copy `livescore-pilot.service` to `/etc/systemd/system/`
     - Reload systemd and start the service:

       ```bash
       sudo systemctl daemon-reload
       sudo systemctl start livescore-pilot.service
       sudo systemctl enable livescore-pilot.service
       ```

## Usage

1. **Data Submission**

   - Contest participants configure their logging software to submit data to livescore servers.

2. **Accessing the Web Interface**

   - Navigate to `https://azure.s53m.com/livescore-pilot` to access the user interface.
   - Select the contest and callsign, and apply any desired filters.

3. **Viewing Live Reports**

   - After submitting the form, the application generates a live report displaying the contest progress.
   - The report automatically refreshes at set intervals to provide real-time updates.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for review.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Developed by Simon, S53ZO.
- Data sourced from competitors via the Contest Score Distribution Network.
