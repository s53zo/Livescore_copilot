import os
import multiprocessing

# Create logs directory if it doesn't exist
log_dir = "/opt/livescore/logs"
os.makedirs(log_dir, exist_ok=True)

# Basic configurations
bind = "127.0.0.1:8089"
workers = multiprocessing.cpu_count() * 2 + 1
#worker_class = "sync"
worker_class = "gthread"
threads = 4
#
timeout = 120
keepalive = 5

max_requests = 1000
max_requests_jitter = 50

# Logging
errorlog = os.path.join(log_dir, "gunicorn-error.log")
accesslog = os.path.join(log_dir, "gunicorn-access.log")
loglevel = "info"  # Temporarily set to debug to get more information

# Ensure proper permissions
capture_output = True
enable_stdio_inheritance = True

# Process naming
proc_name = 'livescore-pilot'

# Graceful timeout
graceful_timeout = 30

# Trust the X-Forwarded-For headers from local proxies
forwarded_allow_ips = '127.0.0.1'

