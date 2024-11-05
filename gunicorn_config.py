bind = "127.0.0.1:8089"
workers = 3
worker_class = "sync"
timeout = 120
keepalive = 5
errorlog = "/opt/livescore/logs/gunicorn-error.log"
accesslog = "/opt/livescore/logs/gunicorn-access.log"
loglevel = "info"
