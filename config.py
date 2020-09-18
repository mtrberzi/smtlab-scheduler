import os
import logging

SMTLAB_API_ENDPOINT = os.environ.get('SMTLAB_API_ENDPOINT') or 'http://127.0.0.1:5000/'
if SMTLAB_API_ENDPOINT[-1] != '/':
    SMTLAB_API_ENDPOINT += "/"

QUEUE_BACKOFF_LIMIT = 8
LOG_LEVEL = logging.INFO
SMTLAB_USERNAME = os.environ.get('SMTLAB_USERNAME')
SMTLAB_PASSWORD = os.environ.get('SMTLAB_PASSWORD')
try:
    THREADS=int(os.environ.get('SMTLAB_SCHEDULER_THREADS') or 1)
except ValueError:
    logging.warn("SMTLAB_SCHEDULER_THREADS must be an integer")
    THREADS=1
