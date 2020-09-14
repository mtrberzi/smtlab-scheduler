import os
import logging

SMTLAB_API_ENDPOINT = os.environ.get('SMTLAB_API_ENDPOINT') or 'http://127.0.0.1:5000'
QUEUE_URL = os.environ.get('SMTLAB_QUEUE_URL') or 'http://127.0.0.1:9324'
LOG_LEVEL = logging.INFO
SMTLAB_USERNAME = os.environ.get('SMTLAB_USERNAME')
SMTLAB_PASSWORD = os.environ.get('SMTLAB_PASSWORD')
