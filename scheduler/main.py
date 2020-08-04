import config
import logging
import stomp
import json
import requests

class EventQueueListener(stomp.ConnectionListener):
    def on_error(self, frame):
        logging.error(f"message queue error: {frame.body}")

    def on_message(self, frame):
        try:
            payload = json.loads(frame.body)
            if 'action' not in payload:
                logging.error("received message with no 'action': {payload}")
            if payload['action'] == 'schedule':
                if 'id' not in payload:
                    logging.error("received 'schedule' action with no 'id'")
                logging.info(f"Scheduling run #{payload['id']}")
                # TODO
            else:
                # unknown action
                logging.error("received message with unknown action {payload['action']}")
        except ValueError:
            logging.error(f"received malformed message: {frame.body}")

class Scheduler(object):
    def __init__(self):
        logging.basicConfig(level=config.LOG_LEVEL)
        self.conn = stomp.Connection(config.QUEUE_CONNECTION)
        self.conn.set_listener('', EventQueueListener())

    def run(self):
        logging.info("Starting SMTLab scheduler")
        self.conn.connect(config.QUEUE_USERNAME, config.QUEUE_PASSWORD, wait=True)
        logging.info("Connected to queue endpoint")
        self.conn.subscribe(destination='queue/scheduler', id=1, ack='auto')
        try:
            while True:
                pass
        except KeyboardInterrupt:
            logging.info("Caught signal, shutting down")
            self.conn.disconnect()
    
