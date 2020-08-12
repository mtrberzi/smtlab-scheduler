import logging
import stomp
import json
import requests

import config

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class EventQueueListener(stomp.ConnectionListener):
    def __init__(self, conn, subscription_id):
        self.conn = conn
        self.subscription_id = subscription_id
    
    def on_error(self, frame):
        logging.error(f"message queue error: {frame.body}")
        
    def on_message(self, frame):
        try:
            payload = json.loads(frame.body)
        except ValueError:
            logging.error(f"received malformed message: {frame.body}")
            self.conn.ack(frame.headers['message-id'], self.subscription_id)
            return
        if 'action' not in payload:
            logging.error("received message with no 'action': {payload}")
        else:
            if payload['action'] == 'schedule':
                if 'id' not in payload:
                    logging.error("received 'schedule' action with no 'id'")
                else:
                    logging.info(f"Scheduling run #{payload['id']}")
                    try:
                        self.schedule_run(payload['id'])
                    except:
                        logging.exception("exception thrown in schedule_run()")
            elif payload['action'] == 'schedule_instances':
                if 'run_id' not in payload:
                    logging.error("received 'schedule_instances' action with no 'run_id'")
                elif 'instance_ids' not in payload:
                    logging.error("received 'schedule_instances' action with no 'instance_ids'")
                else:
                    try:
                        self.schedule_instances(payload["run_id"], payload["instance_ids"])
                    except:
                        logging.exception("exception thrown in schedule_instances()")
            elif payload['action'] == 'process_results':
                if 'run_id' not in payload:
                    logging.error("received 'process_results' action with no 'run_id'")
                elif 'results' not in payload:
                    logging.error("received 'process_results' action with no 'results'")
                else:
                    results_ok = True
                    for result in payload['results']:
                        if 'instance_id' not in result or 'result' not in result or 'stdout' not in result or 'runtime' not in result:
                            results_ok = False
                            break
                    if results_ok:
                        try:
                            self.process_results(payload["run_id"], payload["results"])
                        except:
                            logging.exception("exception thrown in process_results()")
                    else:
                        logging.error("received 'process_results' with invalid 'results' body")    
            else:
                # unknown action
                logging.error("received message with unknown action {payload['action']}")
        # success
        self.conn.ack(frame.headers['message-id'], self.subscription_id)

    def process_results(self, run_id, results):
        logging.info("Processing {} results for run {}".format(len(results), run_id))
        request_body = list(map(lambda x: {'instance_id': x['instance_id'], 'result': x['result'], 'stdout': x['stdout'], runtime: x['runtime']}, results))
        r = requests.post(config.SMTLAB_API_ENDPOINT + "/runs/{}/results".format(run_id), json=request_body)
        r.raise_for_status()
        # TODO validate responses for each result, possibly scheduling validation jobs
        
    def schedule_instances(self, run_id, instance_ids):
        logging.info("Scheduling instances {} for run {}".format(instance_ids, run_id))
        r = requests.get(config.SMTLAB_API_ENDPOINT + "/runs/{}".format(run_id))
        r.raise_for_status()
        run_info = r.json()
        if run_info["performance"]:
            dest_queue = 'queue/performance'
        else:
            dest_queue = 'queue/regression'
        for instance_id in instance_ids:
            # TODO check instance results and see whether this instance has already been run (possibly validating the result if it has)
            pass
        body = {'action': 'run', 'solver_id': run_info['solver_id'], 'instance_ids': instance_ids, 'arguments': run_info['arguments']}
        self.conn.send(body=json.dumps(body), destination=dest_queue)
        
    def schedule_run(self, id):
        logging.info("Scheduling run {}".format(id))
        r = requests.get(config.SMTLAB_API_ENDPOINT + "/runs/{}".format(id))
        r.raise_for_status()
        run_info = r.json()
        r2 = requests.get(config.SMTLAB_API_ENDPOINT + "/benchmarks/{}/instances".format(run_info['benchmark_id']))
        r2.raise_for_status()
        run_instances = r2.json()
        # choose a batch size based on the total number of instances
        if len(run_instances) <= 10:
            batch_size = 1
        elif len(run_instances) <= 100:
            batch_size = 5
        elif len(run_instances) <= 1000:
            batch_size = 10
        elif len(run_instances) <= 10000:
            batch_size = 15
        else:
            batch_size = 20
        for chunk in chunks(run_instances, batch_size):
            instance_ids = [x['id'] for x in chunk]
            # "recursively" schedule this chunk
            chunk_msg = {'action': 'schedule_instances', 'run_id': id, 'instance_ids': instance_ids}
            self.conn.send(body=json.dumps(chunk_msg), destination='queue/scheduler')

class Scheduler(object):
    def __init__(self):
        logging.basicConfig(level=config.LOG_LEVEL)
        self.subscription_id = 1
        self.conn = stomp.Connection(config.QUEUE_CONNECTION)
        self.conn.set_listener('', EventQueueListener(self.conn, self.subscription_id))

    def run(self):
        logging.info("Starting SMTLab scheduler")
        self.conn.connect(config.QUEUE_USERNAME, config.QUEUE_PASSWORD, wait=True)
        logging.info("Connected to queue endpoint")
        self.conn.subscribe(destination='queue/scheduler', id=self.subscription_id, ack='client-individual')
        try:
            while True:
                pass
        except KeyboardInterrupt:
            logging.info("Caught signal, shutting down")
            self.conn.disconnect()
    
