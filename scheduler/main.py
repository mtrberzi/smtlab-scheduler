import logging
import boto3
import json
import requests

import config

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]       

class Scheduler(object):
    def __init__(self):
        logging.basicConfig(level=config.LOG_LEVEL)
        self.client = boto3.resource('sqs', endpoint_url=config.QUEUE_URL, region_name='elasticmq', aws_access_key_id='x', aws_secret_access_key='x', use_ssl=False)

    def handle_message(self, message):
        logging.info(f"got message: {message.body}")
        try:
            payload = json.loads(message.body)
        except ValueError:
            logging.error(f"received malformed message: {message.body}")
            return
        if 'action' not in payload:
            logging.error("received message with no 'action': {payload}")
            return
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
                logging.error(f"received message with unknown action {payload['action']}")

    def process_results(self, run_id, results):
        logging.info("Processing {} results for run {}".format(len(results), run_id))
        request_body = list(map(lambda x: {'instance_id': x['instance_id'], 'result': x['result'], 'stdout': x['stdout'], 'runtime': x['runtime']}, results))
        r = requests.post(config.SMTLAB_API_ENDPOINT + "/runs/{}/results".format(run_id), json=request_body)
        r.raise_for_status()
        # TODO validate responses for each result, possibly scheduling validation jobs
        
    def schedule_instances(self, run_id, instance_ids):
        logging.info("Scheduling instances {} for run {}".format(instance_ids, run_id))
        r = requests.get(config.SMTLAB_API_ENDPOINT + "/runs/{}".format(run_id))
        r.raise_for_status()
        run_info = r.json()
        if run_info["performance"]:
            dest_queue = 'performance'
        else:
            dest_queue = 'regression'
        for instance_id in instance_ids:
            # TODO check instance results and see whether this instance has already been run (possibly validating the result if it has)
            pass
        body = {'action': 'run', 'run_id': run_id, 'solver_id': run_info['solver_id'], 'instance_ids': instance_ids, 'arguments': run_info['arguments']}
        queue = self.client.get_queue_by_name(QueueName=dest_queue)
        queue.send_message(MessageBody=json.dumps(body))
        
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
        queue = self.client.get_queue_by_name(QueueName="scheduler")
        for chunk in chunks(run_instances, batch_size):
            instance_ids = [x['id'] for x in chunk]
            # "recursively" schedule this chunk
            chunk_msg = {'action': 'schedule_instances', 'run_id': id, 'instance_ids': instance_ids}
            queue.send_message(MessageBody=json.dumps(chunk_msg))
        
    def run(self):
        logging.info("Starting SMTLab scheduler")
        scheduler_queue = self.client.get_queue_by_name(QueueName="scheduler")
        try:
            while True:
                for message in scheduler_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5):
                    self.handle_message(message)
                    message.delete()
        except KeyboardInterrupt:
            logging.info("Caught signal, shutting down")
    
