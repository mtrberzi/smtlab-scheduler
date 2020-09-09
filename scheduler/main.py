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
            elif payload['action'] == 'process_validation':
                if 'result_id' not in payload or 'solver_id' not in payload or 'validation' not in payload or 'stdout' not in payload:
                    logging.error("received 'process_validation' action with missing required fields")
                else:
                    # upload this result
                    request_body = [{'solver_id': payload['solver_id'], 'validation': payload['validation'], 'stdout': payload['stdout']}]
                    r = requests.post(config.SMTLAB_API_ENDPOINT + "/results/{}/validation".format(payload['result_id']), json=request_body)
                    r.raise_for_status()
            else:
                # unknown action
                logging.error(f"received message with unknown action {payload['action']}")

    def process_results(self, run_id, results):
        logging.info("Processing {} results for run {}".format(len(results), run_id))
        request_body = list(map(lambda x: {'instance_id': x['instance_id'], 'result': x['result'], 'stdout': x['stdout'], 'runtime': x['runtime']}, results))
        r = requests.post(config.SMTLAB_API_ENDPOINT + "/runs/{}/results".format(run_id), json=request_body)
        r.raise_for_status()
        # the request returns the new result objects, with their IDs...
        result_info = r.json()
        for result in result_info:
            # ...so validate each result that we get back
            self.schedule_validation(result['id'])
        
    def schedule_instances(self, run_id, instance_ids):
        logging.info("Scheduling instances {} for run {}".format(instance_ids, run_id))
        r = requests.get(config.SMTLAB_API_ENDPOINT + "/runs/{}".format(run_id))
        r.raise_for_status()
        run_info = r.json()
        if run_info["performance"]:
            dest_queue = 'performance'
        else:
            dest_queue = 'regression'
        instance_ids_to_run = []
        instance_ids_to_validate = []
        instance_ids_with_results = []
        r_results = requests.get(config.SMTLAB_API_ENDPOINT + "/runs/{}/results".format(run_id))
        r_results.raise_for_status()
        result_info = r_results.json()
        for result in result_info:
            instance_ids_with_results.append(result['instance_id'])
        for instance_id in instance_ids:
            if instance_id in instance_ids_with_results:
                instance_ids_to_validate.append(instance_id)
            else:
                instance_ids_to_run.append(instance_id)
        if len(instance_ids_to_run) > 0:
            queue = self.client.get_queue_by_name(QueueName=dest_queue)
            for instance_id in instance_ids_to_run:
                body = {'action': 'run', 'run_id': run_id, 'solver_id': run_info['solver_id'], 'instance_id': instance_id, 'arguments': run_info['arguments']}
                queue.send_message(MessageBody=json.dumps(body))
        for instance_id in instance_ids_to_validate:
            # map instance ID to its corresponding result ID
            # TODO this is quadratic, and can probably be optimized
            for result in result_info:
                if result['instance_id'] == instance_id:
                    self.schedule_validation(result['id'])

    def schedule_validation(self, result_id):
        logging.info("Checking validations for result {}".format(result_id))
        r = requests.get(config.SMTLAB_API_ENDPOINT + "/results/{}".format(result_id))
        r.raise_for_status()
        result_info = r.json()
        if result_info['result'] == "sat" or result_info['result'] == "unsat":
            validation_solvers_already_used = [] # only for direct validation of this result - not from other runs
            validation_solvers_checked = 0
            validation_solvers_agreeing = 0
            validation_solvers_disagreeing = 0
            validation_solvers_inconclusive = 0
            for validation in result_info['validations']:
                if 'validation' in validation:
                    validation_solvers_already_used.append(validation['solver_id'])
                    validation_solvers_checked += 1
                    if validation['validation'] == "valid":
                        validation_solvers_agreeing += 1
                    elif validation['validation'] == "invalid":
                        validation_solvers_disagreeing += 1
                    else:
                        validation_solvers_inconclusive += 1
                elif 'result' in validation:
                    validation_solvers_checked += 1
                    if validation['result'] == "sat" or validation['result'] == "unsat":
                        if result_info['result'] == validation['result']:
                            validation_solvers_agreeing += 1
                        else:
                            validation_solvers_disagreeing += 1
                    else:
                        validation_solvers_inconclusive += 1
            logging.info("{} solvers checked, {} agreeing, {} disagreeing, {} inconclusive".format(validation_solvers_checked, validation_solvers_agreeing, validation_solvers_disagreeing, validation_solvers_inconclusive))
            # now decide whether to run the remaining validation solvers based on the outcome
            if result_info['result'] == "unsat":
                return
            if validation_solvers_disagreeing > 0:
                return
            r_solvers = requests.get(config.SMTLAB_API_ENDPOINT + "/solvers")
            r_solvers.raise_for_status()
            solver_info = r_solvers.json()
            validation_solvers = []
            for solver in solver_info:
                if solver['validation_solver']:
                    validation_solvers.append(solver['id'])
            for v_id in validation_solvers_already_used:
                validation_solvers.remove(v_id)
            for v_id in validation_solvers:
                body = {'action': 'validate', 'result_id': result_id, 'solver_id': v_id}
                queue = self.client.get_queue_by_name(QueueName="regression")
                queue.send_message(MessageBody=json.dumps(body))
        else:
            logging.info("Result {} is {}, nothing to validate".format(result_id, result_info['result']))
            return
        
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
    
