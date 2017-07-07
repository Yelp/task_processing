#!/usr/bin/env python3
import copy
import logging
import threading
import time
import uuid
from datetime import datetime
from os import listdir
from os.path import isfile
from os.path import join

import yaml
from croniter import croniter
from twisted.internet import reactor

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.async import Async
from task_processing.runners.async import EventHandler

logging.basicConfig()

task_runner = None
task_config = None
dag_store = {}


class Action(object):
    def __init__(self, name, command, depends_on):
        self.name = name
        self.command = command
        self.depends_on = depends_on
        self.finished = False


class DagConfig(object):
    # We need this class so we can create DagExecutor from it.
    def __init__(self, schedule):
        self.schedule = schedule
        self.actions = []

    def add_action(self, action):
        self.actions.append(action)


class DagExecutor(object):
    def __init__(self, dag_config, uuid):
        # Note: This DAG does not need to know the schedule. It is being handled by schedule_dag
        # method.
        self.uuid = uuid
        # Create a copy of actions for execution context of this DAG
        self.actions = copy.deepcopy(dag_config.actions)

    def update_status(self, action_name):
        logging.warning('Status update received! {} {}'.format(
            self.uuid, action_name))
        for action in self.actions:
            logging.warning('This action {} has following dependencies {} and the lenght is {}'.format(
                action.name, action.depends_on, len(action.depends_on)))
            action.depends_on.pop(action_name, None)
            if action.name == action_name:
                action.finished = True

        self.run_next_actions()

    def run_next_actions(self):
        flag = True
        for action in self.actions:
            if len(action.depends_on) == 0 and not action.finished:
                # All the actions that this action depends on have been executed, so we can execute
                # this action
                logging.warning('Running next action {} for job {}'.format(
                    action.name, self.uuid))
                run_container(None, self.uuid, action.name)
                flag = False
        if flag is True:
            # All the actions of this DAG have been executed, remove it from the dag_store
            global dag_store
            logging.info('Removed DAG {} from dag store'.format(self.uuid))
            dag_store.pop(self.uuid, None)


class ConfigParser(object):
    def __init__(self, config_dir):
        self.config_dir = config_dir

    def build_dags(self):
        self._read_config()

    def _read_config(self):
        files = [f for f in listdir(self.config_dir)
                 if isfile(join(self.config_dir, f))]
        for f in files:
            self._parse_file(join(self.config_dir, f))

    def _parse_file(self, f):
        with open(f) as fd:
            executors = yaml.load(fd)

        for key, value in executors.items():
            if key == 'jobs':
                if value is None:
                    # Ignore the value if it does not contain anything
                    return
                for job in value:
                    # Build DAGs for each job specified
                    dag_config = DagConfig(job['schedule'])
                    for action in job['actions']:
                        dag_config.add_action(
                            Action(
                                action['name'],
                                action['command'],
                                {} if 'requires' not in action.keys() else {
                                    action_name[1]: 0 for action_name in enumerate(action['requires'])}
                            )
                        )
                    schedule_dag(dag_config, parse_batch_schedule(
                        dag_config.schedule))


def schedule_dag(dag_config, interval):
    # This function is responsible for periodically executing the dag
    global dag_store
    dag_uuid = str(uuid.uuid4())
    dag_executor = DagExecutor(dag_config, dag_uuid)
    dag_store[dag_uuid] = dag_executor
    logging.info('Created a new DAG executor {}'.format(dag_uuid))
    dag_executor.run_next_actions()
    reactor.callLater(interval, schedule_dag, dag_config, interval)


def parse_batch_schedule(schedule):
    if type(schedule) == dict:
        return 60
    elif 'cron' in schedule:
        # Truncate the string
        cron = croniter(schedule[5:len(schedule)], datetime.now())
        next_exec = cron.get_next(datetime).timestamp()
        ret = next_exec - time.time()
        logging.warning(
            '=======================================================')
        logging.warning('Return value is {}'.format(ret))
        logging.warning(
            '=======================================================')
        return ret
    return 60


def handle_status_update(update):
    logging.warning(
        '=======================================================================')
    logging.warning(
        'Received a new update with terminal state {}'.format(update.success))
    if update.success in (True, False):
        dag_uuid, action_name = update.task_config['name'].split('*')
        global dag_store
        dag_store[dag_uuid].update_status(action_name)
    logging.warning(
        '=======================================================================')


def run_container(job_config, dag_uuid, action_name):
    global task_runner
    task_runner.run(get_new_container_config(
        name='{}*{}'.format(dag_uuid, action_name)))


def get_new_container_config(name):
    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    return TaskConfig(
        name=name,
        image='ubuntu:16.04',
        cmd='/bin/sleep 5'
    )


def main():
    # Iniditialize the task processor so we can run containers on it
    executor = MesosExecutor(
        secret='bee5aeJibee5aeJibee5aeJi',
        mesos_address='10.40.1.51:5050',
        role='*'
    )

    runner = Async(
        executor,
        [EventHandler(
            predicate=lambda x: x.terminal,
            cb=handle_status_update
        )],
    )
    global task_runner
    task_runner = runner

    # ConfigParser('/nail/home/sagarp/task_processing/tronfig').build_dags()
    ConfigParser('/nail/home/sagarp/tronfig/sfo2').build_dags()

    # Start the scheduler which will schedule all the events in the series
    thread = threading.Thread(target=reactor.run, args=())
    thread.start()
    # Block on the thread
    thread.join()
    runner.stop()


if __name__ == '__main__':
    exit(main())
