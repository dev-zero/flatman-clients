#!/usr/bin/env python

import socket
import logging
import shutil
import os
from os import path
from time import sleep
from glob import glob

import click
import click_log
import requests

# py2/3 compat calls
from six.moves.urllib.parse import urlparse  # pylint: disable=import-error

from .runners import ClientError, DirectRunner, SlurmRunner, MPIRunner

TASKS_URL = '{}/api/v2/tasks'

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def task_iterator(sess, url, hostname,
                  ignore_pending=False, ignore_running=False):
    """Fetches new tasks and yields them"""

    states = []
    if not ignore_pending:
        states.append('pending')
    if not ignore_running:
        states.append('running')

    if states:
        logger.info("checking for %s tasks to continue", ' or '.join(states))

        req = sess.get(TASKS_URL.format(url),
                       params={'machine': hostname, 'status': 'pending,running'})
        req.raise_for_status()
        tasks = req.json()

        if tasks:
            for task in tasks:
                yield task

    logger.info("fetching new task")
    req = sess.get(TASKS_URL.format(url), params={'limit': 1, 'status': 'new'})
    req.raise_for_status()
    tasks = req.json()
    if tasks:
        yield tasks[0]


# Register runners here:
RUNNERS = {
    'slurm': SlurmRunner,
    'direct': DirectRunner,
    'mpirun': MPIRunner,
    }


@click.command()
@click.option('--url', type=str, default='https://tctdb.chem.uzh.ch/fatman',
              show_default=True, help="The URL where FATMAN is running")
@click.option('--hostname', type=str, default=socket.gethostname,
              help="Override hostname-detection")
@click.option('--nap-time', type=str, default=5*60,
              show_default=True,
              help="Time to sleep if no new tasks are available")
@click.option('--data-dir', type=click.Path(exists=True, resolve_path=True),
              default='./fdaemon-data', show_default=True,
              help="Data directory")
@click.option('--run/--no-run',
              default=True, show_default=True,
              help="Run new jobs, otherwise only prepare and download them (running jobs are still checked)")
@click.option('--ignore-pending/--no-ignore-pending',
              default=False, show_default=True,
              help="Ignore _pending_ tasks when fetching the list of tasks from the server")
@click.option('--ignore-running/--no-ignore-running',
              default=False, show_default=True,
              help="Ignore _running_ tasks when fetching the list of tasks from the server")
@click.option('--one-shot/--no-one-shot',
              default=False, show_default=True,
              help="Do only one cycle of checking, preparing and running")
@click_log.simple_verbosity_option()
@click_log.init(__name__)
def main(url, hostname, nap_time, data_dir,
         run, ignore_pending, ignore_running, one_shot):
    """FATMAN Calculation Runner Daemon"""

    os.chdir(data_dir)

    sess = requests.Session()
    sess.verify = False  # required to ignore the self-signed cert

    parsed_uri = urlparse(url)
    server = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

    while True:
        for task in task_iterator(sess, url, hostname, ignore_pending, ignore_running):
            task_dir = path.join(data_dir, task['id'])

            if task['status'] == 'new':
                req = sess.patch(server + task['_links']['self'],
                                 json={'status': 'pending', 'machine': hostname})
                req.raise_for_status()
                task = req.json()
                logger.info("aquired new task %s", task['id'])

            elif task['status'] == 'pending':
                logger.info("continue pending task %s", task['id'])
                # fetch the complete object
                req = sess.get(server + task['_links']['self'])
                req.raise_for_status()
                task = req.json()
            else:
                logger.info("checking %s task %s", task['status'], task['id'])
                # fetch the complete object
                req = sess.get(server + task['_links']['self'])
                req.raise_for_status()
                task = req.json()

            # extract the runner info

            runner_name = None
            runner = None

            try:
                runner_name = task['settings']['machine']['runner']
                runner = RUNNERS[runner_name](task['settings'], task_dir)
            except KeyError:
                raise NotImplementedError(
                    "runner '{}' is not (yet) implemented".format(runner_name))

            # prepare the input data for pending tasks (new tasks are at this point also pending)

            if task['status'] == 'pending':

                if path.exists(task_dir):
                    logger.info("removing already existing task dir '%s'", task_dir)
                    shutil.rmtree(task_dir)

                os.mkdir(task_dir)

                # download each input file by streaming
                for infile in task['infiles']:
                    req = sess.get(server + infile['_links']['download'], stream=True)
                    req.raise_for_status()
                    with open(path.join(task_dir, infile['name']), 'wb') as fhandle:
                        for chunk in req.iter_content(1024):
                            fhandle.write(chunk)

            # define a function object to be called by the runners once they started the task
            def set_task_running():
                req = sess.patch(server + task['_links']['self'], json={'status': 'running'})
                req.raise_for_status()

            # running tasks should be checked, while pending task get executed
            try:
                if task['status'] == 'running':
                    runner.check()
                elif run:
                    if run:
                        # there are blocking runners, which is why we use a callback here
                        # to give them the chance of setting a task to running
                        runner.run(set_task_running)
                    else:
                        logger.info("task %s: skip running the task", task['id'])

            except requests.exceptions.HTTPError as error:
                logger.exception("task %s: HTTP error occurred: %s\n%s",
                                 task['id'], error, error.response.text)
                continue  # there is not much we can do now, except retrying

            except ClientError:
                logger.exception("client error occurred, leave the task as is")
                continue

            except Exception:
                logger.exception("task %s: error occurred during run", task['id'])

            # we need this check to not upload partial files, another option
            # would be to check the upload files for duplicate and checksum
            # to determine whether we have to re-upload
            if runner.finished:
                filepaths = []

                # collect files to upload as declared by the server
                for a_name in task['settings']['output_artifacts']:
                    add_filepaths = glob(path.join(task_dir, a_name))

                    if not add_filepaths:
                        runner.data['warnings'].append({
                            'tag': "output_artifacts",
                            'entry': a_name,
                            'msg': "no files found",
                            })
                        logger.warning("task %s: glob for '%s' returned 0 files",
                                       task['id'], a_name)
                        continue

                    else:
                        filepaths += add_filepaths

                # also upload additional non-empty output files from all commands
                filepaths += [f for f in runner.outfiles if path.getsize(f)]

                for filepath in filepaths:
                    data = {'name': path.relpath(filepath, task_dir)}
                    with open(filepath, 'rb') as data_fh:
                        req = sess.post(server + task['_links']['uploads'],
                                        data=data, files={'data': data_fh})
                        req.raise_for_status()

                req = sess.patch(server + task['_links']['self'],
                                 json={'status': 'done' if runner.success else 'error',
                                       'data': runner.data})
                req.raise_for_status()

        if one_shot:
            logger.info("one-shot complete, exiting as requested")
            break

        else:
            logger.info("all done for now, taking a nap")
            sleep(nap_time)
