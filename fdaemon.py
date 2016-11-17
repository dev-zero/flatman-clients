#!/usr/bin/env python

import socket
import logging
import subprocess
import shutil
import os
from os import path
from time import sleep
from glob import glob
# py2/3 compat calls
from six import raise_from
from six.moves.urllib.parse import urlparse

import click
import click_log
import requests

TASKS_URL = '{}/api/v2/tasks'

logger = logging.getLogger(__name__)


# errors which are completely on the client side
# and are thus recoverable
class ClientError(Exception):
    pass


def run_with_slurm(sess, server, task, task_dir):
    subprocess.check_call(['sbatch', task['settings']['cmd']],
                          cwd=task_dir)
    return []


def download_only(sess, server, task, task_dir):
    return []


def run_direct(sess, server, task, task_dir):
    # since we block below, we set the task to running right away
    req = sess.patch(server + task['_links']['self'],
                     json={'status': 'running'})
    req.raise_for_status()

    outfiles = []

    for entry in task['settings']['commands']:
        name = entry['name']
        stdout_fn = path.join(task_dir, "{}.out".format(name))
        stderr_fn = path.join(task_dir, "{}.err".format(name))
        outfiles += [stdout_fn, stderr_fn]

        d_resp = {
            'tag': 'commands',
            'entry': name,
            }

        logger.info("task %s: running command %s", task['id'], name)

        try:
            with open(stdout_fn, 'w') as stdout, \
                 open(stderr_fn, 'w') as stderr:
                subprocess.check_call(
                    [entry['cmd']] + entry['args'],
                    stdout=stdout, stderr=stderr,
                    cwd=task_dir)

        except EnvironmentError as exc:
            raise_from(ClientError("error when opening stdout/err files"), exc)

        except subprocess.CalledProcessError as exc:
            d_resp['msg'] = "command terminated with non-zero exit status"
            d_resp['returncode'] = exc.returncode

            if entry.get('ignore_returncode', False):
                task['data']['warnings'].append(d_resp)
            else:
                task['data']['errors'].append(d_resp)
                raise

        except subprocess.SubprocessError as exc:
            d_resp['msg'] = "error occurred while running: {}".format(exc)
            task['data']['errors'].append(d_resp)
            raise

    return outfiles


RUNNERS = {
    'slurm': run_with_slurm,
    'download-only': download_only,
    'direct': run_direct,
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
@click_log.simple_verbosity_option()
@click_log.init(__name__)
def main(url, hostname, nap_time, data_dir):
    """FATMAN Calculation Runner Daemon"""

    os.chdir(data_dir)

    sess = requests.Session()
    sess.verify = False  # required to ignore the self-signed cert

    parsed_uri = urlparse(url)
    server = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

    while True:
        logger.info("fetching new task")
        req = sess.get(TASKS_URL.format(url),
                       params={'limit': 1, 'status': 'new'})
        req.raise_for_status()
        tasks = req.json()

        if len(tasks) == 0:
            logger.info("no new tasks available, taking a nap")
            sleep(nap_time)
            continue

        req = sess.patch(server + tasks[0]['_links']['self'],
                         json={'status': 'pending', 'machine': hostname})
        req.raise_for_status()
        task = req.json()

        logger.info("aquired new task %s", task['id'])

        task_dir = path.join(data_dir, task['id'])

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

        try:
            runner = RUNNERS[task['settings']['machine']['runner']]
        except KeyError:
            raise NotImplementedError(
                "runner {} is not (yet) implemented".format(runner))

        # create some structure in task.data if not already present:

        if not task['data']:
            task['data'] = {}

        task['data']['warnings'] = task['data'].get('warnings', [])
        task['data']['errors'] = task['data'].get('errors', [])

        try:
            # TODO: the following works only for a blocking runner
            outfiles = runner(sess, server, task, task_dir)

            filepaths = []

            # collect files to upload as declared by the server
            for a_name in task['settings']['output_artifacts']:
                add_filepaths = glob(path.join(task_dir, a_name))

                if not add_filepaths:
                    task['data']['warnings'].append({
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
            filepaths += [f for f in outfiles if path.getsize(f)]

            for filepath in filepaths:
                data = {
                    'name': path.relpath(filepath, task_dir),
                    }
                with open(filepath, 'rb') as data_fh:
                    req = sess.post(server + task['_links']['uploads'],
                                    data=data, files={'data': data_fh})
                    req.raise_for_status()

            req = sess.patch(server + task['_links']['self'],
                             json={'status': 'done', 'data': task['data']})
            req.raise_for_status()

        except requests.exceptions.HTTPError as error:
            logger.exception("task %s: HTTP error occurred: %s\n%s",
                             task['id'], error, error.response.text)
            raise

        except ClientError:
            logger.exception("client error occurred, keep the task running")

        except Exception:
            logger.exception("task %s: error occurred during run", task['id'])
            req = sess.patch(server + task['_links']['self'],
                             json={'status': 'error', 'data': task['data']})
            req.raise_for_status()


if __name__ == '__main__':
    main()
