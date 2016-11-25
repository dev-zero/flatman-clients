#!/usr/bin/env python

import socket
import logging
import subprocess
import shutil
import os
from os import path
from time import sleep
from glob import glob
from abc import ABCMeta, abstractmethod, abstractproperty

import click
import click_log
import requests

# py2/3 compat calls
from six import raise_from, exec_
from six.moves.urllib.parse import urlparse  # pylint: disable=import-error


TASKS_URL = '{}/api/v2/tasks'

logger = logging.getLogger(__name__)


class ClientError(Exception):
    """For errors which are completely on the client side and are thus recoverable"""
    pass


class RunnerBase:
    __metaclass__ = ABCMeta

    """Base class to implement runners"""
    def __init__(self, settings, task_dir, running_task_func):
        self._settings = settings
        self._task_dir = task_dir
        self._running_task_func = running_task_func

        self.data = {
            'warnings': [],
            'errors': [],
            }

    @abstractmethod
    def run(self):
        """Run the calculation"""
        pass

    @abstractmethod
    def check(self):
        """Check the state of the calculation"""
        pass

    @abstractproperty
    def outfiles(self):
        """Get a list of output files"""
        return []

    @abstractproperty
    def finished(self):
        """True if a task is finished (whether successful or not)"""
        return False

    @abstractproperty
    def success(self):
        """True if a task finished successfully"""
        return False


# class SlurmRunner():
#     def __init__(self, *args, **kwargs):
#         super(SlurmRunner, self).__init__(*args, **kwargs)
#
#     @property
#     def blocking(self):
#         return False
#
#     def run(self):
#         subprocess.check_call(['sbatch', self._task['settings']['cmd']],
#                               cwd=self._task_dir)
#         return []
#

class DirectRunner(RunnerBase):
    """A runner to directly run jobs (in a blocking manner)"""

    def __init__(self, *args, **kwargs):
        super(DirectRunner, self).__init__(*args, **kwargs)
        self._outfiles = []
        self._finished = False
        self._success = False

    @property
    def outfiles(self):
        return self._outfiles

    @property
    def finished(self):
        return self._finished

    @property
    def success(self):
        return self._success

    def check(self):
        """This is a blocking runner and check() is only called when the event loop encounters
           a task for this machine in a 'running' state, which basically means that we crashed
           at some point before. And since we can't determine what happened, we are going to
           fail the job and let the user re-submit.
           But we are going to collect non-empty output files from commands to be uploaded.
        """

        for entry in self._settings['commands']:
            name = entry['name']
            stdout_fn = path.join(self._task_dir, "{}.out".format(name))
            stderr_fn = path.join(self._task_dir, "{}.err".format(name))

            if stdout_fn not in self._outfiles:
                self._outfiles.append(stdout_fn)

            if stderr_fn not in self._outfiles:
                self._outfiles.append(stderr_fn)

        self._finished = True
        raise RuntimeError("directrunner is unable to continue a 'running' job")

    def run(self):
        # since we block below, we set the task to running right away
        self._running_task_func()

        # no matter how we exit this function, the task will have terminated
        self._finished = True

        mod_env_changes = ""
        modules = self._settings['environment'].get('modules', [])

        if modules:
            with open(os.devnull, 'w') as devnull:
                mod_env_changes = subprocess.check_output(
                    map(str, ['modulecmd', 'python', 'load'] + modules),
                    stderr=devnull)
                # TODO: add check to ensure mod_env_changes
                #       contains only assignments for os.environ

        def preexec_fn():
            '''pre-exec function for further Popen calls to load
            the environment as specified by the user.

            we are injecting  the environment variables here instead
            of using Popen's env= to inherit the parent environment first'''

            os.environ.update({k: str(v) for k, v in (self._settings['environment']
                                                      .get('variables', {})
                                                      .items())})
            exec_(mod_env_changes)

        for entry in self._settings['commands']:
            name = entry['name']
            stdout_fn = path.join(self._task_dir, "{}.out".format(name))
            stderr_fn = path.join(self._task_dir, "{}.err".format(name))
            self._outfiles += [stdout_fn, stderr_fn]

            d_resp = {
                'tag': 'commands',
                'entry': name,
                }

            logger.info("running command %s", name)

            try:
                stdout = open(stdout_fn, 'w')
                stderr = open(stderr_fn, 'w')
            except (OSError, IOError) as exc:
                raise_from(
                    ClientError("error when opening {}".format(exc.filename)),
                    exc)

            try:
                subprocess.check_call(
                    map(str, [entry['cmd']] + entry['args']),
                    stdout=stdout, stderr=stderr,
                    cwd=self._task_dir, preexec_fn=preexec_fn)

            except subprocess.CalledProcessError as exc:
                d_resp['msg'] = "command terminated with non-zero exit status"
                d_resp['returncode'] = exc.returncode

                if entry.get('ignore_returncode', False):
                    self.data['warnings'].append(d_resp)
                else:
                    self.data['errors'].append(d_resp)
                    raise

            except Exception as exc:
                d_resp['msg'] = "error occurred while running: {}".format(exc)
                self.data['errors'].append(d_resp)
                raise

            finally:
                stdout.close()
                stderr.close()

        self._success = True


def task_iterator(sess, url, hostname, nap_time):
    while True:
        logger.info("checking for pending or running tasks to continue")

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

        logger.info("no new tasks available, taking a nap")
        sleep(nap_time)


RUNNERS = {
    # 'slurm': SlurmRunner,
    'direct': DirectRunner,
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

    for task in task_iterator(sess, url, hostname, nap_time):
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

        # define a function object to be called by the runners once they started the task
        def set_task_running():
            req = sess.patch(server + task['_links']['self'], json={'status': 'running'})
            req.raise_for_status()

        try:
            runner_name = task['settings']['machine']['runner']
            runner = RUNNERS[runner_name](task['settings'], task_dir, set_task_running)
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

        # running tasks should be checked, while pending task get executed
        try:
            if task['status'] == 'running':
                runner.check()
            else:
                runner.run()

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
