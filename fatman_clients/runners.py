"""FATMAN command runners"""

import logging
import subprocess
import os
from os import path
from abc import ABCMeta, abstractmethod

# py2/3 compat calls
from six import raise_from, exec_

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ClientError(Exception):
    """For errors which are completely on the client side and are thus recoverable"""
    pass


class RunnerBase:
    """The runner abstract base class"""
    __metaclass__ = ABCMeta

    """Base class to implement runners"""
    def __init__(self, settings, task_dir, running_task_func):
        self._settings = settings
        self._task_dir = task_dir
        self._running_task_func = running_task_func

        self.outfiles = set()
        self.data = {
            'warnings': [],
            'errors': [],
            }
        self.finished = False
        self.success = False

    @abstractmethod
    def run(self):
        """Run the calculation"""
        pass

    @abstractmethod
    def check(self):
        """Check the state of the calculation"""
        pass


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


class DirectRunner(RunnerBase):
    """A runner to directly run jobs (in a blocking manner)"""

    def __init__(self, *args, **kwargs):
        super(DirectRunner, self).__init__(*args, **kwargs)

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

            # if we actually ran some commands, record their output
            if path.exists(stdout_fn):
                self.outfiles.add(stdout_fn)
            if path.exists(stderr_fn):
                self.outfiles.add(stderr_fn)

        self.finished = True
        raise RuntimeError("directrunner is unable to continue a 'running' job")

    def run(self):
        # since we block below, we set the task to running right away
        self._running_task_func()

        # no matter how we exit this function, the task will have terminated
        self.finished = True

        mod_env_changes = ""
        modules = self._settings['environment'].get('modules', [])

        if modules:
            with open(os.devnull, 'w') as devnull:
                mod_env_changes = subprocess.check_output(
                    map(str, ['modulecmd', 'python', 'load'] + modules),  # pylint: disable=bad-builtin
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
                    map(str, [entry['cmd']] + entry['args']),  # pylint: disable=bad-builtin
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
                self.outfiles.add(stdout_fn)
                self.outfiles.add(stderr_fn)

        self.success = True
