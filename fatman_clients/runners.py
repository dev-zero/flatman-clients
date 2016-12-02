"""FATMAN command runners"""

import logging
import subprocess
import os
from os import path
from abc import ABCMeta, abstractmethod
from itertools import chain

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
    def __init__(self, settings, task_dir):
        self._settings = settings
        self._task_dir = task_dir

        self.outfiles = set()
        self.data = {
            'warnings': [],
            'errors': [],
            }
        self.finished = False
        self.success = False

    @abstractmethod
    def run(self, running_task_func):
        """Run the calculation.

        The function running_task_func is called as soon as the task is running."""
        pass

    @abstractmethod
    def check(self):
        """Check the state of the calculation"""
        pass


class SlurmRunner(RunnerBase):
    """Runner implementation to run FATMAN tasks via SLURM"""

    def __init__(self, *args, **kwargs):
        super(SlurmRunner, self).__init__(*args, **kwargs)

    def check(self):
        """Use squeue and sacct to check for the task."""

        tname = self._settings['name']

        # squeue does not have a --parsable flag
        # what we want is: JOBID|STATE|TIME|NODES
        squeue_out = subprocess.check_output(
            ['squeue', '--noheader', '--format=%i|%T|%M|%D', '--name=' + tname])
        squeue_out = squeue_out.strip()

        # TODO: we might want to store some data in the database:
        # jobid, state, time, nodes = squeue_out.split('|')

        # Anyway, if the job is still in the slurm queue, we can expect it
        # to be either pending, or running or to be terminated.
        # In the latter case we catch eventually it in the next iteration
        if squeue_out:
            return

        # if the job isn't in the queue anymore, it is surely done
        self.finished = True

        # Check the slurm database for more information
        sacct_out = subprocess.check_output(
            ['sacct', '--long', '--parsable2', '--name=' + tname])
        sacct_lines = sacct_out.strip().split('\n')

        # we keep the header for this command to use the columns as keys
        if len(sacct_lines) < 2:
            raise RuntimeError("job could not be found using either squeue or sacct")

        # extract the header column names
        headers = [s.lower() for s in sacct_lines[0].split('|')]
        # .. and convert each line to a dict and store them in a dict,
        # with the jobname as key to match them to our commands
        sacct_data = {d['jobname']: d for d in [dict(zip(headers, l.split('|'))) for l in sacct_lines[1:]]}

        # we are going to upload all metadata in the runner key
        self.data['runner'] = {'commands': sacct_data}

        # check the parent job:
        if sacct_data[tname]['state'] == 'COMPLETED':
            self.success = True

        # try to extract errors from the sacct data
        for entry in self._settings['commands']:
            name = entry['name']
            try:
                if sacct_data[name]['state'] != 'COMPLETED':
                    # if the entire job succeeded, we obviously decided to ignore the error
                    # for this this command
                    # TODO: if the main job failed, we currently report all failed commands
                    #       as errors, even though we decided to ignore their errors
                    self.data['warnings' if self.success else 'errors'].append({
                        'tag': 'commands',
                        'entry': name,
                        'msg': "command terminated with non-zero exit status",
                        'returncode': sacct_data[name]['exitcode'],
                        })

                    # the srun output files might be of interest if they exist
                    stdout_fn = path.join(self._task_dir, "{}.out".format(name))
                    stderr_fn = path.join(self._task_dir, "{}.err".format(name))
                    if path.exists(stdout_fn):
                        self.outfiles.add(stdout_fn)
                    if path.exists(stderr_fn):
                        self.outfiles.add(stderr_fn)

            except KeyError:
                # skipped commands do not appear at all in the sacct list of jobs
                pass

    def run(self, running_task_func):
        stdout_fn = path.join(self._task_dir, "sbatch.out")
        stderr_fn = path.join(self._task_dir, "sbatch.err")

        logger.info("running sbatch")

        try:
            stdout = open(stdout_fn, 'w')
            stderr = open(stderr_fn, 'w')
        except (OSError, IOError) as exc:
            raise_from(
                ClientError("error when opening {}".format(exc.filename)),
                exc)

        try:
            subprocess.check_call(['sbatch', "run.sh"],
                                  stdout=stdout, stderr=stderr,
                                  cwd=self._task_dir)
            running_task_func()

        except subprocess.CalledProcessError as exc:
            self.data['errors'].append({
                'tag': 'commands',
                'entry': 'sbatch',
                'msg': "command terminated with non-zero exit status",
                'returncode': exc.returncode,
                })
            self.finished = True
            raise

        except Exception as exc:
            self.data['errors'].append({
                'tag': 'commands',
                'entry': 'sbatch',
                'msg': "error occurred while running: {}".format(exc),
                })
            self.finished = True
            raise

        finally:
            stdout.close()
            stderr.close()
            self.outfiles.add(stdout_fn)
            self.outfiles.add(stderr_fn)


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

    def run(self, running_task_func):
        # since we block below, we set the task to running right away
        running_task_func()

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


class MPIRunner(DirectRunner):
    """A runner to directly run jobs via mpirun (in a blocking manner)
       This re-uses the direct runner, by patching the settings to prefix them with the mpirun options.
    """

    def __init__(self, *args, **kwargs):
        super(MPIRunner, self).__init__(*args, **kwargs)

        mpirun_args = [chain.from_iterable(("--{}".format(arg), value)
                       for arg, value in self._settings['machine'].get('mpirun_args', {}).items())]

        for command in self._settings['commands']:
            # the new arguments are all passed to mpirun
            command['args'] = mpirun_args + [command['cmd']] + command['args']
            command['cmd'] = "mpirun"
