import tempfile
import time
import random
import uuid

from common import execute_command
import os

__author__ = 'Ahmed G. Ali'


class Job:
    def __init__(self, name, command, queue='', user='', memory=10, stdout=None, stderr=None, start_time=None):
        args = locals()  # dict of local names
        self.__dict__.update(args)  # __dict__ holds and object's attributes
        del self.__dict__["self"]
        self.start_time=start_time
        self.job_id = None
        self.cmd = None
        self.name = name
        # print(self.cmd)
        self.submitted = False
        self.out = None
        self.error = ''
        tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'tmp'))
        if stdout is None:
            self.stdout = os.path.join(tmp_dir, str(uuid.uuid4()))

        if stderr is None:
            self.stderr = os.path.join(tmp_dir, str(uuid.uuid4()))

        self.build_command()
        # print(self.cmd)
        # print(self.stdout)
        # print(self.stderr)
        # exit()

    def build_command(self):
        cmd = ['bsub']
        if self.user:
            cmd.append('-u %s' % self.user)
        if self.queue:
            cmd.append('-q %s' % self.queue)
        if self.memory:
            cmd.append('-M %s' % str(self.memory))
        if self.stdout:
            cmd.append(' -o %s' % self.stdout)
        if self.stderr:
            cmd.append(' -e %s' % self.stderr)

        self.cmd = ' '.join(cmd) + " '%s'" % self.command

    def submit(self):
        out, err = execute_command(self.cmd)
        # print(self.cmd)
        # print(out.decode())
        # print(err.decode())
        # exit()
        if out:
            self.job_id = out.split('<')[1].split('>')[0]
        self.submitted = True

    def is_alive(self):
        if self.submitted:
            cmd = 'bjobs %s' % self.job_id
            # print(cmd)
            out, err = execute_command(cmd)
            # print(out, err)
            status = [i for i in out.split('\n')[1].split(' ') if i != ''][2]

            # print(status)
            if status.lower() == 'done':
                self.extract_output()
                return False
            if status.lower() == 'run' or status.lower() == 'pend':
                return True
            else:
                self.extract_output()
                raise Exception('Job %s exited with status %s' % (self.job_id, status))
        else:
            raise Exception('Job has not been submitted yet')

    def extract_output(self):
        while not (os.path.exists(self.stderr) and os.path.exists(self.stdout)):
            time.sleep(1)
        f = open(self.stdout, 'r')
        self.out = f.read()
        f.close()
        f = open(self.stderr, 'r')
        self.error = f.read()
        f.close()
        os.remove(self.stdout)
        os.remove(self.stderr)


if __name__ == '__main__':
    j = Job("ls -lah; sleep 2; echo a7eeeh; >&2 echo 'error'", 'research-rh7', 'ahmed', 100)
    j.submit()
    a = j.is_alive()
    while a:
        print(a)
        time.sleep(5)
        a = j.is_alive()

    print('OUTPUT: \n' + j.out)
    print('ERROR: \n' + j.error)
