import datetime
import os
from random import shuffle
from time import sleep

from dal.ae2.ae2_transaction import retrieve_public_experiments
from lsf.job import Job
import settings
import logging

__author__ = 'Ahmed G. Ali'

log_file = datetime.datetime.utcnow().isoformat()
log = os.path.join(os.path.dirname(os.path.realpath(__file__)), log_file)
logging.basicConfig(filename=log,
                    level=logging.DEBUG)


# print (os.path.dirname(os.path.realpath(__file__)))
# exit()

def get_command(acc):
    return "export PYTHONPATH=\"${PYTHONPATH}:/nfs/biostudies/pyBiostudies\"; export " \
           "LD_LIBRARY_PATH=/nfs/biostudies/instantclient_12_2:$LD_LIBRARY_PATH;source " \
           "/nfs/biostudies/pyBiostudies/virtual_env/bin/activate;python /nfs/biostudies/pyBiostudies/MT2PT_class.py " \
           "" + acc


def main():
    exp = retrieve_public_experiments()[0]
    jobs = []
    shuffle(exp)
    # exp = exp[:100]
    if exp:
        while True:
            print('%d Jobs Running, %d remaining' % (len(jobs), len(exp)))
            while len(jobs) <= 100 and exp:
                e = exp.pop()
                d = e.acc
                a_d = os.path.join(settings.LOAD_DIR, d.split('-')[1], d)
                if os.path.isdir(a_d):
                    job = Job(name=d, command=get_command(acc=d), queue=settings.LSF_QUEUE, user=settings.LSF_USER,
                              memory=5000)
                    job.submit()
                    jobs.append(job)
            if not jobs:
                break

            for j in jobs:
                try:
                    live = j.is_alive()
                except Exception as e:
                    print(e)
                    live = False
                    logging.error(str(e) + j.error)
                if not live:
                    if j.error or (j.out is not None and 'error' in j.out.lower()):
                        logging.error('Error in %s\nstd_out:%s\nstd_err:%s' % (j.name, j.out, j.error))
                    else:
                        logging.info('%s OK!\n-----------\n' % j.name)
                    print('removing %s' % j.name)
                    jobs.remove(j)
            sleep(10)
    print("Logs are in: " + log)


if __name__ == '__main__':
    main()
