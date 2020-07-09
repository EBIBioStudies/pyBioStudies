import datetime
# import logging
import os
from time import sleep

import settings
from dal.ae2.ae2_transaction import retrieve_all_experiments
from dal.biostudies.biostudies_transaction import get_root_section_by_accession, get_ae_submissions
from lsf.job import Job

__author__ = 'Ahmed G. Ali'

log_file = 'RUNNER_' + datetime.datetime.utcnow().isoformat().split('.')[0].replace(':', '_')
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), log_file + '.csv')
f_logger = open(log_file, 'a')


# import logging

# logger = logging.getLogger(__name__)
# #logger.setLevel(logging.DEBUG)

# create a file handler
# handler = logging.FileHandler(log_file)
# handler.setLevel(logging.DEBUG)

# create a logging format
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)

# add the handlers to the logger
# #logger.addHandler(handler)


class Experiment:
    def __init__(self, acc):
        self.acc = acc


def get_exp_command(acc, skip_copy=False):
    cmd = "export PYTHONPATH=\"${PYTHONPATH}:/nfs/biostudies/pyBiostudies\"; export " \
          "LD_LIBRARY_PATH=/nfs/biostudies/instantclient_12_2:$LD_LIBRARY_PATH;source " \
          "/nfs/biostudies/pyBiostudies/virtual_env/bin/activate;python /nfs/biostudies/pyBiostudies/pagetab_loader.py" \
          " " + acc
    if skip_copy:
        cmd += ' -sc'
    return cmd


def main(acc=None, limit=0, ):
    # print('Starting EFO')
    # efo = EFOCollection()
    # print('Ending EFO')
    # tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'tmp'))
    # serializer_path = os.path.join(tmp_dir, 'efo.binary')
    # serializer_file = open(serializer_path, 'wb')
    # pickle.dump(efo, serializer_file)
    # serializer_file.close()
    exp = []
    in_run = []
    # loaded_exps = []
    added_experiments = []
    if acc:
        for a in acc:
            exp.append(a)
    else:

        exps, con = retrieve_all_experiments()
        print('exp len %d' % len(exps))
        # if limit:
        # print('setting limit to %d experiments' % limit)
        # exps = exps[:limit]
        exp = sorted(list(set([e['acc'] for e in exps])))
        # for e in exps:
        #     # if e['acc'] not in added_experiments:
        #     exp.append(e['acc'])

    jobs = []
    added_experiments = sorted([i['accNo'] for i in get_ae_submissions()])
    # new_exp = list(set([i.acc for i in exp if i.acc not in added_experiments]))[:100]

    # added_experiments = []
    # added_arrays = {}
    # exp_to_load = [i for i in exp if i not in added_experiments ]
    exp_to_load = list(set(exp).difference(set(added_experiments)))
    # exp_to_load = exp
    print('Loading %d Experiments' % len(exp_to_load))

    counter = -1
    if exp_to_load:
        # while True and counter < limit:
        while True:
            while len(jobs) <= 20 and exp_to_load and counter < limit:
                d = exp_to_load.pop()

                if d in in_run:
                    continue

                # db_exp = get_root_section_by_accession(d)
                # if db_exp:
                #     added_experiments.append(d)
                #     continue

                # a_d = os.path.join(settings.LOAD_DIR, d.split('-')[1], d)
                # if os.path.isdir(a_d):
                in_run.append(d)
                skip_copy = False
                if d in added_experiments:
                    skip_copy = True
                job = Job(name=d, command=get_exp_command(acc=d, skip_copy=skip_copy),
                          queue=settings.LSF_QUEUE, user=settings.LSF_USER, memory=3072,
                          start_time=datetime.datetime.utcnow().isoformat())
                job.submit()
                jobs.append(job)
                if limit:
                    counter += 1

            if not jobs:
                break

            for j in jobs:
                # print(j)
                try:
                    # print("Checking alive " + j.job_id)
                    live = j.is_alive()
                    # print(live)
                except Exception as e:
                    # print(e)
                    live = False
                    f_logger.writelines(['\t'.join([j.name, j.start_time, datetime.datetime.utcnow().isoformat(),

                                                    "%s" % (str(e) + j.error)]) + '\n'])

                    # logger.error(str(e) + j.error)
                if not live:
                    # if j.error or (j.out is not None and 'error' in j.out.lower()):
                    if j.error or (j.out is not None and 'ERROR' in j.out):
                        # print('Error in %s\nstd_out:%s\nstd_err:%s' % (j.name, j.out, j.error))
                        f_logger.writelines(['\t'.join([j.name, j.start_time, datetime.datetime.utcnow().isoformat(),
                                                        '"Error :%s"' % j.error]) + '\n'])
                        # logger.error('Error in %s\nstd_out:%s\nstd_err:%s' % (j.name, j.out, j.error))
                    else:
                        # print('%s OK!\n-----------\n' % j.name)
                        f_logger.writelines(['\t'.join([j.name, j.start_time, datetime.datetime.utcnow().isoformat(),
                                                        "Loaded"]) + '\n'])
                        # logger.info('%s OK!\n-----------\n' % j.name)
                        # print(j.cmd)
                        # print(j.out)
                        # print(j.error)
                        # print('=' * 30)
                    # print('removing %s' % j.name)
                    if j in jobs:
                        jobs.remove(j)
            print("%d jobs running!" % len(jobs))

            sleep(10)
            # if limit:
            #     counter+=1
    f_logger.close()
    # print("Logs are in: " + log)


if __name__ == '__main__':
    # logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    # logging.warning('This will get logged to a file')
    from sys import argv

    acc = None
    if len(argv) > 1:
        acc = argv[1:]
    number = 0
    # force = False
    # print(acc)
    try:
        number = int(acc[0])
        acc.remove(acc[0])

    except:
        pass
    # exit()
    # try:
    #     if '-f' in acc:
    #         force = True
    #         acc.remove('-f')
    # except:
    #     pass
    # print(acc)
    main(acc, number)
