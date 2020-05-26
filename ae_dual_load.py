from common import execute_command
from settings import *

__author__ = 'Ahmed G. Ali'
import os


def main():
    unload_accs = os.listdir(os.path.join(STAGING_DIR, 'unload'))
    load_accs = os.listdir(os.path.join(STAGING_DIR, 'load'))
    # reload_accs = list(set(unload_accs).intersection(load_accs))
    # unload_accs = list(set(unload_accs) - set(reload_accs))
    # load_accs = list(set(load_accs) - set(reload_accs))

    for acc in unload_accs:
        print('unloading:', acc)
        delete_command = "java -jar %s -o TRANKLUCATE -s %s -u %s -p %s '%s'" % (
            SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, acc)
        # print(delete_command)
        # print(execute_command(delete_command))
        execute_command(delete_command)
        os.remove(os.path.join(STAGING_DIR, 'unload', acc))
        # os.remove(os.path.join(STAGING_DIR, 'unload', acc))

    runner_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lsf', 'runner.py')
    if load_accs:
        # print('loading:', acc)
        load_command = "export PYTHONPATH=\"${PYTHONPATH}:/nfs/biostudies/pyBiostudies\"; export " \
          "LD_LIBRARY_PATH=/nfs/biostudies/instantclient_12_2:$LD_LIBRARY_PATH;source " \
          "/nfs/biostudies/pyBiostudies/virtual_env/bin/activate;python %s %s" % (runner_path, ' '.join(load_accs))
        # print(load_command)
        print(execute_command(load_command))
    for acc in load_accs:
        os.remove(os.path.join(STAGING_DIR, 'load', acc))


if __name__ == '__main__':
    main()
