from __future__ import print_function
from subprocess import Popen, PIPE

__author__ = 'Ahmed G. Ali'


def execute_command(cmd, user=None, interactive=False):
    """
    Executes shell command with the capability of specifying executing user.

    :param cmd: The command to be executed. could be more than one command separated by ``;``
    :type cmd: str
    :param user: Unix user used for execution.
    :type: str, None
    :param interactive: if `True`, command will be executed in interactive shell. i.e. using ``$bash -i -c``
    :return: std_out, std_err of execution output
    :rtype: :obj:`tuple` of :obj:`str`
    """
    _bash = '-c'
    if interactive:
        _bash = '-i -c'
    if user:
        cmd = """sudo -H -u %s bash %s "%s" """ % (user, _bash, cmd)
    # print 'executing: ', cmd

    p = Popen([cmd], stdout=PIPE, stderr=PIPE, shell=True)
    out, err = p.communicate()

    return out, err


def print_one(print_name, prop_name, prop_array, index, print_stream):
    prop_name = prop_name.strip()
    if prop_name in prop_array.keys() and index < len(prop_array[prop_name]):
        print(print_name + "\t" + prop_array[prop_name][index], file=print_stream)


def get_one(prop_name, prop_array, index):
    prop_name = prop_name.strip()
    if prop_name in prop_array.keys() and index < len(prop_array[prop_name]):
        return prop_array[prop_name][index]
    return ""