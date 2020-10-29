from __future__ import print_function

import os
import uuid
from ftplib import FTP
from subprocess import Popen, PIPE

import requests

from dal.ae2.ae2_transaction import retrieve_owner
from dal.biostudies.user import retrieve_user_by_email, insert_user, update_owner, activate_user_by_id
from settings import *

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

    return out.decode('utf8'), err.decode('utf8')


def print_one(print_name, prop_name, prop_array, index, print_stream):
    prop_name = prop_name.strip().lower().replace(' ', '')
    if prop_name in prop_array.keys() and index < len(prop_array[prop_name]):
        print(print_name + "\t" + prop_array[prop_name][index], file=print_stream)


def get_one(prop_name, prop_array, index):
    dct = {}
    # for k, v in prop_array.items():
    #     dct[k.lower().replace(' ', '')] = v
    prop_name = prop_name.strip().lower().replace(' ', '')
    if prop_name in prop_array.keys() and index < len(prop_array[prop_name]):
        return prop_array[prop_name][index].strip()
    # else:
    # print(prop_name)
    # print(prop_array)
    # exit()
    return ""


def create_magic_folder(u_id, secret):
    folder_path = os.path.join(DROPBOX, secret[:2], secret[2:] + '-a' + str(u_id))
    os.makedirs(name=folder_path, mode=0o777, exist_ok=True)


def create_user(username, password, email, f_name, l_name):
    secret = str(uuid.uuid4())
    u = retrieve_user_by_email(email)
    user_id = None
    if not (u and len(u) > 0):
        insert_user(email=email, full_name='%s %s' % (f_name, l_name),
                    username=username, password=password, secret=secret)
        u = retrieve_user_by_email(email)
        u_id = u[0]['id']
    else:
        u_id = u[0]['id']
    create_magic_folder(u_id, secret)


def migrate_owner(accession):
    owner = retrieve_owner(accession)
    email = None
    if owner and len(owner) > 0:
        # print(owner[0])
        # exit()
        username = owner[0].username
        password = owner[0].userpassword
        email = owner[0].useremail
        first_name = owner[0].firstname
        last_name = owner[0].lastname
        secret = str(uuid.uuid4())
        if email:
            u = retrieve_user_by_email(email)
            if not (u and len(u) > 0):
                insert_user(email=email, full_name='%s %s' % (first_name, last_name),
                            username=username, password=password, secret=secret)
            u = retrieve_user_by_email(email)
            u_id = u[0]['id']
            print(u_id)
            update_owner(accession=accession, new_user=u_id)
    return email


def move_dir(full_path, dir_name, magic_dir, create=False):
    print(full_path, dir_name, magic_dir, )
    ftp = FTP()
    ftp.encoding = 'utf-8'
    ftp.connect(host=FTP_URL, port=8035)
    ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
    ftp.cwd(magic_dir)
    f_lst = ftp.nlst()
    if create:
        if dir_name not in ftp.nlst():
            ftp.mkd(dir_name)
        ftp.cwd(dir_name)
    for f in os.listdir(full_path):
        if f.startswith('.'):
            continue
        file_path = os.path.join(full_path, f)
        if os.path.isdir(file_path):
            move_dir(file_path, f, magic_dir + '/' + dir_name, True)
        else:
            fp = open(file_path, 'rb')
            # print('STOR %s' % f)
            ftp.storbinary('STOR %s' % f, fp, 1024)
    if create:
        ftp.cwd('../')


def isdir(ftp, name):
    try:
        ftp.cwd(name)
        ftp.cwd('..')
        return True
    except:
        return False


def remove_ftp_dir(dir_name):
    print(dir_name)
    ftp = FTP()
    ftp.connect(host=FTP_URL, port=8035)
    ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
    # ftp.sendcmd('mdelete %s/*' % dir_name)
    # ftp.sendcmd('rmdir ' + dir_name)
    ftp.cwd(dir_name)
    f_lst = ftp.nlst()

    for f in f_lst:
        if isdir(ftp, f):
            remove_ftp_dir(dir_name + '/' + f)
        else:
            ftp.delete(f)
    # ftp.rmd(dir_name)


def remove_dir(magic_dir, full_path):
    ftp = FTP()
    ftp.connect(host=FTP_URL, port=8035)
    ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
    ftp.cwd(magic_dir)
    for f in os.listdir(full_path):
        file_path = os.path.join(full_path, f)
        if os.path.isdir(file_path):
            continue

        # fp = open(file_path, 'rb')
        try:
            ftp.delete(f)
        except:
            pass
    # if create:
    #     ftp.cwd('../')


def register_user(accession):
    owner = retrieve_owner(accession)
    # print(owner)
    if owner and len(owner) > 0:
        email = owner[0].useremail.split(',')[0].strip()
        res = retrieve_user_by_email(email)
        bst_usr = None
        if res and len(res) > 0:
            bst_usr = res[0]
        if not bst_usr:
            name = ' '.join([i for i in [owner[0].firstname, owner[0].midinitials, owner[0].lastname] if i is not None])
            URL = SIGNUP_URL
            # print(owner[0])
            PARAMS = {
                'name': name,
                'email': email + '_1',
                'password': owner[0].userpassword,
                'username': owner[0].username,
                'instanceKey': BACKEND_INSTANCE_KEY,
                'path': '/#/activate'

            }
            r = requests.post(url=URL, json=PARAMS)
            print(r.status_code)
            bst_usr = retrieve_user_by_email(email + '_1')[0]
            activate_user_by_id(u_id=bst_usr['id'], email=email)
            bst_usr = retrieve_user_by_email(email)[0]
        bst_usr['password'] = owner[0].userpassword
        if bst_usr['login'] is None:
            bst_usr['login'] = email
    else:
        res = retrieve_user_by_email(SUBMISSION_USERNAME)
        bst_usr = res[0]
        bst_usr['password'] = SUBMISSION_PASSWORD
    secret = bst_usr['secret']
    # magic_dir = secret[:2] + '/' + secret[2:] + '-a' + str(bst_usr['id'])
    return bst_usr


def truncate_dir(tmp_dir):
    for f_name in os.listdir(tmp_dir):
        if '.idf' in f_name or '.sdrf' in f_name or '.adf' in f_name or '.pagetab' in f_name or 'filelist' in f_name or os.path.isdir(
                os.path.join(tmp_dir, f_name)):
            continue
        cmd = "truncate -s 10 " + os.path.join(tmp_dir, f_name)
        # print(cmd)
        execute_command(cmd)
        # print('=' * 30)


# if __name__ == '__main__':
#     # move_dir('/nfs/biostudies/pyBiostudies/tmp/E-MTAB-6268', 'E-MTAB-6268', '2b/4c944c-7bd9-4d46-a345-8697c3eaaccf-a13', True)
#     register_user('E-GEOD-537')


def fix_encoding(file_path):
    # f = open(self.adf_file_path, 'r')
    # c = f.read()
    # f.close()
    # f = open(self.adf_file_path,encoding='utf-8',mode='w')
    # f.write(c)
    # c.close()
    #
    # print( execute_command('file -bi '+self.adf_file_path))
    # return
    # print('file -bi ' + file_path)
    out, err = execute_command('file -bi ' + file_path)
    charset = (out + err).split('=')[1].strip()
    if 'unknown' in charset or 'binary' in charset:
        execute_command('cat -v %s > %s' % (file_path, file_path + '_tmp'))
        execute_command('mv %s  %s' % (file_path + '_tmp', file_path))
        out, err = execute_command('file -bi ' + file_path)
        charset = (out + err).split('=')[1].strip()
    if charset != 'utf-8':
        execute_command('iconv -f {charset} -t UTF-8 {adf} > {adf_tmp}'.format(charset=charset, adf=file_path,
                                                                               adf_tmp=file_path + '_tmp'))
        execute_command('rm -f ' + file_path)
        execute_command('mv %s %s ' % (file_path + '_tmp', file_path))


if __name__ == '__main__':
    register_user('E-MTAB-5200')
