import argparse
import filecmp
import shutil
import zipfile
from distutils.dir_util import copy_tree
from ftplib import FTP

from common import execute_command, move_dir, remove_dir, register_user, truncate_dir, remove_ftp_dir
from dal.biostudies.user import update_owner
from models.magetab_converter import MAGETABConverter
from settings import *

__author__ = 'Ahmed G. Ali'


def extract_args():
    parser = argparse.ArgumentParser(
        description='Converts a given ArrayExpress experiment into a BioStudy and submit it to BioStudies database.')
    parser.add_argument('accession', metavar='E-MTAB-xxxx', type=str,
                        help='''The accession number for the ArrayExpress Study''')
    parser.add_argument('-efo',
                        '--efo',
                        help='EFO serializer path')
    parser.add_argument('-sc',
                        '--skip_copy',action='store_true',
                        help='Skip copying files to user directory')
    parser.add_argument('-sr',
                        '--skip_remove',action='store_true',
                        help='Skip removing files after submission')
    return parser.parse_args()


def main():
    args = extract_args()
    dir_path = os.path.join(LOAD_DIR, args.accession.split('-')[1], args.accession)

    tmp_dir = os.path.join(MAGIC_DIR, args.accession)
    skip_copy = args.skip_copy
    skip_remove = args.skip_remove
    if os.path.exists(tmp_dir):
        execute_command('chmod 777 -R ' + tmp_dir)
        execute_command('rm -rf ' + tmp_dir)
    try:
        os.mkdir(tmp_dir)
        if skip_copy:
            print('skipping copy')
            execute_command('cp -r %s/*.txt %s' % (dir_path, tmp_dir))
        else:
            execute_command('cp -r %s/* %s' % (dir_path, tmp_dir))
            # copy_tree(dir_path, tmp_dir)
            for f in os.listdir(tmp_dir):
                f = os.path.join(tmp_dir, f)
                if os.path.isdir(f):
                    continue
                f_name = os.path.split(f)[-1]

                if f_name.endswith('.zip'):
                    print("extracting:: " + f_name + ' to ' + tmp_dir)
                    execute_command("Unzip %s -d %s" % (f, tmp_dir))
                    # zip_file = zipfile.ZipFile(f, 'r')
                    # zip_file.extractall(tmp_dir)
                    # zip_file.close()


        converter = MAGETABConverter(args.accession, tmp_dir)
        file_lists = converter.page_tab.export()
        bst_usr = register_user(args.accession)
        # truncate_dir(tmp_dir)
        # move_dir(tmp_dir, args.accession, MAGIC_DIR, True)

        files = ''
        if file_lists:
            files = ' -a ' + ','.join(file_lists)

        command = "java -jar {jar} -s {server} -u {user} -p {password} -i {page_tab}   {files}". \
            format(jar=SUBMISSION_TOOL_PATH, server=SUBMISSION_SERVER, user=SUBMISSION_USERNAME,
                   password=SUBMISSION_PASSWORD,
                   page_tab=os.path.join(converter.out_dir, args.accession + '.pagetab.tsv'),
                   files=files)

        print(command)
        out, err = execute_command(command)
        print(out)
        print(err)
        update_owner(args.accession, bst_usr['id'])
        # remove_ftp_dir(MAGIC_DIR + '/' + args.accession)
        if not skip_remove:
            execute_command('chmod 777 -R ' + tmp_dir)
            execute_command('rm -rf '+tmp_dir)
        # remove_dir(magic_dir, tmp_dir)
    except Exception as e:
        print(e)
        if not skip_remove:
            execute_command('chmod 777 -R ' + tmp_dir)
            execute_command('rm -rf ' + tmp_dir)
        raise


if __name__ == '__main__':
    main()
