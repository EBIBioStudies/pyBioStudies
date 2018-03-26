from __future__ import print_function

import filecmp
import logging
import os

import shutil
import threading

import time

from common import execute_command, print_one
from settings import SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, BASE_DIRECTORY, \
    ARRAY_DIR

__author__ = 'Ahmed G. Ali'

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )


class ADFConverter(threading.Thread):
    def __init__(self, accession, dir_path, force=False):
        super(ADFConverter, self).__init__()
        print(accession, dir_path)
        self.adf_dict = {}  # map of IDF properties#
        self.accession = accession
        self.dir_path = dir_path
        self.force = force
        self.adf_file_path = None
        folder = self.accession[0:6]
        if "GEOD" in self.accession:
            folder = self.accession[0:8]  # output folder - for GEO we make smaller folders
        self.out_dir = os.path.join(BASE_DIRECTORY, folder, self.accession)
        # print (self.out_dir)
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        self.page_tab_filename = os.path.join(self.out_dir, self.accession + '.pagetab.tsv')

        self.root_path = (folder, self.accession)
        # self.file_list = os.listdir(self.dir_path)
        self.file_list = [f for f in os.listdir(self.dir_path) if not
                          os.path.isdir(os.path.join(self.dir_path, f))]


    def run(self):
        logging.debug("Start")
        if self.force:
            delete_command = "java -jar %s -o TRANKLUCATE -s %s -u %s -p %s '%s'" % (
                SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, self.accession)

            print(delete_command)
            del_out, del_err = execute_command(delete_command)
            print(del_out)
            print(del_err)

        self.convert()

        command = "java -jar %s -o createupdate -s %s -u %s -p %s -d %s" % (
            SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, self.page_tab_filename)
        print(command)
        out, err = execute_command(command)

        print(out)
        print(err)
        logging.debug("end")

    def convert(self):

        for f in self.file_list:
            des_file = os.path.join(self.out_dir, f)
            src_file = os.path.join(self.dir_path, f)
            if os.path.isdir(src_file):
                continue
            if os.path.exists(des_file):
                if not filecmp.cmp(src_file, des_file):
                    shutil.copy2(src_file, self.out_dir)
            else:
                shutil.copy2(src_file, self.out_dir)

            if f.endswith('adf.txt'):
                self.adf_file_path = os.path.join(self.dir_path, f)

        if not os.path.exists(os.path.dirname(self.page_tab_filename)):
            os.makedirs(os.path.dirname(self.page_tab_filename))
        pt_file = open(self.page_tab_filename, 'w')

        self.parse_adf()
        print("", file=pt_file)

        print("Submission\t" + self.accession + "\tPublic",
              file=pt_file)  # start writing PageTab - only for public experiments for now!!
        print_one("Title", "Array Design Name", self.adf_dict, 0, pt_file)
        print_one("ReleaseDate", "Public Release Date", self.adf_dict, 0, pt_file)
        print("RootPath\t" + "%s/%s" % self.root_path, file=pt_file)
        print("AttachTo\tArrayExpress", file=pt_file)

        print("", file=pt_file)
        print("Array\ts-" + self.accession, file=pt_file)
        print_one("Title", "Array Design Name", self.adf_dict, 0, pt_file)
        print_one("Provider", "Provider", self.adf_dict, 0, pt_file)
        print_one("Description", "Description", self.adf_dict, 0, pt_file)
        print_one("Organism", "Organism", self.adf_dict, 0, pt_file)

        additional_files = self.get_pt_files()
        if additional_files:
            print("", file=pt_file)
            print("Files\tDescription", file=pt_file)
            for item in additional_files:
                print("\t".join(item), file=pt_file)
        pt_file.close()

    def parse_adf(self):
        f = open(self.adf_file_path, "rU")
        adf_content = f.readlines()
        f.close()
        for l in adf_content:
            line = l.strip()
            if line == '':
                continue
            if line == '[main]':
                break
            lst = line.split('\t')
            if len(lst) < 1:
                continue
            a = line.split('\t')
            key = a[0].strip()
            if 'Comment' in key.strip():
                key = key.split('[')[1].replace(']', '')
            value = [i.strip() for i in a[1:]]
            for i, v in enumerate(value):  # strip quotes
                if v.startswith('"') and v.endswith('"'):
                    value[i] = v[1:-1]
            if key in self.adf_dict.keys():
                self.adf_dict[key] += value
            else:
                self.adf_dict[key] = value
        if 'Description' in self.adf_dict.keys():
            self.adf_dict['Description'][0] = self.adf_dict['Description'][0].split('<br')[0]

    def get_pt_files(self):
        readme = [f for f in self.file_list if 'readme' in f.lower()]
        files = []
        added = []
        if readme:
            readme_file = open(os.path.join(self.dir_path, readme[0]), "r")
            lines = readme_file.readlines()
            readme_file.close()
            for line in lines:
                if line.strip() == '':
                    continue

                items = line.split('\t')

                # print(len(items))
                if len(items) > 1:
                    f_dis = items[1].strip()
                    f = items[0].replace('File:', '').split('(')[0].strip()
                else:
                    f = line.replace('File:', '').strip().split(' ')[0].strip()
                    f_dis = line.strip().split(f)[1].strip()

                f_name = self.accession + '.' + '.'.join(f.split('.')[1:])
                if f_name in added:
                    continue
                if f_name in self.file_list:
                    files.append((f_name, f_dis))
                    added.append(f_name)
        for f in [i for i in self.file_list if i not in added]:
            files.append((f, "Additional File"))
        return files


def main():
    threads = []
    running = []
    counter = 0
    for d in os.listdir(ARRAY_DIR):
        a_d = os.path.join(ARRAY_DIR, d)
        if os.path.isdir(a_d):
            arrays = os.listdir(a_d)
            for array in arrays:
                # if counter ==100:
                #     break
                array_dir = os.path.join(ARRAY_DIR, d, array)
                print(array_dir)
                if os.path.isdir(array_dir):
                    adf = ADFConverter(accession=array, dir_path=array_dir, force=True)
                    adf.run()
                    # adf.setDaemon(True)
                    # threads+= [adf]
                    # counter+=1
        # if counter == 100:
        #     break

    # while threads:
    #     while len(running) < 10:
    #         t = threads.pop(0)
    #         print(t, 'NOT ALIVE: ',t.is_alive())
    #         t.start()
    #         # t.join()
    #         running+= [t]
    #
    #     for rt in running:
    #         try:
    #             rt.join(timeout=0.1)
    #         except IOError:
    #             pass
    #         print(str(rt) + 'Alive: ' + str(rt.is_alive()))
    #         if not rt.is_alive():
    #             print("removing ", rt)
    #             running.remove(rt)
    #
    #     print("%d threads running" % len(running))
    #     print("%d threads remaining " % len(threads))
    #     time.sleep(10)


if __name__ == '__main__':
    from sys import argv

    accession = argv[1]
    adf = ADFConverter(accession=accession,
                       dir_path=os.path.join(ARRAY_DIR, accession.split('-')[1], accession)
                       )
    adf.run()
    # main()
