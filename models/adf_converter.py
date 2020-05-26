import datetime
import filecmp
import shutil
from collections import OrderedDict

from ADFscore import score_adf
from common import execute_command, move_dir, register_user, truncate_dir, remove_ftp_dir, fix_encoding
from dal.biostudies.user import update_owner
from models.pagetab import PageTab, Field, Section
from settings import *

__author__ = 'Ahmed G. Ali'


class ADFConverter:
    def __init__(self, accession, out_dir):
        self.accession = accession
        self.out_dir = out_dir
        self.adf_dict = {}
        self.adf_dir = os.path.join(ARRAY_DIR, accession.split('-')[1], accession)
        self.page_tab_filename = os.path.join(self.out_dir, self.accession + '.pagetab.tsv')
        self.files = []
        self.copy_to_tmp()
        self.adf_file_path = os.path.join(self.out_dir, self.accession + '.adf.txt')
        fix_encoding(self.adf_file_path)
        self.adf_content = []
        self.parse_adf()
        # self.score = score_adf(self.adf_content)
        release_date = datetime.datetime.now().date().isoformat()
        if 'Public Release Date' in self.adf_dict.keys():
            release_date = self.adf_dict['Public Release Date'][0]
        elif 'ArrayExpressReleaseDate' in self.adf_dict.keys():
            release_date = self.adf_dict['ArrayExpressReleaseDate'][0]

        self.page_tab = PageTab(title=self.adf_dict['Array Design Name'][0],
                                accession=self.accession,
                                release_date=release_date,
                                out_dir=self.out_dir)
        if not self.adf_dict.get('Provider', ['']):
            self.adf_dict['Provider'] = ['']
        section_fields = OrderedDict(
            [
                ("Title", self.adf_dict['Array Design Name'][0]),
                ("Provider", self.adf_dict.get('Provider', [''])[0]),
                ("Description", self.adf_dict.get('Description', [''])[0]),
                ("Organism", self.adf_dict.get('Organism', [''])[0]),
                ("MIAME Compliant ADF", 'Yes' if self.adf_dict['score']['score'] else 'No'),
                ("Link", [Field(name='Link',
                                value=self.accession,
                                extras=[["Type", "BioStudies Search"], ["Description", "Studies using this array"]]
                                )])
            ]
        )
        table = None
        if self.files:
            table = [['Files', 'Type', 'Description']] + self.files
        self.page_tab.sections.append(Section(name="Array",
                                              section_id='s-' + self.accession,
                                              section_fields=section_fields,
                                              table=table))

        bst_usr = register_user(self.accession)
        # truncate_dir(self.out_dir)
        # move_dir(self.out_dir, self.accession, MAGIC_DIR, True)
        self.page_tab.export()

        command = "java -jar {jar} -s {server} -u {user} -p {password} -i {page_tab}   {files}". \
            format(jar=SUBMISSION_TOOL_PATH, server=SUBMISSION_SERVER, user=SUBMISSION_USERNAME,
                   password=SUBMISSION_PASSWORD,
                   page_tab=self.page_tab_filename,
                   files='')
        update_owner(self.accession, bst_usr['id'])
        print(command)
        out, err = execute_command(command)
        print(out)
        print(err)
        execute_command('chmod 777 -R ' + self.out_dir)
        execute_command('rm -rf ' + self.out_dir)
        # remove_dir(MAGIC_DIR,self.out_dir)
        # remove_ftp_dir(MAGIC_DIR + '/' + self.accession)

    def copy_to_tmp(self):
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        for f in os.listdir(self.adf_dir):
            f = os.path.join(self.adf_dir, f)
            if os.path.isdir(f):
                continue

            f_name = os.path.split(f)[-1]
            if f_name.startswith('.'):
                continue
            des_file = os.path.join(self.out_dir, f_name)
            # print(des_file)
            if os.path.exists(des_file):
                if not filecmp.cmp(f, des_file):
                    shutil.copy2(f, self.out_dir)
            else:
                # print("copying ", f_name)
                shutil.copy2(f, self.out_dir)
            f_parts = f.split('.')
            if len(f_parts) > 2:
                f_type = '%s File' % f_parts[-1].upper()
                f_description = '%s File' % f_parts[1].upper()
            else:
                f_type = f_description = '%s File' % f_parts[-1].upper()
            self.files.append([f_name, f_type, f_description])

    def parse_adf(self):
        try:
            f = open(self.adf_file_path, encoding='utf-8', mode="r")
            self.adf_content = f.read().replace('\r', '\n').split('\n')
        except:
            f = open(self.adf_file_path, encoding='ISO-8859-1', mode="r")
            self.adf_content = f.read().replace('\r', '\n').split('\n')
        # self.adf_content = f.readlines()

        f.close()
        self.adf_dict['score'] = score_adf(self.adf_content)
        for l in self.adf_content:
            line = l.strip()
            if line == '':
                continue
            if line == '[main]':
                print('main found')
                break
            lst = line.split('\t')
            if len(lst) < 1:
                continue
            a = line.split('\t')
            key = a[0].strip().strip('\"')
            if 'Comment' in key.strip():
                key = key.split('[')[1].replace(']', '')
            value = [i.strip().strip('\"') for i in a[1:]]
            # for i, v in enumerate(value):  # strip quotes
            #     if v.startswith('"') and v.endswith('"'):
            #         value[i] = v[1:-1]
            if key in self.adf_dict.keys():
                self.adf_dict[key] += value
            else:
                self.adf_dict[key] = value
        if self.adf_dict.get('Description', []):
            self.adf_dict['Description'][0] = self.adf_dict['Description'][0].split('<br')[0]
        else:
            self.adf_dict['Description'] = ['']


if __name__ == '__main__':
    adf_convert = ADFConverter('A-MTAB-600', '/tmp/A-MTAB-600')
