import os
import re
import string
from collections import OrderedDict

import chardet
import datetime
import csv
from clint.textui import colored
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import unicodedata

from common import fix_encoding
from dal.ae2.study import retrieve_release_date

__author__ = 'Ahmed G. Ali'


class IDFElementSingle:
    """
        Single complex IDF entry. e.g. Experiment

        :param exps: JSON object of fields of the entry
        :type exps: dict with :obj:`str` values
        :param reg: Common name in the fields to be removed
        :type reg: str
        """

    def __init__(self, exps, reg):
        elms = {}
        for title, val in exps.items():
            if not val:
                val = ['']
            elms[title.replace(reg + '_', '')] = val[0]
        self.__dict__.update(elms)


class IDFElementMultiple(list):
    """
    Muliple complex IDF entry. e.g. Person and Protocol

    :param exps: JSON object of fields of the entry
    :type exps: dict with :obj:`list` of :obj:`str` values
    :param reg: Common name in the fields to be removed
    :type reg: str
    """

    def __init__(self, exps, reg):
        super(IDFElementMultiple, self).__init__()
        lst = [len(vals) for t, vals in exps.items()]
        if not lst:
            max_val = 0
        else:
            max_val = max([len(vals) for t, vals in exps.items()])
        elements = []
        for i in range(max_val):
            element = {}
            for k in exps.keys():
                if len(exps[k]) > i:
                    if exps[k][i]:
                        element[k] = [exps[k][i]]
                else:
                    element[k] = [None]
            elements.append(element)
        for element in elements:
            if element:
                self.append(IDFElementSingle(element, reg))


class IDF:
    """
            Main class for IDF mapping. It encapsulates the data of the `IDF` as follows:
                - Single `string` entry: stored as :obj:`str`
                - Complex single entry: stored as an instance of :ref:`IDFElementMultiple`.
                    e.g. experiment attributes below construct single object ``self.experiment``
                        - Experiment Description
                        - Experimental Design
                        - Experimental Design Term Source REF
                        - Experimental Design Term Accession Number
                        - Experimental Factor Name
                        - Experimental Factor Type
                        - Experimental Factor Term Source REF
                        - Experimental Factor Term Accession Number
                - Complex multiple entry: stored as instance of :ref:`IDFElementMultiple`. e.g. Person objects.

            :param idf_path: Full path for the IDF file
            :type idf_path: str
            :param combined: when ``True``, the IDF and SDRF are in a single file, Default: ``False``
            :type combined: bool
            :param skip_release_date: when ``False``, earlier or past release date will be changed to at least 10 days in the
                future
            """

    def __init__(self, idf_path=None,accession=None,  combined=False, skip_release_date=False):
        self.lines = []
        self.rewrite = False
        self.skip_release_date = skip_release_date

        # if self.skip_release_date:
        #     print(colored.magenta('Take care! Release date are not going to be edited.', bold=True))
        self.id_path = idf_path
        self.combined = combined
        self.original_mapping = {}
        if idf_path:
            self.__load_idf()
        self.accession = accession
        if not hasattr(self, 'public_release_date'):
            self.public_release_date = [retrieve_release_date(self.accession)[0]['releasedate'].date().isoformat()]
        self.experiment = IDFElementSingle(dict([i for i in self.__dict__.items() if i[0].startswith('experiment')]),
                                           'experiment')
        self.persons = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('person')]),
                                          'person')
        self.protocols = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('protocol')]),
                                            'protocol')

        self.terms = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('term')]),
                                        'term')
        self.comments = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('comment')]),
                                           'comment')
        self.publications = IDFElementMultiple(
            dict([i for i in self.__dict__.items() if i[0].startswith('publication') or i[0].startswith('pubmed')]),
            'publication')
        self.__dict__['mage-tab_version'] = ['1.1']

    def __load_idf(self):
        """
        Private method called with ``__init__``
        Loads IDF file.
        """
        fix_encoding(self.id_path)
        elms = {}
        lines = []
        with open(self.id_path, encoding='utf-8', mode='r') as csvfile:
            idf_reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
            for row in idf_reader:
                # print(row)
                if not row:
                    continue
                row= [i.strip().replace('^M', '') for i in row]
                # print(row)
                # row = [unicodedata.normalize('NFKD', i) for i in row]
                # for i in range(len(row)):
                # line = row[i]
                # line = line
                # line = unicodedata.normalize('NFKD', line)
                # line = u"".join([c for c in line if not unicodedata.combining(c)])
                # line = filter(lambda x: x in string.printable, line)
                # row[i] = line

                if self.combined:
                    if '[idf]' in row[0].lower() or row == ['']:
                        continue
                    if '[sdrf]' in row[0].lower():
                        break
                line = '\t'.join(row)
                self.rewrite = True
                # line = filter(lambda x: x in string.printable, line)
                lines.append(line)
                # if row[0] == 'Public Release Date':
                #     if parse(row[1]) < datetime.datetime.now() + datetime.timedelta(
                #             days=10) and not self.skip_release_date:
                #         self.rewrite = True
                #         line = line.replace(row[1], (datetime.date.today() + datetime.timedelta(days=10)).isoformat())
                #         row[1] = (datetime.date.today() + datetime.timedelta(days=10)).isoformat()
                #         # lines[i] = line.encode('utf8', 'ignore')
                #
                #     elif parse(row[1]) > (datetime.datetime.now() + relativedelta(years=2)):
                #         self.rewrite = True
                #         line = line.replace(row[1], (datetime.date.today() + relativedelta(years=2)).isoformat())
                #         row[1] = (datetime.date.today() + relativedelta(years=2)).isoformat()
                        # lines[i] = line.encode('utf8', 'ignore')
                self.original_mapping[row[0].lower().replace(' ', '_')] = row[0]
                elms[row[0].lower().replace(' [', '_').replace(' ', '_').replace('[', '_').replace(']', '').replace(':', '_').strip().strip('\"')] = \
                    [i.strip().strip('\"') for i in row[1:]]
                if not line.startswith('SDRF File'):
                    self.lines.append(line)

        self.__dict__.update(elms)

    def generate_idf(self):
        """
        Exports IDF to as one string.

        """
        fields = OrderedDict([
            ('MAGE-TAB Version', '1.1'),
            ('Investigation Title', ''),
            ('Experiment Description', None),
            ('Experimental Design', None),
            ('Experimental Design Term Source REF', None),
            ('Experimental Design Term Accession Number', None),
            ('Experimental Factor Name', None),
            ('Experimental Factor Type', None),
            ('Experimental Factor Term Source REF', None),
            ('Experimental Factor Term Accession Number', None),
            ('Person Last Name', None),
            ('Person First Name', None),
            ('Person Mid Initials', None),
            ('Person Email', None),
            ('Person Phone', None),
            ('Person Fax', None),
            ('Person Address', None),
            ('Person Affiliation', None),
            ('Person Roles', None),
            ('Date of Experiment', None),
            ('Public Release Date', None),
            ('Protocol Name', None),
            ('Protocol Type', None),
            ('Protocol Term Source REF', None),
            ('Protocol Term Accession Number', None),
            ('Protocol Description', None),
            ('Protocol Hardware', None),
            ('Protocol Software', None),
            ('Term Source Name', None),
            ('Term Source File', None),
            ('Term Source Version', None)
        ])
        if hasattr(self, 'comments'):
            for c, v in self.comments.items():
                fields['Comment[%s]' % c] = v

        for k, v in fields.items():
            fields[k] = '\t'.join(getattr(self, k.lower().replace(' ', '_'), []))
        idf_lines = []
        for h, v in fields.items():
            idf_lines.append('%s\t%s' % (h, v))
        return '\n'.join(idf_lines)


if __name__ == '__main__':
    idf = IDF('/home/gemmy/PycharmProjects/pyBioStudies/experiments/MTAB/E-MTAB-5179/E-MTAB-5179.idf.txt')
    print(idf)
