import csv
import datetime
from collections import OrderedDict

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

__author__ = 'Ahmed G. Ali'


class Comment:
    def __init__(self, name, val):
        self.name = name.replace('Comment', '').replace('[', '').replace(']', '').strip()
        self.val = val


class IDF:
    """
            Main class for IDF mapping. It encapsulates the data of the `IDF`.

            :param idf_path: Full path for the IDF file
            :type idf_path: str

            """

    def __init__(self, idf_path=None):
        self.lines = []
        self.rewrite = False
        # if self.skip_release_date:
        #     print(colored.magenta('Take care! Release date are not going to be edited.', bold=True))
        self.id_path = idf_path
        self.original_mapping = {}
        self.comments = []
        if idf_path:
            self._load_idf()

        # self.experiment = IDFElementSingle(dict([i for i in self.__dict__.items() if i[0].startswith('experiment')]),
        #                                    'experiment')
        # self.persons = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('person')]),
        #                                   'person')
        # self.protocols = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('protocol')]),
        #                                     'protocol')
        #
        # self.terms = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('term')]),
        #                                 'term')
        # self.comments = IDFElementMultiple(dict([i for i in self.__dict__.items() if i[0].startswith('comment')]),
        #                                    'comment')
        # self.publications = IDFElementMultiple(
        #     dict([i for i in self.__dict__.items() if i[0].startswith('publication') or i[0].startswith('pubmed')]),
        #     'publication')
        # self.__dict__['mage-tab_version'] = ['1.1']

    def add_comments(self, rows):
        for r in rows:
            self.comments.append(Comment(r[0], r[1]))

    def _load_idf(self):
        """
        Private method called with ``__init__``
        Loads IDF file.
        """
        elms = {}
        lines = []
        with open(self.id_path, encoding='utf-8', mode='r') as csvfile:
            idf_reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
            comment_rows = [r for r in idf_reader if 'Comment' in r[0]]
            self.add_comments(comment_rows)
            comment_rows = [r for r in idf_reader if 'Comment' in r[0]]
            self.add_comments(comment_rows)

            for row in idf_reader:
                if not row:
                    continue
                if row[0] == 'Investigation Title':
                    self.title = row[1]
                elif row[0] == 'Public Release Date':
                    self.release_date = row[1]
                elif row[0] == 'Experiment Description':
                    self.description = row[1]

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
    idf = IDF('/tmp/E-ATMX-23/E-ATMX-23.idf.txt')
