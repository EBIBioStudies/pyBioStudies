import os

__author__ = 'Ahmed G. Ali'


class Field:
    def __init__(self, name, value, extras, separate=False):
        self.name = name
        self.extras = extras
        self.value = value
        self.separate = separate

    def get_lines(self):

        if self.separate:
            lines = ['', '{name}\t{value}'.format(name=self.name, value=self.value)]
        else:
            lines = ['{name}\t{value}'.format(name=self.name, value=self.value)]
        for extra in self.extras:
            lines.append('\t'.join(extra))
        return lines


class Section:
    def __init__(self, name, section_id='', section_fields={}, parent_id='', table=[], extra_tables=[]):
        self.id = section_id
        self.name = name
        self.fields = section_fields
        self.parent = parent_id
        self.table = table
        self.extra_tables = extra_tables
        self.out_dir = None
        self.file_list = None

    def __str__(self):
        if not self.fields and len(self.table) < 2:
            return ''
        lines = ['\t'.join([i for i in [self.name, self.id, self.parent] if i])]
        # lines = ['{}\t{}'.format()]
        # lines = ['{}\t{}'.format()]

        for k, v in self.fields.items():
            if type(v) is list:
                for val in v:
                    if type(val) is Field:
                        lines += val.get_lines()
                    else:
                        if val:
                            lines.append('\t'.join([i for i in [k, val] if i]))
            else:
                if v:
                    lines.append('\t'.join([i for i in [k, str(v)] if i]))
        if self.table:
            if self.table[0][0] == 'Files' and len(self.table) > 1000:
                self.file_list = self.id + '_filelist'
                f = open(os.path.join(self.out_dir, self.file_list), 'w')
                f.write('\n'.join(['\t'.join(i) for i in self.table]))
                lines.append('File List\t' + self.file_list)
                lines.append('')
                self.file_list = os.path.join(self.out_dir, self.file_list)
            else:
                lines.append('')
                for l in self.table:
                    lines.append('\t'.join(l))
                lines.append('')
        if self.extra_tables:
            for table in self.extra_tables:
                if table[0][0] == 'Files' and len(table) > 1000:
                    self.file_list = self.id + '_filelist'
                    f = open(os.path.join(self.out_dir, self.file_list), 'w')
                    f.write('\n'.join(['\t'.join(i) for i in self.table]))
                    lines.append('File List\t' + self.file_list)
                    lines.append('')
                    self.file_list = os.path.join(self.out_dir, self.file_list)
                else:
                    lines.append('')
                    for l in table:
                        lines.append('\t'.join(l))
                    lines.append('')
        lines.append('')
        lines.append('')
        return "\n".join(lines)


class PageTab:
    def __init__(self, title, accession, release_date, out_dir, is_double_blind=False):
        self.title = title
        self.accession = accession
        self.release_date = release_date
        self.out_dir = out_dir
        self.sections = []
        self.is_double_blind = is_double_blind

    def export(self):

        txt = '\n'.join(['Submission\t' + self.accession,
                         'Title\t' + self.title,
                         'ReleaseDate\t' + self.release_date,
                         'RootPath\t' + self.accession,
                         'AttachTo\tArrayExpress'])
        if self.is_double_blind:
            txt += '\nReviewType\tDoubleBlind\n'
        txt += '\n\n'
        file_lists = []
        for section in self.sections:
            section.out_dir = self.out_dir
            txt += str(section)
            if section.file_list:
                file_lists.append(section.file_list)
        # print(type(txt))
        # print(txt)
        # print(txt.encode(encoding = 'UTF-8',errors = 'strict'))
        # f = open(os.path.join(self.out_dir, self.accession + '.pagetab.tsv'), 'w')

        txt = txt.encode(encoding='UTF-8', errors='strict')
        with open(os.path.join(self.out_dir, self.accession + '.pagetab.tsv'), encoding='utf-8', mode='w') as f:
            f.write(txt.decode('utf-8'))
            # f.write(txt)
        # f.close()
        # exit()
        return file_lists
        # return txt
