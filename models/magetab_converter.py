import datetime
import filecmp
import os
import shutil
from collections import OrderedDict
from itertools import groupby, count

import requests

import MIAMEscore
# from ADFscore import score_adf
from checkers import is_microarray
from dal.ae2.ae2_transaction import retrieve_arrays_by_exp_acc
from dal.biostudies.biostudies_transaction import get_root_section_by_accession
from dal.biostudies.submittion import retrieve_submission_by_acc
from models.adf_converter import ADFConverter
from models.idf import IDF
from models.pagetab import PageTab, Section, Field
from models.sdrf import SdrfCollection
from settings import *
from utils.efo import EFOCollection

__author__ = 'Ahmed G. Ali'


def as_range(g):
    l = list(g)
    return l[0], l[-1]


def get_ena_link(runs):
    accessions = sorted(list(set(runs)))
    sequential = True
    for i in range(1, len(accessions)):

        if int(accessions[i][3:]) - int(accessions[i - 1][3:]) != 1:
            sequential = False
            break
    if sequential:
        return 'https://www.ebi.ac.uk/ena/browser/view/%s-%s' % (accessions[0], accessions[-1])
    return 'https://www.ebi.ac.uk/ena/browser/view/%s' % ','.join(accessions)

    runs_int = sorted(
        list(set(list([int(r[3:]) for r in runs if r != '']))))
    urls = []

    l = [as_range(g) for _, g in groupby(runs_int, key=lambda n, c=count(): n - next(c))]
    url = 'https://www.ebi.ac.uk/ena/browser/view/%s' % ','.join(['ERR%s-ERR%s' % (i[0], i[1]) for i in l])
    return url


class MAGETABConverter:
    def __init__(self, accession, out_dir):
        self.accession = accession
        self.efo = EFOCollection()
        self.tags = {}
        self.dir_path = os.path.join(LOAD_DIR, self.accession.split('-')[1], self.accession)
        folder = accession[0:6]
        if "GEOD" in accession:
            folder = accession[0:8]
        # self.out_dir = os.path.join(BASE_DIRECTORY, folder, accession)
        self.out_dir = out_dir
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        idf_file = os.path.join(self.dir_path, '%s.idf.txt' % self.accession)
        idf_target = os.path.join(self.out_dir, '%s.idf.txt' % self.accession)
        if os.path.exists(idf_target):
            os.remove(idf_target)

        shutil.copy(idf_file, idf_target)

        self.idf = IDF(idf_path=os.path.join(self.out_dir, '%s.idf.txt' % self.accession),
                       combined=False,
                       skip_release_date=True,
                       accession=self.accession)
        # print(dir(self.idf))
        self.is_microarray = True
        if len(self.idf.comments) > 0:
            e_type = getattr(self.idf.comments[0], 'AEExperimentType'.lower(), '')
            if e_type:
                self.is_microarray = is_microarray(e_type)
                self.tags = self.efo.get_tags([e_type])

        self.sdrf = []
        # print(dir(self.idf))
        for sdrf in self.idf.sdrf_file:
            if sdrf == '' or '^' in sdrf:
                continue
            sdrf_file = os.path.join(self.dir_path, sdrf)
            sdrf_target = os.path.join(self.out_dir, sdrf)
            if os.path.exists(sdrf_target):
                os.remove(sdrf_target)
            shutil.copy(sdrf_file, sdrf_target)
            self.sdrf.append(SdrfCollection(os.path.join(self.out_dir, sdrf)))
        if not self.idf.public_release_date[0]:
            self.idf.public_release_date[0] = datetime.datetime.utcnow().date().isoformat()
        self.page_tab = PageTab(title=self.idf.investigation_title[0],
                                accession=self.accession,
                                release_date=self.idf.public_release_date[0],
                                out_dir=self.out_dir)
        self.technologies = []
        self.molecules = []
        self.types = []
        self.arrays = {}
        organisms = []
        if self.is_microarray:
            self.array_score()
        for row in self.sdrf[0].rows:
            for char in row.chars:
                o = char.get('Organism', {}).get('value', None)
                if o and o.strip():
                    organisms.append(o)
        self.nodes = OrderedDict()
        self.factors = []
        self.organisms = list(set(organisms))

        ## Starting Sections

        self.study_section()
        self.author()
        # self.tags_section()
        self.publication()
        self.samples_and_exp_factors()

        # self.protocols()
        # self.chars_and_factors()
        self.assays_and_data()
        # self.data_files()
        # self.magetab_files()
        # self.arrays_section()
        # self.study_links()
        self.score_section()

    def chars_and_factors(self, parent_id=None):
        if not parent_id:
            parent_id = 's-%s' % self.accession
        header = self.sdrf[0].header
        sdrf_content = self.sdrf[0].lines
        nodes = [(i, x.split('Name')[0].strip()) for i, x in enumerate(header) if x.endswith('Name')]

        self.factors = [n for n in header if 'factor' in n.lower()]
        self.get_char_txt(header, sdrf_content, self.factors,
                          ch_id="exp_factor-" + self.accession,
                          title="Experimental Factors",
                          parent=parent_id,
                          att_name="Factors", id_base='factors')

        for i in range(len(nodes)):
            node = nodes[i]
            next_index = len(header)
            if i + 1 < len(nodes):
                next_index = nodes[i + 1][0]
            name = node[1]
            n_index = node[0]
            _char = [n for n in header[n_index:next_index] if 'characteristics' in n.lower()]
            self.nodes[name] = {'index': n_index, 'next': next_index, 'chars': _char[:]}
            self.get_char_txt(header, sdrf_content, _char,
                              ch_id="%s_chars-%s" % (name.lower(), self.accession),
                              title="%s Characteristics" % name,
                              parent=parent_id, att_name="Characteristics",
                              id_base=name.lower())

    def array_score(self):
        arrays = retrieve_arrays_by_exp_acc(self.accession)
        for a in arrays:
            array = a.array

            if array is None or not array.startswith('A-'):
                continue
            bst_array = retrieve_submission_by_acc(array)
            if bst_array and len(bst_array) > 0:
                score = {'score': 0}

                sections = get_root_section_by_accession(array)
                for s in sections:
                    if s['name'] == 'MIAME Compliant ADF':
                        if s['value'] == 'Yes':
                            score = {'score': 1}
                        break



            else:
                adf_convert = ADFConverter(array, os.path.join(MAGIC_DIR, array))
                score = adf_convert.adf_dict['score']
                # url = "https://www.ebi.ac.uk/arrayexpress/files/%s/%s.adf.txt" % (array, array)
                # r = requests.get(url)
                # lines = r.content.decode('utf8').split('\n')
                # score = score_adf(lines)
            self.arrays[array] = score

    def tags_section(self):
        fields = []
        # try:
        #     r = requests.get(ATLAS_API).json()
        #     atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]
        #
        #     if self.accession in atlas_accs:
        #         fields.append(
        #             Field(name='Link',
        #                   value=self.accession,
        #                   extras=[["Type", "gxa"], ["Description", "Expression Atlas - " + self.accession]]
        #                   )
        #         )
        #     r = requests.get(ATLAS_SC_API).json()
        #     sc_atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]
        #     if self.accession in sc_atlas_accs:
        #         fields.append(
        #             Field(name='Link',
        #                   value=self.accession,
        #                   extras=[["Type", "gxa-sc"],
        #                           ["Description", "Single Cell Expression Atlas - " + self.accession]]
        #                   )
        #         )
        # except:
        #     pass
        experimental_designs = self.extract_multi_idf_fields(field_base='experimental_design',
                                                             field_name='Experimental Designs')
        experimental_factors = self.extract_multi_idf_fields(field_base='experimental_factor_name',
                                                             field_name='Experimental Factors',
                                                             term_name='experimental_factor_type')
        quality_controls = self.extract_multi_idf_fields(field_base='quality_control_type',
                                                         field_name='Quality Controls',
                                                         ref_name='quality_control_term_source_ref',
                                                         ref_id='quality_control_term_accession_number')
        replicates = self.extract_multi_idf_fields(field_base='replicate_type',
                                                   field_name='Replicates',
                                                   ref_name='replicate_term_source_ref',
                                                   ref_id='replicate_term_accession_number')
        normalizations = self.extract_multi_idf_fields(field_base='normalization_type',
                                                       field_name='Normalizations',
                                                       ref_name='normalization_term_source_ref',
                                                       ref_id='normalization_term_accession_number')

        tags_fields = OrderedDict([
            # ("Technology", list(set(self.technologies))),
            # ("Assay by Molecule", list(set(self.molecules))),
            # ("Organism", self.organisms),
            ("link", fields),
            ("Experimental Designs", experimental_designs),
            ("Experimental Factors", experimental_factors),
            ("Quality Controls", quality_controls),
            ("Replicates", replicates),
            ("Normalizations", normalizations)
        ])
        return tags_fields
        # self.page_tab.sections.append(Section(name="Experiment's Tags",
        #                                       section_id='%s-tags' % self.accession,
        #                                       parent_id='s-' + self.accession,
        #                                       section_fields=tags_fields))

    def extract_multi_idf_fields(self, field_base, field_name, ref_name=None, ref_id=None, term_name=None):
        designs = getattr(self.idf, field_base, [])
        if ref_name:
            designs_ref = getattr(self.idf, ref_name, [])
        else:
            designs_ref = getattr(self.idf, field_base + '_term_source_ref', [])
        if ref_id:
            designs_ids = getattr(self.idf, ref_id, [])
        else:
            designs_ids = getattr(self.idf, field_base + '_term_accession_number', [])

        term_names = []
        if term_name:
            term_names = getattr(self.idf, term_name, [])
        experimental_designs = []
        for i in range(len(designs)):
            design = designs[i]
            if not design:
                continue
            ref = None
            ref_id = None
            t_name = None
            try:
                ref_id = designs_ref[i]
            except:
                pass
            try:
                ref_id = designs_ids[i]
            except:
                pass
            try:
                t_name = term_names[i]
            except:
                pass

            if ref or ref_id or term_name:
                extras = []
                if ref:
                    extras.append(['[Ontology]', ref])
                if ref_id:
                    extras.append(['[TermId]', ref_id])

                if t_name:
                    extras.append(['[TermName]', t_name])

                experimental_designs.append(Field(name=field_name, value=design, extras=extras))
            else:
                experimental_designs.append(design)
        return experimental_designs

    def study_section(self):

        for k, v in self.tags.items():
            self.technologies += v['technology']
            self.molecules += v['molecule']
            self.types.append(
                Field(name='Study type',
                      value=k,
                      extras=[["[Ontology]", "EFO"], ["[TermId]", v['id']]]
                      )
            )
        section_fields = OrderedDict([
            ("Title", self.idf.investigation_title[0]),
            ("Types", self.types),
            ("Description", getattr(self.idf, 'experiment_description', '')),
            ("Organism", self.organisms),
            # ("Assay count", len(list(set([r.assay_name for r in self.sdrf[0].rows if r.assay_name is not None])))),
            # ("Sample count", len(list(set([r.source for r in self.sdrf[0].rows])))),
            # ("Experimental Factor count", len(getattr(self.idf, 'experimental_factor_name', []))),

        ])
        # tags = self.tags_section()
        # section_fields.update(tags)
        extra_tables = [self.protocols(parent_id='s-' + self.accession)] + self.study_links(
            parent_id='s-' + self.accession)

        self.page_tab.sections.append(
            Section(name='Study', section_id='s-' + self.accession, section_fields=section_fields,
                    extra_tables=extra_tables))

    def score_section(self):
        score = MIAMEscore.score(self.idf.lines, [s.lines for s in self.sdrf])
        fields = OrderedDict()
        section_name = ''
        if self.is_microarray:

            section_name = 'MIAME Score'
            array_score = '*'
            for s in self.arrays.values():
                if not s['score']:
                    array_score = '-'
                    break
            fields['Platforms'] = array_score
        else:
            section_name = 'MINSEQE Score'
            fields['Exp. Design'] = '*' if score['exp_design_score'] else '-'

        fields.update({"Protocols": '*' if score['protocol_score'] else '-',
                       "Variables": '*' if score['fv_score'] else '-',
                       "Processed": '*' if score['processed_score'] else '-',
                       "Raw": '*' if score['raw_score'] else '-'
                       })

        self.page_tab.sections.append(
            Section(name=section_name, section_id='score-' + self.accession, section_fields=fields))

    def publication(self):
        for pub in self.idf.publications:
            if getattr(pub, 'title', None) or getattr(pub, 'pubmed_id', None) or getattr(pub, 'doi', None):
                fields = OrderedDict([
                    ("Title", getattr(pub, 'title', '')),
                    ("Authors", getattr(pub, 'author_list', '')),
                    ("DOI", getattr(pub, 'doi', '')),
                    ("Status", getattr(pub, 'status', '')),
                    ("[Ontology]", getattr(pub, 'status_term_source_ref', '')),

                ])
                if getattr(pub, 'doi', None):
                    fields.update(
                        {"Link": [Field(name='Link', value=pub.doi, extras=[['Type', 'DOI']], separate=True)]})
                self.page_tab.sections.append(
                    Section(name='Publication', section_id=getattr(pub, 'pubmed_id', ''),
                            section_fields=fields))

    def author(self):
        organizations = []
        for i in range(len(self.idf.persons)):
            person = self.idf.persons[i]
            if not (hasattr(person, 'first_name') and hasattr(person, 'last_name')):
                continue
            if person.first_name is None or person.last_name is None:
                continue
            author_name = person.first_name + ' ' + person.last_name
            if not author_name.strip():
                continue

            section_fields = OrderedDict([
                ("Name", author_name),
                ("Email", getattr(person, 'email', '')),
                ("Phone", getattr(person, 'phone', '')),
                ("Fax", getattr(person, 'fax', '')),
                ("Role", getattr(person, 'roles', '')),
                ("[Ontology]", getattr(person, 'roles_term_source_ref', ''))
                # "<affiliation>", 'o' + str(i + 1)
            ])
            # print(section_fields)
            if hasattr(person, 'affiliation'):
                section_fields["<affiliation>"] = 'o' + str(i + 1)
                organizations.append(
                    Section(name="Organization", section_id='o' + str(i + 1), section_fields=OrderedDict([
                        ("Name", person.affiliation),
                        ("Address", getattr(person, 'address', ''))
                    ])))

            self.page_tab.sections.append(Section(name='Author', section_id='',
                                                  section_fields=section_fields))
        self.page_tab.sections += organizations

    def protocols(self, parent_id=None):
        if not parent_id:
            parent_id = 's-' + self.accession
        # p_id = 'protocols-' + self.accession
        table = [["Protocols[%s]" % parent_id, "Name", "Type", "[Ontology]", "[TermId]", "Description", "Parameters",
                  "Hardware", "Software", "Contact"]]
        for p in self.idf.protocols:
            if not (getattr(p, 'name', None)):
                continue
            row = [p.name, p.name, getattr(p, 'type', ''), getattr(p, 'term_source_ref', ''),
                   getattr(p, 'term_accession_number', ''), getattr(p, 'description', ''), getattr(p, 'parameters', ''),
                   getattr(p, 'hardware', ''), getattr(p, 'software', ''), getattr(p, 'contact', '')]
            table.append(['' if v is None else v for v in row])
        return table
        # self.page_tab.sections.append(
        #     Section(
        #         name='Experiment Protocols',
        #         section_id=p_id,
        #         parent_id=parent_id,
        #         table=table
        #     ))

    @staticmethod
    def convert_char_dict(s_char_dict, c_names, parent_id, att_name="Characteristics", id_base=''):
        if not c_names:
            return []
        if len(s_char_dict.keys()) == 0:
            return ""
        header = ["%s Table[%s]" % (att_name, parent_id)] + c_names + ["No. of Samples"]
        ret_txt = ''
        i = 0
        rows = []
        for k, v in s_char_dict.items():
            # print(v)

            ret_txt += "%s\t%s\t%s" % (id_base + '_' + str(i), '\t'.join(k), len(list(set(v["samples"]))))
            row = [id_base + '_' + str(i)] + list(k) + [str(len(list(set(v["samples"]))))]

            if 'runs' in v.keys():
                # ena_link = get_ena_link(v['runs'])
                row.append(get_ena_link(v['runs']))
                if '[url]' not in header:
                    header.append('[url]')
            rows.append(row)
            # print(ret_txt)
            # exit()
            i += 1
        # print(ret_txt)
        return [header] + rows

    def get_char_txt(self, header, sdrf_content, source_char, ch_id, parent='', title="", att_name="",
                     id_base=''):
        s_char_dict = OrderedDict()
        indices = []
        c_names = []
        units = []
        char_dict = OrderedDict()
        sources_pt_txt = ""
        section_fields = OrderedDict()
        vals = {}
        one2one = []
        singular = []
        s = [a for a in header if a.lower().startswith('sample') or a.lower().startswith('source')]
        idx = [header.index(a) for a in s]
        remove = []
        for c in source_char:
            samples = []
            index = header.index(c)
            for i in sdrf_content[1:]:
                line = i.split('\t')
                sample = [line[x] for x in idx]
                if sample in samples:
                    continue
                samples.append(sample)
                if len(line) <= index:
                    continue
                if c in vals.keys():
                    vals[c].append(line[index])
                else:
                    vals[c] = [line[index]]
            # vals[c] = [i.split('\t')[index] for i in sdrf_content[1:]]

            if c in vals and len(vals[c]) == len(list(set(vals[c]))):
                one2one.append([c.split('[')[1].split(']')[0].replace(')', '').replace('(', ''), vals[c]])
                remove.append(c)
            if c in vals and len(list(set(vals[c]))) == 1:
                singular.append([c.split('[')[1].split(']')[0].replace(')', '').replace('(', ''), vals[c]])
                remove.append(c)
        for item in remove:
            if item in source_char:
                source_char.remove(item)

        for c in source_char:
            c_index = header.index(c)
            c_name = c.split('[')[1].split(']')[0].replace(')', '').replace('(', '')

            indices.append(c_index)
            c_names.append(c_name)
            if c_index + 1 < len(header) and header[c_index + 1].lower().startswith('unit'):
                units.append(c_index + 1)
            else:
                units.append(None)

        for k in c_names:
            char_dict[k] = set([])
        if indices:

            # print(indices)
            for i in range(1, len(sdrf_content)):
                line = sdrf_content[i].split('\t')
                # print(len(line))
                if len(line) <= max(indices):
                    continue
                vals = [line[a].strip() for a in indices]
                for j in range(len(units)):
                    if units[j] is not None:
                        vals[j] += ' '
                        if len(line) > units[j]:
                            vals[j] += line[units[j]].strip()

                sample = '-'.join(line[a] for a in idx)
                run_index = None
                if not self.is_microarray and 'Comment [ENA_SAMPLE]' in header:
                    run_index = header.index('Comment [ENA_SAMPLE]')
                if not self.is_microarray and 'Comment[ENA_SAMPLE]' in header:
                    run_index = header.index('Comment[ENA_SAMPLE]')

                if tuple(vals) in s_char_dict.keys():
                    s_char_dict[tuple(vals)]["samples"].append(sample)
                    if not self.is_microarray and run_index:
                        s_char_dict[tuple(vals)]['runs'].append(line[run_index])

                else:
                    s_char_dict[tuple(vals)] = {"samples": [sample]}
                    if not self.is_microarray and run_index:
                        s_char_dict[tuple(vals)]['runs'] = [line[run_index]]

        for tup in s_char_dict.keys():
            for i in range(len(tup)):
                char_dict[c_names[i]].add(tup[i])

        for k, v in singular:
            # sources_pt_txt += "%s\t%s\n" % (k.capitalize(), v[0].strip())
            key = k.capitalize()
            if key in section_fields:
                section_fields[key].append(v[0].strip())
            else:
                section_fields[key] = [v[0].strip()]
        for k, v in char_dict.items():
            chars = [i.strip() for i in v if i.strip() != '']
            for i in range(min(15, len(chars))):
                key = k.capitalize()
                if key in section_fields:
                    section_fields[key].append(chars[i])
                else:
                    section_fields[key] = [chars[i]]
            if len(chars) > 15:
                key = k.capitalize()
                if key in section_fields:
                    section_fields[key].append('... %d other values' % (len(chars) - 15))
                else:
                    section_fields[key] = ['... %d other values' % (len(chars) - 15)]
                # sources_pt_txt += "%s\t%s\n" % (k.capitalize(), '... %d other values' % (len(chars) - 15))

        for k, v in one2one:
            chars = [i.strip() for i in v if i.strip() != '']
            for i in range(min(3, len(chars))):
                # sources_pt_txt += "%s\t%s\n" % (k.capitalize(), chars[i])
                key = k.capitalize()
                if key in section_fields:
                    section_fields[key].append(chars[i])
                else:
                    section_fields[key] = [chars[i]]
            if len(chars) > 3:
                # sources_pt_txt += "%s\t%s\n" % (k.capitalize(), '... %d other values' % (len(chars) - 3))
                key = k.capitalize()
                if key in section_fields:
                    section_fields[key].append('... %d other values' % (len(chars) - 3))
                else:
                    section_fields[key] = '... %d other values' % (len(chars) - 3)
        self.page_tab.sections.append(
            Section(name=title, section_id=ch_id, parent_id=parent, section_fields=section_fields,
                    table=self.convert_char_dict(s_char_dict, c_names, ch_id, att_name, id_base=id_base)))

    def _data_files(self, parent_id=None):
        if not parent_id:
            parent_id = 's-%s' % self.accession
        header = self.sdrf[0].header
        file_array = []  # list of File column indices
        for idx, headerStr in enumerate(header):
            if headerStr.endswith("Data File"):
                file_array.append(idx)

        for f in range(len(file_array)):
            f_index = file_array[f]
            t_header = ['Files']
            rows = []
            indexes = []
            for k, v in self.nodes.items():
                if v['chars']:
                    indexes += range(v['index'], v['next'])
                    # t_header += header[v['index']:v['next']]
                    # for l in self.sdrf[0].lines[1:]:
                    #     line = l.strip().split('\t')
                    #     rows.append([line[header.index('Array Data File')]]+line[v['index']:v['next']])
            for factor in self.factors:
                i = header.index(factor)
                indexes.append(i)
                # u = header[i+1].lower()
                if len(header) > i + 1 and 'unit' in header[i + 1].lower():
                    indexes.append(i + 1)
            t_header += [header[i] for i in indexes]
            for l in self.sdrf[0].lines[1:]:
                line = l.strip().split('\t')
                if len(line) > f_index and line[f_index].strip():
                    r = [line[f_index]]
                    for i in indexes:
                        if len(line) > i:
                            r.append(line[i])
                        else:
                            r.append('')
                    rows.append(r)
                    # rows.append([line[f_index]] + [line[i] for i in indexes])

            self.page_tab.sections.append(
                Section(name='Array Data File',
                        section_id='data-files-' + str(f),
                        parent_id=parent_id,
                        section_fields={},
                        table=[t_header] + rows))

    def raw_files_section(self, parent_id=None):
        if not parent_id:
            parent_id = 's-%s' % self.accession
        header = self.sdrf[0].header
        processed_files = []  # list of File column indices
        for idx, headerStr in enumerate(header):
            if headerStr == "Array Data File":
                processed_files.append(idx)

        files = OrderedDict()

        for l in self.sdrf[0].lines[1:]:
            for f_index in processed_files:
                line = l.strip().split('\t')
                if len(line) > f_index and line[f_index].strip():
                    f_name = line[f_index]
                    if f_name in files.keys():
                        files[f_name].append(line[0])
                    else:
                        files[f_name] = [line[0]]

        processed_files = []
        for f, s in files.items():
            processed_files.append([f, ', '.join(sorted(list(set(s)))), 'Raw Data'])

        processed_tbl = [['Files', 'Samples', 'Description']] + processed_files
        if processed_files:
            self.page_tab.sections.append(
                Section(name='Raw Data',
                        section_id='raw-data',
                        parent_id=parent_id,
                        section_fields={},
                        table=processed_tbl))

    def processed_files_section(self, parent_id=None):
        if not parent_id:
            parent_id = 's-%s' % self.accession
        header = self.sdrf[0].header
        processed_files = []  # list of File column indices
        for idx, headerStr in enumerate(header):
            if "Derived Array Data File" in headerStr:
                processed_files.append(idx)

        files = []

        for l in self.sdrf[0].lines[1:]:
            for f_index in processed_files:
                line = l.strip().split('\t')
                if len(line) > f_index and line[f_index].strip():
                    files.append(line[f_index])
        files = list(set(files))
        processed_files = []
        for f in files:
            processed_files.append([f, 'Processed Data'])

        processed_tbl = [['Files', 'Description']] + processed_files
        if processed_files:
            self.page_tab.sections.append(
                Section(name='Processed Data',
                        section_id='processed-data',
                        parent_id=parent_id,
                        section_fields={},
                        table=processed_tbl))

    def magetab_files(self, parent_id=None):
        if not parent_id:
            parent_id = 's-' + self.accession
        header = ['Files', 'Type', 'Format', 'Description']
        rows = [
            [self.idf.id_path.split('/')[-1], 'IDF File', 'tab-delimited text', 'Investigation Design Format (IDF)']]
        for s in self.sdrf:
            rows.append([s.file_path.split('/')[-1], 'SDRF File', 'tab-delimited text',
                         'Sample and Data Relationship Format (SDRF)'])
        self.page_tab.sections.append(Section(name='MAGE-TAB Files', section_id='mt-' + self.accession,
                                              parent_id=parent_id, table=[header] + rows))

    def arrays_section(self, parent_id=None):
        if not parent_id:
            parent_id = 's-' + self.accession
        if self.arrays:
            header = ['Links', 'Type']
            rows = []
            # a = ', '.join(list(self.arrays.keys())[:10])
            # if len(self.arrays) > 10:
            #     a += ' ... and %d Arrays more' % (len(self.arrays) - 10)

            for array in self.arrays:
                rows.append([array, 'Array Design'])
            self.page_tab.sections.append(Section(name='Array Designs', section_id='ad-' + self.accession,
                                                  parent_id=parent_id,
                                                  table=[header] + rows))

    def study_links(self, parent_id=''):
        data_links = [i for i in getattr(self.idf, 'comment_secondaryaccession', []) if i != '']
        related_studies = []
        studies_accs = []
        tables = []
        for acc in data_links[:]:
            if acc.startswith('E-GEOD-'):
                related_studies.append([acc, 'Biostudies'])
                studies_accs.append(acc)
                data_links.remove(acc)
            elif acc.startswith('E-'):
                related_studies.append([acc, 'Biostudies'])
                data_links.remove(acc)
                studies_accs.append(acc)

        header = ['Links', 'Type']

        if related_studies:
            # self.page_tab.sections.append(
            #     Section(name='Related Studies', section_id='related-studies-' + self.accession,
            #             parent_id='s-' + self.accession,
            #             section_fields={'Related Study': ', '.join(studies_accs)},
            #             table=[header] + related_studies))
            tables.append([header] + related_studies)
        rows = []
        for i in range(len(data_links)):
            acc = data_links[i]
            if acc.startswith('ERP') or acc.startswith('SRP'):
                rows.append([acc, 'ENA'])
            elif acc.startswith('GSE'):
                rows.append([acc, 'GEO'])
        try:
            r = requests.get(ATLAS_API).json()
            atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]

            if self.accession in atlas_accs:
                rows.append([self.accession, 'gxa', ])

            r = requests.get(ATLAS_SC_API).json()
            sc_atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]
            if self.accession in sc_atlas_accs:
                rows.append([self.accession, 'gxa-sc', ])
        except:
            pass
        # sequence_links = [i for i in getattr(self.idf, 'comment_sequencedatauri', []) if i != '']
        # for seq in sequence_links:
        #     rows.append([seq.split('/')[-1], 'ENA', 'ENA Runs'])
        if rows:
            tables.append([header] + rows)
        # self.page_tab.sections.append(Section(name='Study Links', section_id='slinks-' + self.accession,
        #                                       parent_id='s-' + self.accession,
        #                                       table=[header] + rows))
        if tables:
            return tables
        return []

    def samples_and_exp_factors(self):
        section_fields = OrderedDict([
            ("Sample count", len(list(set([r.source for r in self.sdrf[0].rows])))),
            # ("Experimental Factor count", len(getattr(self.idf, 'experimental_factor_name', []))),

        ])
        tags = self.tags_section()
        section_fields.update(tags)
        sec_id = 's-samples-factors-' + self.accession
        # extra_tables = [self.protocols(parent_id=sec_id),]
        self.page_tab.sections.append(
            Section(name='Samples and Experimental Factors',
                    parent_id='s-' + self.accession,
                    section_id=sec_id,
                    section_fields=section_fields, ))

        self.chars_and_factors(parent_id=sec_id)

    def assays_and_data(self):
        section_fields = OrderedDict([
            ("Assay count", len(list(set([r.assay_name for r in self.sdrf[0].rows if r.assay_name is not None])))),
            ("Technology", list(set(self.technologies))),
            ("Assay by Molecule", list(set(self.molecules))),
        ])
        sec_id = 's-assays-data-' + self.accession
        self.page_tab.sections.append(
            Section(name='Assays and Data',
                    parent_id='s-' + self.accession,
                    section_id=sec_id,
                    section_fields=section_fields))
        self.raw_files_section(parent_id=sec_id)
        self.processed_files_section(parent_id=sec_id)
        self.magetab_files(parent_id=sec_id)
        self.arrays_section(parent_id=sec_id)


if __name__ == '__main__':
    pass
    # adf_convert = ADFConverter('A-AFFY-43', '/tmp/A-AFFY-43')
    # converter = MAGETABConverter('E-MTAB-5555', '/tmp/E-MTAB-5555')
    # converter = MAGETABConverter('E-MTAB-7922', '/tmp/E-MTAB-7922')
    converter = MAGETABConverter('E-MTAB-6302', '/tmp/E-MTAB-6302')
    file_lists = converter.page_tab.export()
    print(file_lists)
    # f = open(os.path.join(converter.out_dir, 'E-MTAB-5782.pagetab.tsv'), 'w')
    # txt = ''
    # for section in converter.page_tab.sections:
    #     txt += str(section)
    # converter.page_tab.export()
    # f.write(txt)
    # f.close()
    # print(converter.is_microarray)
    # print(converter.idf.sdrf_file)
