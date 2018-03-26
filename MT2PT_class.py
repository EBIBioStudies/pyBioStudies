from __future__ import print_function

import argparse
import collections
import filecmp
import shutil
import zipfile
from stat import *

from common import execute_command, print_one, get_one
from dal.ae2.ae2_transaction import retrieve_arrays_by_exp_acc, retrieve_public_experiments
from settings import *


class MAGETABConverter:
    def __init__(self, accession, dir_path, file_names, force=False):
        self.accession = accession
        self.dir_path = dir_path
        self.file_names = file_names
        self.force = force
        self.folder = accession[0:6]
        if "GEOD" in accession:
            self.folder = accession[0:8]  # output folder - for GEO we make smaller folders
        self.out_dir = os.path.join(BASE_DIRECTORY, self.folder, accession)
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self.page_tab_filename = os.path.join(self.out_dir, accession + '.pagetab.tsv')
        # print(self.page_tab_filename)
        # self.out_dir = None
        self.export()
        for file_name in self.file_names:
            # print("Working on:" + file_name)
            if file_name.endswith("idf.txt"):
                if oct(os.stat(self.dir_path)[ST_MODE])[-1:] == '0':  # translating only public AE experiments
                    continue
                self.idf_file_path = os.path.join(self.out_dir, file_name)

                self.idf_dict = self.parse_idf(self.idf_file_path)

        if "SDRF File" in self.idf_dict.keys():
            sdrf_num = len(self.idf_dict["SDRF File"])

            for i in range(0, sdrf_num):
                # if i > 0: print(file_name)
                sdrf_file_path = os.path.join(self.out_dir, self.idf_dict["SDRF File"][i])
                with open(sdrf_file_path, "r") as sdrf_file:
                    sdrf_content = sdrf_file.readlines()

                header = sdrf_content[0].replace('"', '').rstrip('\n').split('\t')

                self.organisms = self.get_organism(sdrf_content, header)

    @staticmethod
    def convert_char_dict(s_char_dict, c_names, parent_id, att_name="Characteristics", id_base=''):
        if len(s_char_dict.keys()) == 0:
            return ""
        ret_txt = "%s[%s]\t%s\tNo. of Samples\n" % (parent_id.split('-')[0], parent_id, '\t'.join(c_names))
        i = 0
        for k, v in s_char_dict.items():
            ret_txt += "%s\t%s\t%s\n" % (id_base + '_' + str(i), '\t'.join(k), len(list(set(v["samples"]))))
            i += 1
        # print(ret_txt)
        return ret_txt

    def manage_chars(self, sdrf_content, parent_id, accession):
        ret_text = ""
        header = sdrf_content[0].split('\t')
        # print(header)
        nodes = [(i, x.split('Name')[0].strip()) for i, x in enumerate(header) if x.endswith('Name')]

        for i in range(len(nodes)):
            node = nodes[i]
            next_index = len(header)
            if i + 1 < len(nodes):
                next_index = nodes[i + 1][0]
            name = node[1]
            n_index = node[0]
            _char = [n for n in header[n_index:next_index] if 'characteristics' in n.lower()]
            sources_pt_txt = self.get_char_txt(header, sdrf_content, _char,
                                               ch_id="%s_chars-%s" % (name.lower(), accession),
                                               title="%s Characteristics\t%s_chars-%s\t%s" % (
                                                   name, name.lower(), accession, parent_id),
                                               id_base=name.lower())
            if sources_pt_txt:
                ret_text += "\n" + sources_pt_txt

        return ret_text

    def get_char_txt(self, header, sdrf_content, source_char, ch_id, title="", att_name="Characteristics", id_base=''):
        s_char_dict = collections.OrderedDict()
        indices = []
        c_names = []
        units = []
        char_dict = collections.OrderedDict()
        sources_pt_txt = ""
        # singular_char = []
        # print(char_dict.items())
        # for k, v in char_dict.items():
        #     chars = [i.strip() for i in v if i.strip() != '']
        #     # print(chars)
        #     sources_pt_txt += "%s\t%s\n" % (k, ', '.join(chars))
        #     if len(v) == 1:
        #         singular_char.append(k)
        vals = {}
        one2one = []
        singular = []
        char_index = [header.index(c) for c in source_char]
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
                if c in vals.keys():
                    vals[c].append(line[index])
                else:
                    vals[c] = [line[index]]
            # vals[c] = [i.split('\t')[index] for i in sdrf_content[1:]]

            if len(vals[c]) == len(list(set(vals[c]))):
                one2one.append([c.split('[')[1].split(']')[0], vals[c]])
                remove.append(c)
            if len(list(set(vals[c]))) == 1:
                singular.append([c.split('[')[1].split(']')[0], vals[c]])
                remove.append(c)
        for item in remove:
            if item in source_char:
                source_char.remove(item)

        for c in source_char:
            c_index = header.index(c)
            c_name = c.split('[')[1].split(']')[0]

            indices.append(c_index)
            c_names.append(c_name)
            if c_index + 1 < len(header) and header[c_index + 1].lower().startswith('unit'):
                units.append(c_index + 1)
            else:
                units.append(None)

        if not indices:
            return ""
        for k in c_names:
            char_dict[k] = set([])

        for i in range(1, len(sdrf_content)):
            line = sdrf_content[i].split('\t')
            vals = [line[a].strip() for a in indices]
            for j in range(len(units)):
                if units[j] is not None:
                    vals[j] += ' ' + line[units[j]].strip()

            sample = '-'.join(line[a] for a in idx)

            if tuple(vals) in s_char_dict.keys():
                s_char_dict[tuple(vals)]["samples"].append(sample)

            else:
                s_char_dict[tuple(vals)] = {"samples": [sample]}
        for tup in s_char_dict.keys():
            for i in range(len(tup)):
                char_dict[c_names[i]].add(tup[i])

        for k, v in singular:
            sources_pt_txt += "%s\t%s\n" % (k.capitalize(), v[0].strip())
        for k, v in char_dict.items():
            chars = [i.strip() for i in v if i.strip() != '']
            for char in chars:
                sources_pt_txt += "%s\t%s\n" % (k.capitalize(), char)

        for k, v in one2one:
            chars = [i.strip() for i in v if i.strip() != '']
            for i in range(min(3, len(chars))):
                sources_pt_txt += "%s\t%s\n" % (k.capitalize(), chars[i])
            if len(chars) > 3:
                sources_pt_txt += "%s\t%s\n" % (k.capitalize(), '... %d other values' % (len(chars) - 3))

        sources_pt_txt += '\n'
        sources_pt_txt += self.convert_char_dict(s_char_dict, c_names, ch_id, att_name, id_base=id_base)
        # sources_pt_txt = ""
        if sources_pt_txt and title:
            sources_pt_txt = title + '\n' + sources_pt_txt
        return sources_pt_txt

    def convert_experiment_to_page_tab(self):
        files_to_copy = []
        copied_files = []
        for file_name in self.file_names:
            # print("Working on:" + file_name)
            if file_name.endswith('.zip'):
                # print('ZIP FILE: ' + file_name)
                files_to_copy.append(os.path.join(self.dir_path, file_name))
            elif file_name.endswith("idf.txt"):
                # if oct(os.stat(self.dir_path)[ST_MODE])[-1:] == '0':  # translating only public AE experiments
                #     continue
                # idf_file_path = os.path.join(self.dir_path, file_name)
                # files_to_copy.append(idf_file_path)
                # self.idf_dict = self.parse_idf(idf_file_path)

                # accession = file_name[:file_name.find('.')]

                # if "Comment[ArrayExpressAccession]" in self.idf_dict.keys():
                #     # the case when filename does not correspond to the accession
                #
                #     accession = self.idf_dict["Comment[ArrayExpressAccession]"][0]

                # self.out_dir = BASE_DIRECTORY  # RootPath is not working for submission. Will change back when it is fixed in BS

                if not os.path.exists(os.path.dirname(self.page_tab_filename)):
                    os.makedirs(os.path.dirname(self.page_tab_filename))
                pt_file = open(self.page_tab_filename, 'w')

                print("", file=pt_file)

                print("Submission\t" + self.accession + "\tPublic",
                      file=pt_file)  # start writing PageTab - only for public experiments for now!!
                print_one("Title", "Investigation Title", self.idf_dict, 0, pt_file)
                print_one("ReleaseDate", "Public Release Date", self.idf_dict, 0, pt_file)
                print("RootPath\t" + "%s/%s" % (self.folder, self.accession), file=pt_file)
                print("AttachTo\tArrayExpress", file=pt_file)

                print("", file=pt_file)
                print("Study\ts-" + self.accession, file=pt_file)
                print_one("Title", "Investigation Title", self.idf_dict, 0, pt_file)
                print_one("Description", "Experiment Description", self.idf_dict, 0, pt_file)
                print_one("Study type", "Comment [AEExperimentType]", self.idf_dict, 0, pt_file)
                if self.organisms:
                    if len(self.organisms) > 1:
                        print("Organisms\t%s-org\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for o in self.organisms:
                            print("Organisms\t%s" % o, file=pt_file)
                    else:
                        print("Organism\t%s" % self.organisms[0], file=pt_file)

                # print("RootPath\t" + '/E-MTAB/E-MTAB-13/', file=f)

                # print("", file=pt_file)
                if "Experimental Design" in self.idf_dict.keys():
                    ed_num = len(self.idf_dict["Experimental Design"])
                    if ed_num:
                        # print("Experimental Designs\texp-des-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for i in range(0, ed_num):
                            print("Experimental Designs\t%s" % (self.idf_dict["Experimental Design"][i]),
                                  file=pt_file)
                            print_one("[Ontology]", "Experimental Design Term Source REF", self.idf_dict, i, pt_file)
                            print_one("[TermId]", "Experimental Design Term Accession Number", self.idf_dict, i,
                                      pt_file)
                        # print("", file=pt_file)

                if "Experimental Factor Name" in self.idf_dict.keys():
                    ef_num = len(self.idf_dict["Experimental Factor Name"])
                    if ef_num:
                        # print("Experimental Factors\texp-fac-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for i in range(0, ef_num):
                            print(
                                "Experimental Factors\t%s" % (self.idf_dict["Experimental Factor Name"][i]),
                                file=pt_file)
                            print_one("[Ontology]", "Experimental Factor Term Source REF", self.idf_dict, i, pt_file)
                            print_one("[TermId]", "Experimental Factor Term Accession Number", self.idf_dict, i,
                                      pt_file)
                            print_one("[TermName]", "Experimental Factor Type", self.idf_dict, i, pt_file)
                        # print("", file=pt_file)
                if "Quality Control Type" in self.idf_dict.keys():
                    qc_num = len(self.idf_dict["Quality Control Type"])
                    if qc_num:
                        # print("Quality Control\texp-qual-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for i in range(0, qc_num):
                            print("Quality Controls\t" + self.idf_dict["Quality Control Type"][i], file=pt_file)
                            print_one("[Ontology]", "Quality Control Term Source REF", self.idf_dict, i, pt_file)
                            print_one("[TermId]", "Quality Control Term Accession Number", self.idf_dict, i, pt_file)
                        # print("", file=pt_file)

                if "Replicate Type" in self.idf_dict.keys():
                    rep_num = len(self.idf_dict["Replicate Type"])
                    if rep_num:
                        # print("Replicate\texp-rep-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for i in range(0, rep_num):
                            print("Replicates\t%s" % (self.idf_dict["Replicate Type"][i]), file=pt_file)
                            print_one("[Ontology]", "Replicate Term Source REF", self.idf_dict, i, pt_file)
                            print_one("[TermId]", "Replicate Term Accession Number", self.idf_dict, i, pt_file)
                        # print("", file=pt_file)

                if "Normalizations" in self.idf_dict.keys():
                    nor_num = len(self.idf_dict["Normalization Type"])
                    if nor_num:
                        # print("Normalizations\texp-nrom-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                        for i in range(0, nor_num):
                            print("Normalization Type\t%s" % (self.idf_dict["Normalization Type"][i]),
                                  file=pt_file)
                            print_one("[Ontology]", "Normalization Term Source REF", self.idf_dict, i, pt_file)
                            print_one("[TermId]", "Normalization Term Accession Number", self.idf_dict, i, pt_file)
                        # print("", file=pt_file)

                pub_num1 = 0
                pub_num2 = 0
                pub_num3 = 0  # multiple ways in MAGE-TAB to describe publications
                if "PubMed ID" in self.idf_dict.keys():
                    pub_num1 = len(self.idf_dict["PubMed ID"])
                if "Publication DOI" in self.idf_dict.keys():
                    pub_num2 = len(self.idf_dict["Publication DOI"])
                if "Publication Title" in self.idf_dict.keys():
                    pub_num3 = len(self.idf_dict["Publication Title"])
                for i in range(0, max(pub_num1, pub_num2, pub_num3)):
                    print("", file=pt_file)
                    if "PubMed ID" in self.idf_dict.keys() and i < len(self.idf_dict["PubMed ID"]):
                        print("Publication\t" + self.idf_dict["PubMed ID"][i],
                              file=pt_file)  # in PageTab identifier of the Publication section is the PubMed id, if present
                    else:
                        print("Publication", file=pt_file)
                    print_one("Title", "Publication Title", self.idf_dict, i, pt_file)
                    print_one("Authors", "Publication Author List", self.idf_dict, i, pt_file)
                    print_one("DOI", "Publication DOI", self.idf_dict, i, pt_file)
                    print_one("Status", "Publication Status", self.idf_dict, i, pt_file)

                affiliation_list = []
                address_list = []
                if "Person Last Name" in self.idf_dict.keys():
                    person_num = len(
                        self.idf_dict["Person Last Name"])  # we assume that all contacts will have at least Last Name
                    for i in range(0, person_num):
                        print("", file=pt_file)
                        print("Author", file=pt_file)
                        name = self.idf_dict["Person Last Name"][i]
                        if "Person Mid Initials" in self.idf_dict.keys() and i < len(
                                self.idf_dict["Person Mid Initials"]):
                            name = self.idf_dict["Person Mid Initials"][i] + " " + name
                        if "Person First Name" in self.idf_dict.keys() and i < len(self.idf_dict["Person First Name"]):
                            name = self.idf_dict["Person First Name"][i] + " " + name
                        print("Name\t" + name, file=pt_file)
                        print_one("Email", "Person Email", self.idf_dict, i, pt_file)
                        print_one("Role", "Person Role", self.idf_dict, i, pt_file)
                        print_one("Phone", "Person Phone", self.idf_dict, i, pt_file)
                        print_one("Fax", "Person Fax", self.idf_dict, i, pt_file)
                        affiliation = ""
                        if "Person Affiliation" in self.idf_dict.keys() and i < len(
                                self.idf_dict["Person Affiliation"]):
                            affiliation = self.idf_dict["Person Affiliation"][i]
                        address = ""
                        if "Person Address" in self.idf_dict.keys() and i < len(self.idf_dict["Person Address"]):
                            address = self.idf_dict["Person Address"][i]
                        index = -1
                        if affiliation in affiliation_list:  # we have already seen this affiliation
                            index = affiliation_list.index(affiliation)
                            if address_list[index] != address:
                                # turns out the address is different -
                                # let's treat this as a different organization after all
                                index = -1
                        if index != -1:
                            print("<affiliation>\to" + str(index + 1), file=pt_file)
                        else:
                            affiliation_list.append(affiliation)
                            address_list.append(address)
                            print("<affiliation>\to" + str(len(affiliation_list)), file=pt_file)

                # print out Organizations - a separate PageTab section for each
                org_num = len(affiliation_list)
                for i in range(0, org_num):
                    print("", file=pt_file)
                    print("Organization\to" + str(i + 1), file=pt_file)
                    if not affiliation_list[i]:
                        print("Name\t" + address_list[i], file=pt_file)
                    else:
                        print("Name\t" + affiliation_list[i], file=pt_file)
                        print("Address\t" + address_list[i], file=pt_file)

                # end of IDF transformation	(except Protocols - will come after Files extracted from SDRF(s) )
                if "Protocol Name" in self.idf_dict.keys():
                    protocol_num = len(self.idf_dict["Protocol Name"])
                    protocols_id = 'protocols-' + self.accession
                    if protocol_num > 0:
                        print("", file=pt_file)

                        print("Experiment Protocols\t%s\ts-%s" % (protocols_id, self.accession), file=pt_file)
                        print("", file=pt_file)
                        print("Protocols[%s]\tName\tType\t[Ontology]\t[TermId]\t"
                              "Description\tParameters\tHardware\tSoftware\tContact" % protocols_id, file=pt_file)
                        for i in range(0, protocol_num):
                            p_row = [self.idf_dict["Protocol Name"][i] + "\t" + self.idf_dict["Protocol Name"][i]]
                            p_type = get_one("Protocol Type", self.idf_dict, i)
                            p_row.append(p_type)
                            p_ontology = get_one("Protocol Term Source REF", self.idf_dict, i)
                            p_row.append(p_ontology)
                            p_id = get_one("Protocol Term Accession Number", self.idf_dict, i)
                            p_row.append(p_id)
                            p_description = get_one("Protocol Description", self.idf_dict, i)
                            p_row.append(p_description)
                            p_params = get_one("Protocol Parameters", self.idf_dict, i)
                            p_row.append(p_params)
                            p_Hardware = get_one("Protocol Hardware", self.idf_dict, i)
                            p_row.append(p_Hardware)
                            p_software = get_one("Protocol Software", self.idf_dict, i)
                            p_row.append(p_software)
                            p_contact = get_one("Protocol Contact", self.idf_dict, i)
                            p_row.append(p_contact)
                            print('\t'.join(p_row), file=pt_file)
                        print("", file=pt_file)
                        # print_one("Type", "Protocol Type", self.idf_dict, i, pt_file)
                        # print_one("[Ontology]", "Protocol Term Source REF", self.idf_dict, i, pt_file)
                        # print_one("[TermId]", "Protocol Term Accession Number", self.idf_dict, i, pt_file)
                        # print_one("Description", "Protocol Description", self.idf_dict, i, pt_file)
                        # print_one("Protocol Parameters", "Protocol Parameters", self.idf_dict, i, pt_file)
                        # print_one("Protocol Hardware", "Protocol Hardware", self.idf_dict, i, pt_file)
                        # print_one("Protocol Software", "Protocol Software", self.idf_dict, i, pt_file)
                        # print_one("Protocol Contact", "Protocol Contact", self.idf_dict, i, pt_file)

                if "SDRF File" in self.idf_dict.keys():
                    sdrf_num = len(self.idf_dict["SDRF File"])

                    for i in range(0, sdrf_num):
                        # if i > 0: print(file_name)
                        sdrf_file_path = os.path.join(self.out_dir, self.idf_dict["SDRF File"][i])
                        # files_to_copy.append(sdrf_file_path)
                        with open(sdrf_file_path, "r") as sdrf_file:
                            sdrf_content = sdrf_file.readlines()
                        for k in range(len(sdrf_content)):
                            sdrf_content[k] = sdrf_content[k].replace('"', '')

                        header = sdrf_content[0].rstrip('\n').split('\t')
                        mask = self.sdrf_mask(header, sdrf_content)

                        # organisms = self.get_organism(sdrf_content, header)

                        print("", file=pt_file)
                        char_text = self.manage_chars(sdrf_content, parent_id='s-' + self.accession,
                                                      accession=self.accession)
                        if char_text:
                            # print("Characteristics\tchars-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                            print(char_text, file=pt_file)

                        _factors = [n for n in header if 'factor' in n.lower()]
                        factors_pt_txt = self.get_char_txt(header, sdrf_content, _factors,
                                                           ch_id="exp_factor-" + self.accession,
                                                           title="Experimental Factors\texp_factor-%s\ts-%s" % (
                                                               self.accession, self.accession),
                                                           att_name="Factors", id_base='factors')
                        if factors_pt_txt:
                            print("\n" + factors_pt_txt, file=pt_file)

                        new_header = [t for t, flag in zip(header, mask) if flag]  # apply mask to the header
                        new_contents = []
                        for j in range(1, len(sdrf_content)):
                            current_line = sdrf_content[j].rstrip('\n').split('\t')
                            new_contents.append([t for t, flag in zip(current_line, mask) if flag])
                            # as well as to the contents - now we have a SDRF (in newContents) with less columns

                        file_array = []  # list of File column indices
                        for idx, headerStr in enumerate(new_header):
                            if headerStr.endswith(" File"):
                                file_array.append(idx)
                        for j in range(0, len(file_array)):  # we treat each of the File columns separately
                            file_name_prop_vector = collections.OrderedDict()
                            # map from file name to property vector, i.e., a set of all the different props

                            for k in range(0, len(new_contents)):
                                prop_array = new_contents[k]
                                file_name = prop_array[file_array[j]]
                                if len(prop_array[0:file_array[j]]) > 0:
                                    # props will be, for a specific File, all other values in SDRF
                                    props = '\t'.join(prop_array[0:file_array[j]]) + '\t' + '\t'.join(
                                        prop_array[file_array[len(file_array) - 1] + 1:len(new_header)])
                                else:
                                    props = '\t'.join(prop_array[file_array[len(file_array) - 1] + 1:len(new_header)])
                                prop_vector = collections.OrderedDict()
                                if file_name in file_name_prop_vector.keys():
                                    prop_vector = file_name_prop_vector[file_name]
                                prop_vector[props] = 1
                                file_name_prop_vector[file_name] = prop_vector
                            if len(file_name_prop_vector) == 0:
                                continue

                            minimum = min([len(v) for v in file_name_prop_vector.values()])
                            # for files in this column, what is the minimum number of different props that they have?
                            maximum = max([len(v) for v in file_name_prop_vector.values()])

                            if minimum < 3:
                                # we think this column contains e.g. .cel files (where each file has a single props vector),
                                # or perhaps, for 2-colour experiments, two different lines per file

                                print("", file=pt_file)
                                print(new_header[file_array[j]], "Section\ts" + str(i) + "-" + str(j), file=pt_file)
                                print("", file=pt_file)
                                if len(prop_array[0:file_array[j]]) > 0:  # Files table header
                                    print("Files\t" + '\t'.join(new_header[0:file_array[j]]) + '\t' + '\t'.join(
                                        new_header[file_array[len(file_array) - 1] + 1:len(new_header)]), file=pt_file)
                                else:
                                    print(
                                        "Files\t" + '\t'.join(
                                            new_header[file_array[len(file_array) - 1] + 1:len(new_header)]),
                                        file=pt_file)
                                for f_name in file_name_prop_vector.keys():  # the actual Files table
                                    f_path = os.path.join(self.dir_path, f_name)
                                    if os.path.exists(f_path):
                                        files_to_copy.append(f_path)
                                    else:
                                        f_path = os.path.join(self.dir_path, 'unpacked', f_name)
                                        if os.path.exists(f_path):
                                            files_to_copy.append(f_path)
                                        # else:
                                        #     raise Exception("%s NOT FOUND.")

                                    for props in file_name_prop_vector[f_name].keys():
                                        if f_name.strip():
                                            print(f_name, file=pt_file, end="")
                                            print("\t", file=pt_file, end="")
                                            print(props, file=pt_file)
                            else:
                                print("", file=pt_file)
                                print(new_header[file_array[j]], "Section\ts" + str(i) + "-" + str(j), file=pt_file)
                                for idx, f_name in enumerate(file_name_prop_vector.keys()):
                                    # for this case we will create a separate section for each File

                                    if f_name.strip() == '':
                                        continue
                                    section_id = "s" + str(i) + "-" + str(j) + "-" + str(idx)
                                    print("", file=pt_file)
                                    print("Data File: " + f_name + "\t" + section_id + "\ts" + str(i) + "-" + str(j),
                                          file=pt_file)  # section header
                                    print("", file=pt_file)
                                    print("File\t" + f_name, file=pt_file)  # single file
                                    print("", file=pt_file)

                                    if len(prop_array[0:file_array[j]]) > 0:
                                        print("List of associated samples [" + section_id + "]\t" + '\t'.join(
                                            new_header[0:file_array[j]]) + '\t' + '\t'.join(
                                            new_header[file_array[len(file_array) - 1] + 1:len(new_header)]),
                                              file=pt_file)  # "sample table" header
                                    else:
                                        print("List of associated samples [" + section_id + "]\t" + '\t'.join(
                                            new_header[file_array[len(file_array) - 1] + 1:len(new_header)]),
                                              file=pt_file)
                                    for props in file_name_prop_vector[f_name].keys():  # the actual "sample table"
                                        print("\t", file=pt_file, end="")
                                        print(props, file=pt_file)

                print("", file=pt_file)
                print("MAGE-TAB Files\tmt-" + self.accession, file=pt_file)
                print("", file=pt_file)
                print("File\t" + self.idf_file_path.split('/')[-1] + '\tmt-' + self.accession, file=pt_file)
                print("Type\tIDF File", file=pt_file)
                print("Format\ttab-delimited text", file=pt_file)
                print("Description\tInvestigation Design Format (IDF)", file=pt_file)
                print("", file=pt_file)
                for i in range(0, sdrf_num):
                    print("File\t" + self.idf_dict["SDRF File"][i] + '\tmt-' + self.accession, file=pt_file)
                    print("Type\tSDRF File", file=pt_file)
                    print("Format\ttab-delimited text", file=pt_file)
                    print("Description\tSample and Data Relationship Format (SDRF)", file=pt_file)
                    print("", file=pt_file)
                data_links = []
                if 'Comment [SecondaryAccession]' in self.idf_dict.keys():
                    data_links += self.idf_dict['Comment [SecondaryAccession]']
                elif 'Comment[SecondaryAccession]' in self.idf_dict.keys():
                    data_links += self.idf_dict['Comment[SecondaryAccession]']
                arrays = []
                arrays_id = []
                res = retrieve_arrays_by_exp_acc(self.accession)
                if res:
                    for a in res:
                        array = a.array
                        if array:
                            arrays.append("%s\tBiostudies\tArray" % array)
                            arrays_id.append(array)
                if arrays_id:
                    for i in arrays_id:
                        # print("Array Design\t%s\tarrays-%s" % (i, self.accession), file=pt_file)
                        print ("", file=pt_file)
                        print("Link\t%s\ts-%s"%(i, self.accession), file=pt_file)
                        print("Type\tBiostudies", file=pt_file)
                        print("Description\tArray Design", file=pt_file)
                related_studies = []

                for acc in data_links:
                    if acc.startswith('E-GEOD-'):
                        related_studies.append(
                            "r-%s\t%s\tGEO (ArrayExpress)\tRelated GEO Study" % (acc, acc))
                    elif acc.startswith('E-MTAB-'):
                        related_studies.append(
                            "r-%s\t%s\tBiostudies (ArrayExpress)\tRelated ArrayExpress Study" % (acc, acc))

                if related_studies:
                    print("", file=pt_file)
                    print("Related Studies\trelated-studies-%s\ts-%s" % (self.accession, self.accession), file=pt_file)
                    print("Related Studies[related-studies-%s]\tAccession\tType\tDescription", file=pt_file)

                    for r in related_studies:
                        print(r, file=pt_file)

                if data_links:
                    print("", file=pt_file)
                    print("Study Links\tslinks-" + self.accession, file=pt_file)
                    print("", file=pt_file)

                    print("Links[slinks-%s]\tType\tDescription" % self.accession, file=pt_file)

                for i in range(len(data_links)):
                    acc = data_links[i]
                    if acc.startswith('ERP'):
                        print("%s\tENA\tENA Project" % acc, file=pt_file)

                    elif acc.startswith('E-GEOD-'):
                        print(
                            "%s\tGEO\tRelated ArrayExpress Study" % acc,
                            file=pt_file)
                    elif acc.startswith('E-MTAB-'):
                        print(
                            "%s\tBiostudies\tRelated ArrayExpress Study" % acc,
                            file=pt_file)
                seq_links = []
                if 'Comment [SequenceDataURI]' in self.idf_dict.keys():
                    seq_links += self.idf_dict['Comment [SequenceDataURI]']
                elif 'Comment[SequenceDataURI]' in self.idf_dict.keys():
                    seq_links += self.idf_dict['Comment[SequenceDataURI]']

                if seq_links:
                    for i in range(len(seq_links)):
                        print("%s\tENA\tENA Runs" % seq_links[i].split('/')[-1], file=pt_file)

                # print("E-MTAB-1\tBiostudy\tRelated Experiment", file=pt_file)
                # for a in arrays:
                #     print(a, file=pt_file)
                pt_file.close()
        # print(files_to_copy)
        for f in files_to_copy:
            if os.path.isdir(f):
                continue
            f_name = os.path.split(f)[-1]
            des_file = os.path.join(self.out_dir, f_name)
            # print(des_file)
            if os.path.exists(des_file):
                if not filecmp.cmp(f, des_file):
                    # print("overwriting ", f_name)
                    # print(f)
                    shutil.copy2(f, self.out_dir)
            else:
                # print("copying ", f_name)
                shutil.copy2(f, self.out_dir)

            copied_files.append(des_file)

            if f_name.endswith('.zip'):
                # print("extracting:: " + f_name)
                zip_file = zipfile.ZipFile(f, 'r')
                zip_file = zipfile.ZipFile(f, 'r')
                files = zip_file.namelist()
                copied_files += [os.path.join(self.out_dir, a) for a in files]
                zip_file.extractall(self.out_dir)
                zip_file.close()

        return copied_files

    @staticmethod
    def sdrf_mask(sdrf_header, sdrf_content):
        u = []
        simple_node_mask = []
        nameNodeIndex = []  # I think not used
        prev_node_column_map = {}
        next_node_column_map = {}
        prev_node_map = {}
        next_node_map = {}
        prev_node_column_idx = -1
        # here we remember, for each Name column,
        # the previous Name column index (prevNodeColumnMap),
        # as well as the next one (nextNodeColumnMap);
        # -1 if does not exist.
        for h in range(len(sdrf_header)):
            u.append(set())  # this will be, for each column, a set of unique values in that column
            if sdrf_header[h].lower().startswith('sample') or sdrf_header[h].lower().startswith('source'):
                simple_node_mask.append(True)
            elif sdrf_header[h].lower().endswith("name"):
                simple_node_mask.append(False)
                # initially we think that this column will not have useful info for PageTab
                # (i.e., all nodes will have a single previous node and a single next node)

                prev_node_column_map[h] = prev_node_column_idx
                if prev_node_column_idx != -1:
                    next_node_column_map[prev_node_column_idx] = h
                prev_node_column_idx = h
            else:
                simple_node_mask.append(True)
        if prev_node_column_idx != -1:
            next_node_column_map[prev_node_column_idx] = -1
        # for j in range(1, len(sdrf_content)):
        #     current_line = sdrf_content[j].rstrip('\n').split('\t')
        #     for h in range(len(sdrf_header)):
        #         u[h].add(current_line[h])
        #         if sdrf_header[h].lower().endswith("name"):
        #             cur_node = current_line[h]
        #             if prev_node_column_map[h] != -1:
        #                 prev_node = current_line[prev_node_column_map[h]]
        #                 if cur_node in prev_node_map.keys():
        #                     if prev_node_map[cur_node] != prev_node:
        #                         simple_node_mask[h] = True
        #                         # we have found that this node actually has several previous nodes
        #                         # therefore the column cannot be ignored
        #                 else:
        #                     prev_node_map[cur_node] = prev_node
        #             if next_node_column_map[h] != -1:
        #                 next_node = current_line[next_node_column_map[h]]
        #                 if cur_node in next_node_map.keys():
        #                     if next_node_map[cur_node] != next_node:
        #                         simple_node_mask[h] = True
        #                 else:
        #                     next_node_map[cur_node] = next_node
        mask = []
        # this will need a better name. Singletons are unique values (except when they are file names)
        for h in range(len(sdrf_header)):
            if len(u[h]) == 1 and not sdrf_header[h].lower().endswith("file") and 'ftp' not in sdrf_header[h].lower():
                mask.append(False)  # we will not want singleton columns in the file list
            elif not simple_node_mask[h] or 'ftp' in sdrf_header[h].lower():
                # ignoring also FTP file columns, as well as columns that contain only "simple" nodes
                mask.append(False)
            else:
                mask.append(True)
        return mask

    @staticmethod
    def parse_idf(idf_file_path):
        with open(idf_file_path, "r") as idf_file:
            idf_content = idf_file.readlines()
        line_to_read = 0
        idf_dict = {}  # map of IDF properties
        while line_to_read < len(idf_content):
            line = idf_content[line_to_read]
            line_to_read += 1
            line = line.rstrip('\n').rstrip('\r')
            a = line.split('\t')
            while a and a[-1] == "": a.pop()  # delete empty values from the end
            if len(a) < 1: continue
            key = a[0].strip()
            value = a[1:]
            for i, v in enumerate(value):  # strip quotes
                if v.startswith('"') and v.endswith('"'):
                    value[i] = v[1:-1]
            if key in idf_dict.keys():
                idf_dict[key] += value
            else:
                idf_dict[key] = value
        return idf_dict

    def run(self, ):

        if self.force:
            delete_command = "java -jar %s -o TRANKLUCATE -s %s -u %s -p %s '%s'" % (
                SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, self.accession)

            # print(delete_command)
            del_out, del_err = execute_command(delete_command)
            # print(del_out)
            # print(del_err)
            #
        copied_files = self.convert_experiment_to_page_tab()

        command = "java -jar %s -o createupdate -s %s -u %s -p %s -d %s" % (
            SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, self.page_tab_filename)
        # print(command)
        # print("args: ",
        #       SUBMISSION_TOOL_PATH, SUBMISSION_SERVER, SUBMISSION_USERNAME, SUBMISSION_PASSWORD, self.page_tab_filename)
        out, err = execute_command(command)
        if 'error' in err.lower() or 'error' in out.lower():
            print(self.accession)
            print(out)
            print(err)
            print('=' * 50)

        # print(execute_command(command)[1])
        # for f in list(set(copied_files)):
        #     print("removing: " + f)
        #     os.remove(f)

    def get_organism(self, sdrf_content, header):
        idx = []
        organisms = []
        for i in range(len(header)):
            if '[organism]' in header[i].strip().replace(' ', ''):
                idx.append(i)
        for l in sdrf_content[1:]:
            line = l.strip().split(('\t'))
            for i in idx:
                organisms.append(line[i])
        return list(set(organisms))

    def export(self):
        # print(self.out_dir)
        # print ("A7eeeh: ", os.pathsep.join(self.out_dir.split(os.pathsep)[:-1]))
        cmd = "%s --no-adf --output %s %s" % (EXPORTER_SCRIPT, '/'.join(self.out_dir.split('/')[:-1]), self.accession)
        # print(cmd)
        out, err = execute_command(cmd)
        if 'error' in err.lower():
            print('EXPORT ERROR %s\n' % self.accession, out, err)


def main():
    parser = argparse.ArgumentParser(
        description='Converts a given ArrayExpress experiment into a BioStudy and submit it to BioStudies database.')
    parser.add_argument('accession', metavar='E-MTAB-xxxx', type=str,
                        help='''The accession number for the ArrayExpress Study''')
    parser.add_argument('-s',
                        '--server',
                        default='beta',
                        const='beta',
                        nargs='?',
                        choices=['production', 'beta', 'dev'],
                        help='submission server(default: %(default)s)')
    parser.add_argument('-f',
                        '--force', action='store_true',
                        help='Force delete Study if exists.')
    args = parser.parse_args()
    dir_path = os.path.join(LOAD_DIR, args.accession.split('-')[1], args.accession)
    file_names = os.listdir(dir_path)
    converter = MAGETABConverter(args.accession, dir_path, file_names, args.force)
    converter.run()


if __name__ == '__main__':
    _acc = argv[1]
    dir_path = os.path.join(LOAD_DIR, _acc.split('-')[1], _acc)
    file_names = os.listdir(dir_path)
    # print(dir_path)
    converter = MAGETABConverter(_acc, dir_path, file_names, True)
    converter.run()
    # exit()
    # main()
    # main()
    # exp = retrieve_public_experiments()[0]
    # if exp:
    #     # print(len(exp))
    #     i = 0
    #     for e in exp:
    #         d=e.acc
    #         # print(d)
    #         a_d = os.path.join(LOAD_DIR,d.split('-')[1], d)
    #         # print(a_d, os.path.isdir(a_d))
    #         if os.path.isdir(a_d):
    #
    #                     adf = MAGETABConverter(accession=d, dir_path=a_d,file_names=os.listdir(a_d), force=True)
    #                     adf.run()
    #         i+=1
    #         if i==25:
    #             exit()
