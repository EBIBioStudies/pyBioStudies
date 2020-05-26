import os
import string
import unicodedata
import chardet
from clint.textui import colored

# from dal.oracle.era.sample import retrieve_alias_by_sample_id
# from settings import ENA_FTP_URI, TEMP_FOLDER
from common import fix_encoding
from dal.ena.sample import retrieve_alias_by_sample_id

__author__ = 'Ahmed G. Ali'


class SdrfRaw:
    def __init__(self, index, source, layout, assay_name, extract, data_file, md5, performer, library_selection,
                 library_source, library_strategy, nominal_length, nominal_sdev, exp_protocols, extract_protocol,
                 factors, chars, spot_length, read_index, ena_sample, bio_sample, ena_alias, ena_run, fastq_url,
                 combined=False):
        # # print extract_protocol
        # exit()
        self.combined = combined
        self.index = index
        self.source = source
        self.layout = layout
        self.assay_name = assay_name
        self.original_assay = assay_name
        self.data_file = data_file
        self.extract = extract
        self.md5 = md5
        self.ena_run = ena_run
        self.fastq_url = fastq_url
        # encoding = chardet.detect(performer)
        # char_set = encoding['encoding']
        # if not char_set:
        #     pass
        # else:
        #     performer = performer.decode('utf8')
        #     performer = unicodedata.normalize('NFKD', performer).encode('ascii', 'ignore').decode('ascii', 'ignore')
        #     performer = u"".join([c for c in performer if not unicodedata.combining(c)])
        # self.performer = performer.encode('utf8', 'ignore')

        self.library_selection = library_selection
        self.library_source = library_source
        self.library_strategy = library_strategy
        self.nominal_length = nominal_length
        self.nominal_sdev = nominal_sdev
        self.exp_protocols = exp_protocols
        self.extract_protocol = extract_protocol
        self.factors = factors
        self.chars = chars
        self.spot_length = spot_length
        self.read_index = read_index
        self.is_paired = False
        self.pair_order = ''
        self.new_data_file = data_file
        if layout == 'PAIRED':
            self.is_paired = True
        self.bio_sample = bio_sample
        self.ena_sample = ena_sample
        self.ena_alias = ena_alias
        self.extract_detailed = self.extract
        if self.layout and self.extract:
            self.extract_detailed = self.extract + '_' + self.layout[0].lower()

        self.fq_uri = None

    def rename_data_file(self):
        parts = self.data_file.replace('-', '_').split('.')
        name = parts[0]
        underscore = name.rfind('_')
        new_name = name[:underscore - 2] + name[underscore + 1:] + '_' + name[underscore - 2:underscore]
        self.new_data_file = '.'.join([new_name] + parts[1:])


class SdrfCollection:
    def __init__(self, file_path, combined=False, neglect_patterns=[], combined_pairs=False, mixed_pairs=False,
                 has_extra_rows=False, b10_x=False):
        self.file_path = file_path
        self.neglect_patterns = neglect_patterns
        self.combined = combined
        self.sdrf_index = None
        self.rows = []
        self.source_index = 0
        self.layout_index = 0
        self.assay_index = 0
        self.data_index = 0
        self.md5_index = 0
        self.header = []
        self.pairs = []
        self.file_path = file_path
        self.combined_pairs = combined_pairs
        self.b10_x = b10_x
        if self.b10_x:
            self.combined_pairs = True
        self.lines = []
        fix_encoding(self.file_path)
        self.__load_sdrf_file()
        self.mixed_pairs = mixed_pairs
        self.has_extra_rows = has_extra_rows
        self.extra_rows = []

    def __load_sdrf_file(self):
        self.rows = []
        f = open(self.file_path, 'r')
        lines = [l.strip() for l in f.readlines() if l.strip() != '']
        f.close()
        self.lines = lines[:]
        if self.combined:
            self.sdrf_index = [l.lower() for l in lines].index('[sdrf]') + 1
            if lines[self.sdrf_index] == '':
                self.sdrf_index += 1
        lines = lines[self.sdrf_index:]
        header = lines[0]
        self.header = [i.replace(' [', '[').strip('\"') for i in header.strip().split('\t')]
        # print(self.header)
        self.source_index = self.header.index('Source Name') if 'Source Name' in self.header else 0
        self.layout_index = self.header.index(
            'Comment[LIBRARY_LAYOUT]') if 'Comment[LIBRARY_LAYOUT]' in self.header else None
        self.library_selection_index = self.header.index(
            'Comment[LIBRARY_SELECTION]') if 'Comment[LIBRARY_SELECTION]' in self.header else None
        self.library_source_index = self.header.index(
            'Comment[LIBRARY_SOURCE]') if 'Comment[LIBRARY_SOURCE]' in self.header else None
        self.library_strategy_index = self.header.index('Comment[LIBRARY_STRATEGY]') if 'Comment[LIBRARY_STRATEGY]' in self.header else None
        self.nominal_length_index = self.header.index(
                'Comment[NOMINAL_LENGTH]') if 'Comment[NOMINAL_LENGTH]' in self.header else None
        self.nominal_sdev_index = self.header.index(
            'Comment[NOMINAL_SDEV]') if 'Comment[NOMINAL_SDEV]' in self.header else None
        self.spot_length_index = self.header.index(
            'Comment[SPOT_LENGTH]') if 'Comment[SPOT_LENGTH]' in self.header else None
        self.read_index_index = self.header.index(
            'Comment[READ_INDEX_1_BASE_COORD]') if 'Comment[READ_INDEX_1_BASE_COORD]' in self.header else None
        self.ena_sample_index = self.header.index(
            'Comment[ENA_SAMPLE]') if 'Comment[ENA_SAMPLE]' in self.header else None
        self.ena_run_index = self.header.index(
            'Comment[ENA_RUN]') if 'Comment[ENA_RUN]' in self.header else None
        self.fastq_url_index = self.header.index(
            'Comment[FASTQ_URI]') if 'Comment[FASTQ_URI]' in self.header else None
        self.bio_sample_index = self.header.index(
            'Comment[BioSD_SAMPLE]') if 'Comment[BioSD_SAMPLE]' in self.header else None
        self.assay_index = self.header.index('Assay Name') if 'Assay Name' in self.header else None
        # self.organism_index = self.header.index('Comment[Organism]') if 'Comment[Organism]' in self.header else None
        # # print self.assay_index
        # # print self.header
        # exit()
        self.data_index = None
        if 'Array Data File' in self.header:
            self.data_index = self.header.index('Array Data File')
        # elif 'Scan Name' in header:
        #     self.data_index = self.header.index('Scan Name')
        elif 'Comment[Array Data File]' in self.header:
            self.data_index = self.header.index('Comment[Array Data File]')
        elif 'Comment [Array Data File]' in self.header:
            self.data_index = self.header.index('Comment [Array Data File]')

        self.md5_index = None
        if 'Comment[MD5]' in header:
            self.md5_index = self.header.index('Comment[MD5]')

        self.extract_index = self.header.index('Extract Name') if 'Extract Name' in self.header else None
        self.performer_index = self.header.index('Performer') if 'Performer' in self.header else None
        self.factors_indexes = [i for i, x in enumerate(self.header) if "Factor Value" in x]
        self.exp_indexes = [i for i, x in enumerate(self.header[:self.extract_index]) if x == "Protocol REF"]
        self.char_indexes = [i for i, x in enumerate(self.header[:self.extract_index]) if "Characteristics" in x]
        self.extract_protocol_index = [i for i, x in enumerate(self.header[:self.assay_index]) if x == "Protocol REF"] \
            if self.assay_index else None
        if self.extract_protocol_index:
            self.extract_protocol_index = self.extract_protocol_index[-1]
        else:
            self.extract_protocol_index = None
        repeated_files = {}
        added_files = []
        for i in range(1, len(lines)):
            line = lines[i].strip()
            # encoding = chardet.detect(str.encode(line))
            # char_set = encoding['encoding']
            # if not char_set:
            #     continue
            # line = line.decode('utf-8')
            # line = unicodedata.normalize('NFKD', line)
            # line = u"".join([c for c in line if not unicodedata.combining(c)])
            # line = filter(lambda x: x in string.printable, line)
            # # print type(line)
            # exit()
            # lines[i] = line.encode('utf8', 'ignore')
            # lines[i] = line
            if line == '':
                continue
            line = [part.strip('\"') for part in line.split('\t')]

            factors = []
            chars = []
            for index in self.factors_indexes:
                if len(line) > index:
                    if line[index] == '':
                        continue
                    factors.append({line[index]: self.header[index].split('[')[1].split(']')[0]})
            for index in self.char_indexes:
                unit = None
                if len(line) > index:
                    if 'unit' in self.header[index + 1].lower():
                        unit = line[index + 1]
                    chars.append({
                        self.header[index].replace('Characteristics[', '').replace(']', '').capitalize(): {'value': line[index],
                                                                                              'unit': unit}
                    })
            nominal_length = None
            nominal_sdev = None
            spot_length = ''
            read_index = ''
            ena_sample = None
            bio_sample = None
            ena_run = None
            fastq_url = None
            ena_alias = None
            if self.nominal_length_index:
                try:
                    nominal_length = line[self.nominal_length_index]
                except:
                    pass
            if self.nominal_sdev_index:
                try:
                    nominal_sdev = line[self.nominal_sdev_index]
                except:
                    pass
            if self.spot_length_index:
                try:
                    spot_length = line[self.spot_length_index]
                except:
                    pass
            if self.read_index_index:
                try:
                    read_index = line[self.read_index_index]
                except:
                    pass
            if self.data_index and len(line) > self.data_index:

                if line[self.data_index] not in added_files:
                    added_files.append(line[self.data_index])
                if line[self.data_index] in repeated_files.keys():
                    repeated_files[line[self.data_index]].append(line[self.source_index])
                else:
                    repeated_files[line[self.data_index]] = [line[self.source_index]]
            if self.ena_sample_index:
                ena_sample = line[self.ena_sample_index]
                if ena_sample.strip():
                    ena_alias = retrieve_alias_by_sample_id(ena_sample)[0].sample_alias
                else:
                    ena_alias = ''
            if self.bio_sample_index:
                bio_sample = line[self.bio_sample_index]
            if self.ena_run_index:
                if len(line) > self.ena_run_index:
                    ena_run = line[self.ena_run_index]

            if self.fastq_url_index:
                if len(line) > self.fastq_url_index:
                    fastq_url = line[self.fastq_url_index]
                else:
                    fastq_url = ''
            md5 = ''
            if self.md5_index:
                md5 = line[self.md5_index]
            # # print line[self.data_index]
            data_file = None
            if self.data_index and len(line) > self.data_index:
                data_file = line[self.data_index]
            self.rows.append(
                SdrfRaw(
                    index=i,
                    source=line[self.source_index],
                    layout=line[self.layout_index] if self.layout_index else None,
                    assay_name=line[self.assay_index] if self.assay_index else None,
                    extract=line[self.extract_index] if self.extract_index else None,
                    data_file=data_file,
                    md5=md5,
                    performer=line[self.performer_index] if self.performer_index else None,
                    library_selection=line[self.library_selection_index] if self.library_selection_index else None,
                    library_source=line[self.library_source_index] if self.library_source_index else None,
                    library_strategy=line[self.library_strategy_index] if self.library_strategy_index else None,
                    nominal_length=nominal_length,
                    nominal_sdev=nominal_sdev,
                    exp_protocols=[line[i] for i in self.exp_indexes if len(line) > i],
                    extract_protocol=line[self.extract_protocol_index] if self.extract_protocol_index else None,
                    factors=factors,
                    chars=chars,
                    spot_length=spot_length,
                    read_index=read_index,
                    ena_sample=ena_sample,
                    bio_sample=bio_sample,
                    ena_alias=ena_alias,
                    ena_run=ena_run,
                    fastq_url=fastq_url,
                    combined=self.combined_pairs
                )
            )
        repeated = False
        # msg = []
        # for k, v in repeated_files.items():
        #     l = list(set(v))
        #     if len(l) > 1:
        #         repeated = True
        #         msg.append('%s was used by %s samples: %s' % (k, str(len(l)), ', '.join(l)))
        #
        # if repeated:
        #     raise Exception('ERROR:\n These file(s) are repeated in the SDRF. Please correct it:\n' + '\n'.join(msg))
        #     # print colored.red('ERROR:\n These file(s) are repeated in the SDRF. Please correct it:\n') + colored.blue(
        #     #     '\n'.join(msg))
        #     exit()

    def generate_file_to_check(self, file_path, added_samples=[]):
        # # print added_samples
        # # print colored.green('%s Samples are being added' % str(len(self.rows) - len(added_samples)))
        lines = ['\t'.join(['Comment[LIBRARY_LAYOUT]', 'Assay Name', 'Array Data File', 'Comment[MD5]'])]
        for row in self.rows:
            if row.source in added_samples or '.bam' in row.new_data_file:
                continue
            # # print colored.red(row.new_data_file)
            lines.append(u'\t'.join([row.layout, row.assay_name, row.new_data_file, row.md5]))
        f = open(file_path, 'w')
        n_line = u'%s' % os.linesep
        all = n_line.join(lines)
        all = all.encode('utf8', 'ignore')
        # # print all
        # exit()
        f.write(all)
        f.close()

    def __find_pairs(self, renamed=False):
        # if renamed:
        # print colored.black('RENAMED')
        paired_rows = [r for r in self.rows if r.is_paired]
        paired_index = []
        for i in range(len(paired_rows)):
            if i in paired_index:
                continue
            # # print paired_rows[i].new_data_file
            pair_found = False
            splitter = '.' + paired_rows[i].new_data_file.split('.')[-2]
            common_part_i = paired_rows[i].new_data_file.split(splitter)[0].replace('.txt', '')
            if common_part_i.endswith('R1_001'):
                common_part_i = common_part_i.replace('R1_001', 'R1')
                # paired_rows[i].new_data_file = paired_rows[i].new_data_file.replace('R1_001', 'R1')
            elif common_part_i.endswith('R2_001'):
                common_part_i = common_part_i.replace('R2_001', 'R2')
                # paired_rows[i].new_data_file = paired_rows[i].new_data_file.replace('R2_001', 'R2')
            if common_part_i.endswith('R_001'):
                common_part_i = common_part_i.replace('R_001', 'R')
                # paired_rows[i].new_data_file = paired_rows[i].new_data_file.replace('R_001', 'R')
            if common_part_i.endswith('F_001'):
                common_part_i = common_part_i.replace('F_001', 'F')
                # paired_rows[i].new_data_file = paired_rows[i].new_data_file.replace('F_001', 'F')
            for j in range(i + 1, len(paired_rows)):
                if j in paired_index:
                    continue
                common_part_j = paired_rows[j].new_data_file.split(splitter)[0].replace('.txt', '')
                if common_part_j.endswith('R1_001'):
                    common_part_j = common_part_j.replace('R1_001', 'R1')
                    # paired_rows[j].new_data_file = paired_rows[j].new_data_file.replace('R1_001', 'R1')
                elif common_part_j.endswith('R2_001'):
                    common_part_j = common_part_j.replace('R2_001', 'R2')
                    # paired_rows[j].new_data_file = paired_rows[j].new_data_file.replace('R2_001', 'R2')
                if common_part_j.endswith('R_001'):
                    common_part_j = common_part_j.replace('R_001', 'R')
                    # paired_rows[j].new_data_file = paired_rows[j].new_data_file.replace('R_001', 'R')
                if common_part_j.endswith('F_001'):
                    common_part_j = common_part_j.replace('F_001', 'F')
                    # paired_rows[j].new_data_file = paired_rows[j].new_data_file.replace('F_001', 'F')
                tmp_assay_name = common_part_i

                for pattern in self.neglect_patterns:
                    common_part_i = common_part_i.replace(pattern, '')
                    common_part_j = common_part_j.replace(pattern, '')
                if common_part_i[:-1] == \
                        common_part_j[:-1]:
                    if paired_rows[i].source != paired_rows[j].source:
                        break
                    pair_found = True
                    paired_index.append(i)
                    paired_index.append(j)
                    paired_rows[i].pair_order = '_1'
                    paired_rows[j].pair_order = '_2'
                    self.pairs.append([paired_rows[i], paired_rows[j]])
                    paired_rows[i].assay_name = tmp_assay_name[:-1]
                    paired_rows[j].assay_name = tmp_assay_name[:-1]
                    break
                    # else:
                    #     # print paired_rows[i].new_data_file.split(splitter)[0].replace('.txt', '')[:-1]
                    #     # print paired_rows[j].new_data_file.split(splitter)[0].replace('.txt', '')[:-1]
                    #     exit()
            if not pair_found:
                if not self.mixed_pairs:
                    if not renamed:
                        # # print colored.yellow(
                        #     'No pairs found. Going to check for combined files or rename the file names')
                        # # print paired_rows[i].new_data_file, paired_rows[i].data_file
                        # exit()
                        if len(list(set([r.source for r in self.rows]))) == len(self.rows):
                            # self.combined_pairs = True
                            paired_rows[i].combined = True
                            # # print colored.cyan('Files are combined. No need for validating fastq.')
                            # print colored.cyan('%s is combined.' % paired_rows[i])
                            continue
                            self.__load_sdrf_file()
                            self.pairs = []
                            return
                        # # print colored.yellow('Renaming data files')
                        # self.rename_data_files()
                        self.pairs = []
                        self.__find_advanced_pairs()
                        # self.__find_pairs(renamed=True)
                        break
                    else:
                        for row in self.rows:
                            row.new_data_file = row.data_file
                        self.__find_advanced_pairs()
                        break
                else:
                    # print paired_rows[i].source
                    paired_rows[i].combined = True
                    paired_rows[i].assay_name = common_part_i[:-1]
                    self.pairs.append([paired_rows[i], paired_rows[i]])

    def rename_data_files(self):
        for row in self.rows:
            row.rename_data_file()

    def add_spot_length(self, files):
        # print files
        paired_rows = [r for r in self.rows if r.is_paired]
        if not paired_rows:
            return
        if not self.spot_length_index and 'Comment[SPOT_LENGTH]' not in self.header:
            self.header.insert(self.md5_index + 1, 'Comment[SPOT_LENGTH]')
            self.header.insert(self.md5_index + 2, 'Comment[READ_INDEX_1_BASE_COORD]')
        for row in self.rows:
            if row.new_data_file not in files.keys():
                # print 'already submitted', row.spot_length, row.read_index
                # print 'Not in files', row.new_data_file
                # exit()
                continue
            val = files[row.new_data_file]
            # print row.new_data_file, val
            if val is None:
                row.spot_length = ''
                row.read_index = ''
            else:
                row.spot_length = str(2 * val)
                row.read_index = str(val + 1)
                # self.save()

    def add_ena_accessions(self, samples, experiments, runs, biosamples):
        # for k,v in runs.items():
        #     # print '%s: %s' %(k, v)
        # exit()
        for source, acc in samples.items():
            rows = [r for r in self.rows if r.source.replace(' ', '').strip() == source.replace(' ', '').strip()]
            for row in rows:
                row.ena_sample = acc
        for source, acc in biosamples.items():
            rows = [r for r in self.rows if r.source.replace(' ', '').strip() == source.replace(' ', '').strip()]
            for row in rows:
                row.bio_sample = acc

        for exp, acc in experiments.items():
            rows = [
                r for r in self.rows
                if r.extract_detailed.replace(' ', '').strip() == exp.replace(' ', '').strip()
                   or r.extract.replace(' ', '').strip() == exp.replace(' ', '').strip()
            ]
            if not rows:
                rows = [r for r in self.rows if r.extract.replace(' ', '').strip() == exp.replace(' ', '').strip()]
                if not rows:
                    # # print str(experiments)
                    # # print '-' * 30
                    # # print str([r.extract_detailed.replace(' ', '') for r in self.rows])
                    # # print '-' * 30
                    # # print str([r.extract.replace(' ', '') for r in self.rows])
                    # # print '-' * 30
                    # # print exp, acc
                    # # print '-' * 30
                    continue
                    raise Exception('afas')
                    raise Exception(str(experiments) + '\n==========================\n' +
                                    str([r.extract_detailed.replace(' ', '') for r in self.rows]))
            for row in rows:
                row.ena_experiment = acc
        # # print runs
        # # print len(runs.items())
        added_rows = []
        for run, acc in runs.items():
            # # print run, acc
            vol = None
            sub_dir = acc[:6]
            if len(acc) > 9:
                vol = acc[9:]
                vol = '0' * (3 - len(vol)) + vol

            rows = [r for r in self.rows if
                    r.assay_name.strip() == run.strip()
                    or
                    r.original_assay.strip() == run.strip()
                    or
                    r.data_file.replace('.csfasta.gz', '').replace('.qual.gz', '').strip() ==
                    run.replace('.csfasta.gz', '').replace('.qual.gz', '').strip()
                    or
                    r.new_data_file.replace('.csfasta.gz', '').replace('.qual.gz', '').strip() ==
                    run.replace('.csfasta.gz', '').replace('.qual.gz', '').strip()
                    or
                    r.data_file.split(r.new_data_file.split('.')[-2])[0].strip() ==
                    run.split(r.new_data_file.split('.')[-2])[0].strip()
                    or
                    r.new_data_file.split(r.new_data_file.split('.')[-2])[0].strip() ==
                    run.split(r.new_data_file.split('.')[-2])[0].strip()

                    ]
            if not rows:
                rows = [
                    r for r in self.rows if r.assay_name.strip() in run or '_'.join(
                        r.data_file.split(r.new_data_file.split('.')[-2])[0].split('_')[:-1]
                    ).strip() == run.split(r.new_data_file.split('.')[-2])[0].strip()
                ]
            if not rows:
                rows = [r for r in self.rows if run in r.new_data_file]
                # # print '=======>' ,rows
                # exit()
            if not rows:
                continue
                # # print [
                #     r.assay_name for r in self.rows ]
                # # print r.new_data_file.split('.')[-2]
                # # print r.data_file.split(r.new_data_file.split('.')[-2])[0].strip()
                # # print run.split(r.new_data_file.split('.')[-2])[0].strip()
                raise Exception('a7a')
            for row in rows:
                added_rows.append(row)
                row.ena_run = acc
                row.fq_uri = ENA_FTP_URI + sub_dir
                if vol:
                    row.fq_uri += '/' + vol
                row.fq_uri += '/' + acc + '/' + acc
                row.fq_uri += '%s.fastq.gz' % row.pair_order
                # # print colored.red(row.__dict__)
                # # print colored.yellow('='*40)
                # # print len(added_rows)

                # for row in self.rows:
                #     # print row.source, row.ena_sample, row.ena_experiment, row.ena_run

    def get_lines_with_ena(self):
        f = open(self.file_path, 'r')
        lines = [l.strip() for l in f.readlines()]
        f.close()
        write_lines = []
        header = self.header[:]
        header.remove('Comment[MD5]')
        header[self.data_index] = 'Scan Name'
        header.insert(header.index('Scan Name') + 1, 'Comment[SUBMITTED_FILE_NAME]')
        header.insert(header.index('Scan Name') + 2, 'Comment[ENA_RUN]')

        header.insert(header.index('Source Name') + 1, 'Comment[ENA_SAMPLE]')
        header.insert(header.index('Source Name') + 2, 'Comment[BioSD_SAMPLE]')
        header.insert(header.index('Technology Type') + 1, 'Comment[ENA_EXPERIMENT]')

        if self.b10_x:
            header.insert(header.index('Scan Name') + 3, 'Comment[BAM_URI]')
        else:
            header.insert(header.index('Scan Name') + 3, 'Comment[FASTQ_URI]')

        write_lines.append('\t'.join(header))
        for i in range(1, len(lines)):
            line = lines[i].split('\t')
            if line == ['']:
                continue
            if self.combined:
                if i <= self.sdrf_index:
                    continue
            if self.combined:
                row = self.rows[i - (self.sdrf_index + 1)]
            else:
                row = self.rows[i - 1]
            del line[self.md5_index]
            # # print 'before', line[header.index('Technology Type')]

            line.insert(header.index('Comment[ENA_SAMPLE]'), row.ena_sample)
            line.insert(header.index('Comment[BioSD_SAMPLE]'), row.bio_sample)
            line.insert(header.index('Comment[ENA_EXPERIMENT]'), row.ena_experiment)
            line.insert(header.index('Comment[SUBMITTED_FILE_NAME]'), row.new_data_file)
            try:
                line.insert(header.index('Comment[ENA_RUN]'), row.ena_run)
            except:
                # # print row.__dict__
                raise

            if self.b10_x:
                line.insert(header.index('Comment[BAM_URI]'), row.fq_uri)
            else:
                line.insert(header.index('Comment[FASTQ_URI]'), row.fq_uri)

            # # print 'mid', line[header.index('Technology Type')]
            if 'Comment[SPOT_LENGTH]' in header:
                if len(line) < len(header):
                    line.insert(header.index('Comment[SPOT_LENGTH]'), row.spot_length)
                    line.insert(header.index('Comment[SPOT_LENGTH]') + 1, row.read_index)
                else:
                    line[header.index('Comment[SPOT_LENGTH]')] = row.spot_length
                    line[header.index('Comment[SPOT_LENGTH]') + 1] = row.read_index

            line[header.index('Scan Name')] = row.new_data_file
            # # print header
            line[header.index('Assay Name')] = row.original_assay

            # assert len(line) == len(header)
            # # print 'after', line[header.index('Technology Type')]
            # exit()
            l = []
            # # print line
            for x in line:
                if isinstance(x, unicode):
                    l.append(x.encode('utf8'))
                else:
                    l.append(x)
            add_line = '\t'.join(l)
            add_line = add_line.decode('utf-8', 'ignore')
            if row.combined:
                # print colored.yellow('%s is combined' % row.source)
                if self.b10_x:
                    write_lines.append(add_line)
                else:
                    write_lines.append(add_line.replace(row.fq_uri, row.fq_uri.replace('.fastq.gz', '_1.fastq.gz')))
                    write_lines.append(add_line.replace(row.fq_uri, row.fq_uri.replace('.fastq.gz', '_2.fastq.gz')))
            else:
                write_lines.append(add_line)
        return write_lines

    def save(self):
        f = open(self.file_path, 'r')
        lines = [l.strip() for l in f.readlines()]
        f.close()
        write_lines = []
        if self.combined:
            write_lines = lines[:self.sdrf_index]

        write_lines.append('\t'.join(self.header))
        for i in range(1, len(lines)):
            line = lines[i].split('\t')
            if line == ['']:
                continue
            if self.combined:
                if i < self.sdrf_index:
                    continue
            if self.combined:
                row = self.rows[i - (self.sdrf_index + 1)]
            else:
                row = self.rows[i - 1]
            line[self.data_index] = row.new_data_file
            line[self.assay_index] = row.assay_name
            if 'Comment[SPOT_LENGTH]' in self.header:
                line.insert(self.md5_index + 1, row.spot_length)
                line.insert(self.md5_index + 2, row.read_index)
            write_lines.append('\t'.join(line))

        f = open(self.file_path, 'w')
        f.write(os.linesep.join(write_lines))
        f.close()

    def __find_advanced_pairs(self):
        self.pairs = []
        paired_rows = [r for r in self.rows if r.is_paired]
        paired_index = []
        for i in range(len(paired_rows)):
            if i in paired_index:
                continue
            pair_found = False
            for j in range(i + 1, len(paired_rows)):
                if j in paired_index:
                    continue
                matched = self._check_matches(paired_rows[i], paired_rows[j])
                if matched:
                    if paired_rows[i].source == paired_rows[j].source:
                        self.pairs.append([paired_rows[i], paired_rows[j]])
                        paired_index.append(i)
                        paired_index.append(j)
                        pair_found = True
                        paired_rows[i].pair_order = '_1'
                        paired_rows[j].pair_order = '_2'
                        break
            if not pair_found:
                if not self.has_extra_rows:
                    # print colored.red("Couldn't find pair for sample: %s with file name: %s" % (
                    #     paired_rows[i].source, paired_rows[i].new_data_file), bold=True)
                    raise Exception('Pair not found')
                else:
                    self.extra_rows.append(paired_rows[i])


if __name__ == '__main__':
    # sdrf = SdrfCollection(file_path=os.path.join(TEMP_FOLDER, 'E-MTAB-3901', 'combined.txt'), combined=True)
    # sdrf = SdrfCollection(file_path=os.path.join(TEMP_FOLDER, 'submission7367_annotare_v1.sdrf.txt'))
    sdrf = SdrfCollection(file_path='/home/gemmy/Downloads/E-MTAB-1367.sdrf.txt')
    # sdrf.generate_file_to_check('/home/gemmy/Downloads/check.txt')
