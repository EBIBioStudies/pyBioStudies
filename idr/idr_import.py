__author__ = 'Ahmed G. Ali'

import os
from sys import argv


def generate_file_list(exp_dir, out_dir):
    files = os.listdir(exp_dir)
    annotation_file = [f for f in files if 'annotation.csv' in f.lower()]
    if annotation_file:
        annotation_file = annotation_file[0]
    else:
        raise Exception('Missing annotation file for %s' % exp_dir.split('/')[-1])
    filepath_file = [f for f in files if 'filepaths.tsv' in f.lower()]
    if filepath_file:
        filepath_file = filepath_file[0]
    else:
        raise Exception('Missing filesPath file for %s' % exp_dir.split('/')[-1])







def main(dir_path):
    out_dir = os.path.join(dir_path, 'biostudies')
    if os.path.exists(out_dir):
        os.rmdir(out_dir)

    os.mkdir(out_dir)
    files = os.listdir(dir_path)
    exps = [f for f in files if os.path.isdir(os.path.join(dir_path, f)) and 'experiment' in f.lower()]
    for e_dir in exps:
        generate_file_list(os.path.join(dir_path, e_dir), out_dir)

if __name__ == '__main__':
    dir_path = argv[1]
    main(dir_path)