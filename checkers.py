from distutils.util import strtobool

from settings import EXP_TYPES_PATH

__author__ = 'Ahmed G. Ali'


def is_microarray(exp_type):
    if not exp_type:
        return True
    f = open(EXP_TYPES_PATH, 'r')
    lines = f.readlines()
    f.close()
    types = {}
    for line in lines:
        tmp = line.strip().split('\t')
        types[tmp[0].lower()] = [tmp[1], tmp[2]]
    if exp_type.lower() in types.keys():
        return strtobool(types[exp_type.lower()][0])
    return True


if __name__ == '__main__':
    t = is_microarray('transcription profiling by high throughput sequencing')
    print(t)
