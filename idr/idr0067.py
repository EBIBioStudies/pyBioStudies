__author__ = 'Ahmed G. Ali'

annot_file = "/home/gemmy/Desktop/imaging/IDR/idr0067/experimentA/idr0067-experimentA-annotation.csv"
file_paths_file = "/home/gemmy/Desktop/imaging/IDR/idr0067/experimentA/idr0067-experimentA-filePaths.tsv"

f = open(annot_file, 'r')
annotats_cntnt = f.readlines()
f.close()
f = open(file_paths_file, 'r')
file_paths_cntnt = f.readlines()
f.close()

data_sets = {}
for line in file_paths_cntnt:
    if not line.strip():
        continue
    l = line.strip().split('\t')
    project = l[0].split(':')[-1]
    f_path = [i for i in l[1].split('/') if 'ftp' in i][0]
    data_sets[project] =  f_path
header =annotats_cntnt[0].split(',')
header[1] = header[0]
header[0] = 'Files'

file_lst = ['\t'.join(header)]
path_index = header.index('Comment [Image File Path]')
for i in range(1, len(annotats_cntnt)):
    l = annotats_cntnt[i].split(',')
    data_set = l[0]
    f_name = l[1]
    print(l[path_index+1])
    f_path = data_sets[data_set]+'/' +l[path_index+1]+'/'+f_name

    # # path_list = [data_sets[l[0]].split('/')[0]] +l[path_index+1].split('/')
    # path_list = data_sets[l[0]].split('/')
    # unique_path_list = []
    # for x in path_list:
    #     if x not in unique_path_list:
    #         unique_path_list.append(x)
    #
    # f_name = '/'.join(unique_path_list)+'/' + f_name
    l[0] = f_path.replace('//','/')
    l[1] = data_set
    file_lst.append('\t'.join(l))

f = open('/home/gemmy/Desktop/imaging/IDR/idr0067/file_list.tsv', 'w')
f.write(''.join(file_lst))
f.close()
