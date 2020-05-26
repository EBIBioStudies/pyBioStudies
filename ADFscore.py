from __future__ import print_function
import os
from stat import *
import collections
import sys


# p = "/ebi/ftp/pub/databases/microarray/data/array/MEXP/A-MEXP-1502"
# p = "/ebi/ftp/pub/databases/microarray/data/array/MEXP"
import requests


def score_adf(lines):
    print('Lines: ',len(lines))
    inHeader = True
    headerParsed = False
    repCol = -1  # column with Reporter names
    csCol = -1  # column with Composite Sequence names
    repSeq = -1  # column with Reporter sequences
    repAnnot = []  # columns with Reporter annotations
    csAnnot = []  # columns with CS annotations
    totalRep = 0
    totalCS = 0
    totalGoodRep = 0
    totalGoodCS = 0
    allReporters = set()
    # f = open(x[0] + '/' + y, "rb")
    # lines = f.read().replace('\r', '\n').split('\n')
    # f.close()
    for line in lines:
        # print(line)
        if inHeader and not line.startswith("[main]"): continue  # skip until [main]
        inHeader = False
        if line.startswith("[main]"): continue
        line = line.strip()
        if line =='':
            continue

        a = line.split('\t')
        if not headerParsed:
            for i, v in enumerate(a):
                if v == "Reporter Name": repCol = i
                if v == "Composite Element Name": csCol = i
                if v == "Reporter Sequence": repSeq = i
                if v.startswith("Reporter Database Entry"): repAnnot.append(i)
                if v.startswith("Composite Element Database Entry"): csAnnot.append(i)
            headerParsed = True
            continue

        try:
            if repCol != -1:
                if a[repCol] == "" or a[repCol] in allReporters: continue  # should not count the same Reporter many times
                totalRep += 1
                allReporters.add(a[repCol])
                if repSeq != -1 and len(a[repSeq]) > 10:
                    totalGoodRep += 1  # if there is sequence of length at least 11, this is a Good Reporter ..
                elif len(repAnnot) != 0:
                    for i in range(0, len(repAnnot)):
                        if a[repAnnot[i]] != "":
                            totalGoodRep += 1  # .. or, some annotation - also a Good Reporter
                            break
        except:
            pass
        try:
            if csCol != -1:
                if a[csCol] != "": totalCS += 1
                if len(csAnnot) != 0:
                    for i in range(0, len(csAnnot)):
                        if a[csAnnot[i]] != "":
                            totalGoodCS += 1
                            break
        except:
            pass

    arrayScore = 0
    if (totalRep > 0 and totalGoodRep / float(totalRep) > .7) or (totalCS > 0 and totalGoodCS / float(
            totalCS) > 0): arrayScore = 1
    # print(y[:y.find('.')], totalGoodRep, totalRep, totalGoodCS, totalCS, arrayScore)
    a =  {'good_reps': totalGoodRep, 'total_reps': totalRep, 'good_cs': totalGoodCS, 'total_cs': totalCS,
            'score': arrayScore}
    print(a)
    return a
if __name__ == '__main__':
    from sys import argv
    accession = argv[1]
    url = "https://www.ebi.ac.uk/arrayexpress/files/%s/%s.adf.txt" % (accession, accession)
    print(url)
    r = requests.get(url)
    print(type(r.content.decode('utf8')))
    lines = r.content.decode('utf8').split('\n')
    # print(lines)
    score_adf(lines)