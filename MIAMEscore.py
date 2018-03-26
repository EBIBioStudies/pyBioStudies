from __future__ import print_function
import os

# from stat import *
# import collections
# import sys

# p = "/ebi/ftp/pub/databases/microarray/data/experiment/MTAB"
p = os.path.join(os.path.dirname(__file__), 'experiments')
# p = "/ebi/ftp/pub/databases/microarray/data/experiment/MTAB/E-MTAB-193"
filenum = 0


def score(content):
    # filenum += 1
    # # uncomment to score only public experiments
    # #			if oct(os.stat(x[0])[ST_MODE])[-1:]=='0':
    # #				continue

    lineToRead = 0
    aa = {}
    #			print(y)
    while lineToRead < len(content):
        line = content[lineToRead]
        lineToRead += 1
        line = line.rstrip('\n').rstrip('\r')
        a = line.split('\t')
        while a and a[-1] == "": a.pop()  # ignore epmty tokens
        if len(a) < 1: continue
        key = a[0].strip()
        value = a[1:]
        for i, v in enumerate(value):
            if v.startswith('"') and v.endswith('"'):
                value[i] = v[1:-1]
        aa[key] = value
    # Protocol scoring - here only what's in IDF.
    # Mismatches with the scores computed on the DB will occur, because sometimes protocol definitions in the DB and in IDF differ.
    # We need to extract the best protocol definition available, either from IDFs or the DB
    protocolScore = 0
    if aa.has_key("Protocol Name"):
        pNum = len(aa["Protocol Name"])
        for i in range(0, pNum):
            protocolScore = 1  # at least 1 protocol - good
            if aa.has_key("Protocol Description") and i < len(aa["Protocol Description"]) and len(
                    aa["Protocol Description"][i]) >= 50: continue
            if aa.has_key("Protocol Parameters") and i < len(aa["Protocol Parameters"]) and len(
                    aa["Protocol Parameters"][i]) > 0: continue
            protocolScore = 0  # if we are here - description too short, and no parameters
            break
    fvScore = True
    rawScore = True
    processedScore = True
    if aa.has_key("SDRF File"):
        sdrfNum = len(aa["SDRF File"])
        for i in range(0, sdrfNum):
            #					if i>0: print(y)
            with open(os.path.join(p, 'MTAB', 'E-MTAB-13', aa["SDRF File"][i]), "r") as fff:
                sdrfContent = fff.readlines()

            header = sdrfContent[0].rstrip('\n').split('\t')
            for h in range(len(header)):
                if header[h].startswith('"') and header[h].endswith('"'):
                    header[h] = header[h][1:-1]
                #					print(header)

                # find SDRF columns relevant for scoring
            assayCol = 0  # column with assay/hybridization names
            organismCols = []  # column(s) with Organisms
            fvCols = []  # column(s) with factor values
            rawCols = []  # column(s) with raw data
            derivedCols = []  # column(s) with derived data
            for h in range(len(header)):
                if header[h].startswith("Hybridization") or header[h].startswith("Assay"): assayCol = h
                if header[h].endswith("[Organism]") or header[h].endswith("[organism]"): organismCols.append(h)
                if header[h].startswith("Factor Value") or header[h].startswith("FactorValue"): fvCols.append(h)
                if header[h].startswith("Array Data") or header[h].startswith("Array data") or header[
                    h].startswith("ArrayData"): rawCols.append(h)
                if header[h].startswith("Derived"):    derivedCols.append(h)
                if header[h].endswith("[ENA_RUN]"): rawCols.append(h)

            # find assays that have annotation related to scoring
            assays = set()  # this is all assays
            assaysWithOrg = set()
            assaysWithFV = set()
            assaysWithRaw = set()
            assaysWithDerived = set()
            for j in range(1, len(sdrfContent)):
                currentLine = sdrfContent[j].rstrip('\n').split('\t')
                assayName = currentLine[assayCol]
                assays.add(assayName)
                for k in range(0, len(organismCols)):
                    if currentLine[organismCols[k]].strip() != "": assaysWithOrg.add(assayName)
                for k in range(0, len(fvCols)):
                    if currentLine[fvCols[k]].strip() != "": assaysWithFV.add(assayName)
                for k in range(0, len(rawCols)):
                    if currentLine[rawCols[k]].strip() != "": assaysWithRaw.add(assayName)
                for k in range(0, len(derivedCols)):
                    if currentLine[derivedCols[k]].strip() != "": assaysWithDerived.add(assayName)
                #					print(assays)
                #					print(assaysWithRaw)
                #					print(assays.difference(assaysWithRaw))
                #					print(assaysWithFV)
                #					print(rawCols)
                #					print(fvCols)

                # factor value score - 1 if there is at least 1 assay, and all assays have Organism and Factor value annotation
            curFvScore = False
            if len(assays) > 0 and len(assays.difference(assaysWithOrg)) == 0 and len(
                    assays.difference(assaysWithFV)) == 0: curFvScore = True
            fvScore = fvScore and curFvScore  # we do this because there may be more than 1 SDRF, and this score is 1 only if it is 1 for all SDRFs

            # all assays need raw data attached
            curRawScore = False
            if len(assays.difference(assaysWithRaw)) == 0: curRawScore = True
            rawScore = rawScore and curRawScore

            # all assays need processed data attached
            curProcessedScore = False
            if len(assays.difference(assaysWithDerived)) == 0: curProcessedScore = True
            processedScore = processedScore and curProcessedScore
    print('E-MTAB-13', 1 if fvScore else 0, 1 if rawScore else 0, 1 if processedScore else 0, protocolScore)

f = open(os.path.join(p, 'MTAB','E-MTAB-13', 'E-MTAB-13.idf.txt'))
content = f .readlines()
f.close()
score(content)
#
# for x in os.walk(p):
#     for y in x[2]:
#         if y.endswith("idf.txt"):
#             with open(x[0] + '/' + y, "r") as ff:
#                 content = ff.readlines()
#             score(content)
