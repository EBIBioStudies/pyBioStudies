import json
import logging

import requests


urllib3_log = logging.getLogger("urllib3")
urllib3_log.setLevel(logging.CRITICAL)
# requests_log = logging.getLogger("requests")
# requests_log.addHandler(logging.NullHandler())
# requests_log.propagate = False

__author__ = 'Ahmed G. Ali'


# def do_query(efo_id):
#     url = 'https://www.ebi.ac.uk/ols/api/ontologies/efo/children?id=' + efo_id
#     # print(url)
#     r = requests.get(url).json()
#     links = r['_links']
#     next_url = True
#     # if 'next' in links:
#     #     next_url = links['next']['href']
#     # print(next)
#     while next_url:
#         ch = []
#         res = r['_embedded']
#         terms = []
#         if 'terms' in res:
#             terms = res['terms']
#         for t in terms:
#             ch.append([t['short_form'], t['label']])
#         if 'next' in links:
#             next_url = links['next']['href']
#         else:
#             next_url = False
#     return ch


class EFO:
    def __init__(self, term_id, name=None, has_children=False):
        self.id = term_id
        self.name = name
        self.has_children = has_children
        self.children = []
        if not self.name:
            self.retrieve_term_name()
            # print(self.name)
        if self.has_children:
            self.retrieve_children()

    def find_term(self, term):
        found, lst, t_id = self.search(term)
        if found:
            if len(lst) > 1:
                return lst[1], t_id
            return lst[0], t_id
        return False

    def search(self, term):
        found = False
        t_id = None
        p = [self.name]
        for ch in self.children:
            if term.lower() == ch.name.lower():
                t_id = ch.id
                return True, p, t_id
            found, a, t_id = ch.search(term)

            if found:
                p += a
                break

        return found, p, t_id

    def retrieve_term_name(self):
        url = 'https://www.ebi.ac.uk/ols/api/select?q=%s&queryFields={obo_id}&ontology=efo' % self.id
        # print (url)
        r = requests.get(url)
        # print(r.text)
        r = r.json()
        
        self.name = r['response']['docs'][0]['label']

    def retrieve_children(self):
        url = 'https://www.ebi.ac.uk/ols/api/ontologies/efo/children?id=' + self.id
        
        r = requests.get(url).json()
        
        next_url = True
        # if 'next' in links:
        #     next_url = links['next']['href']
        # print(next)

        while next_url:
            res = {}
            if '_embedded' in r.keys():
                res = r['_embedded']
            links = r['_links']
            terms = []
            if 'terms' in res:
                terms = res['terms']
            for t in terms:
                self.children.append(EFO(term_id=t['short_form'], name=t['label'], has_children=t['has_children']))
            if 'next' in links:
                next_url = links['next']['href']
                r = requests.get(next_url).json()
            else:
                next_url = False


class EFOCollection:
    def __init__(self):
        self.array = EFO(term_id='EFO_0002696',has_children=True)
        self.seq = EFO(term_id='EFO_0003740',has_children=True)
        self.assay_by_molecule = EFO(term_id='EFO_0002772',has_children=True)
        # self.assay_by_instrument = EFO(term_id='EFO_0002773',has_children=True)

    def get_tags(self, exp_types):
        types = {}
        for exp_type in exp_types:
            result = self.array.find_term(exp_type)
            if result:

                if exp_type in types.keys():
                    types[exp_type]['technology'].append('Array assay')
                else:
                    types[exp_type] = {
                        'technology':['Array assay'],
                        'id' : result[1],
                        'molecule' : []
                    }

            result = self.seq.find_term(exp_type)
            if result:
                # result = list(result)
                # result[0] = 'Sequencing assay'
                # types[exp_type] = result
                if exp_type in types.keys():
                    types[exp_type]['technology'].append('Sequencing assay')
                else:
                    types[exp_type] = {
                        'technology':['Sequencing assay'],
                        'id' : result[1],
                        'molecule' : []
                    }

            result = self.assay_by_molecule.find_term(exp_type)

            if result:
                if exp_type in types.keys():
                    types[exp_type]['molecule'].append(result[0])
                else:
                    types[exp_type] = {
                        'technology':[],
                        'id' : result[1],
                        'molecule' : []
                    }
            # result = self.assay_by_instrument.find_term(exp_type)
            #
            # if result:
            #     if exp_type in types.keys():
            #         types[exp_type]['molecule'].append(result[0])
            #     else:
            #         types[exp_type] = {
            #             'technology':[],
            #             'id' : result[1],
            #             'molecule' : []
            #         }
        return types


if __name__ == '__main__':
    # efo = EFO(term_id='EFO_0002773', has_children=True)
    # print(efo)
    # f = efo.find_term('IP-seq')
    # print(f)
    # f2 = efo.find_term('A7a')
    # print(f2)
    # f3 = efo.find_term('454 Sequencing')
    # print(f3)
    # f4 = efo.find_term('ChIP-chip by array')
    # print(f4)
    # f4 = efo.find_term('Fast-ATAC')
    # print(f4)
    # f4 = efo.find_term('assay by sequencer')
    # print(f4)
    efo = EFOCollection()
    import pickle
    pickle.dump(efo, open('/home/gemmy/PycharmProjects/pyBioStudies/efo.binary', 'wb'))
    efo2 = pickle.load(open('/home/gemmy/PycharmProjects/pyBioStudies/efo.binary', 'rb'))
    print (efo2.get_tags(['proteomic profiling by array',  'RNA-seq of non coding RNA']))
    print (efo2.get_tags(['proteomic profiling by array']))
    print (efo2.get_tags(['transcription profiling by array']))

