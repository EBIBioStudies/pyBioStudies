import requests

from dal.biostudies.biostudies_transaction import retrieve_gxa_studies, insert_gxa_link
from settings import *
__author__ = 'Ahmed G. Ali'


def get_atlas_accessions():
    r = requests.get(ATLAS_API).json()
    # print(r.text)
    atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]
    sc_atlas_accs = [i.get('experimentAccession', None) for i in r.get('aaData', [])]
    return atlas_accs, sc_atlas_accs


def construct_result(res):
    experiments = {}
    for record in res:
        acc = record['accNo']
        if acc not in experiments.keys():
            experiments[acc] = {}
        experiments[acc][record['link_id']] = {
                    'section_id':record['section_id'],
                    'type': record['value']
                }

    return experiments

def main():
    atlas_accs, sc_atlas_accs = get_atlas_accessions()
    experiments = construct_result(retrieve_gxa_studies())

    for k, v in experiments.items():
        if k in atlas_accs:
            link_found, section_id = check_link_exists(v)
            if not link_found:
                insert_gxa_link(acc=k, section_id=section_id,link_type='gxa', dscr='Expression Atlas')
        elif k in sc_atlas_accs:
            link_found, section_id = check_link_exists(v, 'gxa-sc')
            if not link_found:
                insert_gxa_link(acc=k, section_id=section_id,link_type='gxa-sc', dscr='Single Cell Expression Atlas')
        # break


def check_link_exists(v, link_type='gxa'):
    link_found = False
    section_id = None
    for l_id, link in v.items():
        section_id = link['section_id']
        if link['type'] == link_type:
            link_found = True
            break
    return link_found, section_id


if __name__ == '__main__':
    main()