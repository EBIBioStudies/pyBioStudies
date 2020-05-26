from dal.common import execute_select
from dal.ena import db

__author__ = 'Ahmed G. Ali'


def retrieve_xml_by_biosample_id_list(bio_samples_ids):
    sql = '''SELECT BIOSAMPLE_ID, xmltype.getStringVal(SAMPLE_XML) as sample_xml from SAMPLE WHERE BIOSAMPLE_ID in (%s)''' % ','.join(
        ["'%s'" % i for i in bio_samples_ids])

    return execute_select(sql, db)


def retrieve_alias_by_sample_id(ena_sample):
    sql = """SELECT SAMPLE_ALIAS from ERA.SAMPLE where SAMPLE_ID = '%s'""" % ena_sample
    print(sql)
    return execute_select(sql, db)


def retrieve_sample_by_acc(acc):
    return execute_select("""SELECT * FROM SAMPLE WHERE SAMPLE_ID = '%s'""" % acc, db)


def retrieve_samples_by_submission_acc(submission_acc):
    return execute_select("""Select * from SAMPLE where SUBMISSION_ID = '%s'""" % submission_acc, db)
