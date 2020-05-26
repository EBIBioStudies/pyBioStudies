from dal.ae2 import db
from dal.common import execute_select

__author__ = 'Ahmed G. Ali'


def retrieve_study_type_by_accession(accession):
    sql = """SELECT * FROM STUDY_TYPE WHERE ACC='%s'""" % accession
    return execute_select(sql, db)
