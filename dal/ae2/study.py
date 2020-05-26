from dal.ae2 import db
from dal.common import execute_select

__author__ = 'Ahmed G. Ali'


def retrieve_release_date(accession):
    sql = "select RELEASEDATE from Study where acc = '%s'" % accession
    # print(sql)
    return execute_select(sql, db)
