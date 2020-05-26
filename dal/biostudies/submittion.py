__author__ = 'Ahmed G. Ali'
from dal.biostudies import db
from dal.common_mysql import execute_select, execute_insert


def retrieve_submission_by_acc(acc):
    sql = """SELECT * FROM Submission WHERE accNo ='{acc}'""".format(acc=acc)
    # print(sql)
    return execute_select(sql, db)


def remove_submission(sub_id):
    sql = """DELETE FROM Submission where id={}""".format(sub_id)
    # print(sql)
    execute_insert(sql, db)


def get_subscount_by_userid(user_id):
    sql = """SELECT count(*) as `count` from Submission where owner_id = {}""".format(user_id)
    # print(sql)
    return execute_select(sql, db)
