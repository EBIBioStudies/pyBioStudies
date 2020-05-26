from dal.ae2 import db
from dal.common import execute_select

__author__ = 'Ahmed G. Ali'


def retrieve_arrays_by_exp_acc(exp_acc):
    sql = """SELECT distinct(ma.ARRAYACCESSION) as array
FROM AE2.NODE n  join study s on s.id=n.STUDY_ID join AE2.MATERIAL ma on n.MATERIAL_ID = ma.ID
where s.acc = '%s'""" % exp_acc
    return execute_select(sql, db)


def retrieve_public_experiments():
    sql = """select s.id, s.ACC from 
AE2.SC_LABEL la join AE2.SC_OWNER o on la.ID = o.SC_LABEL_ID 
join AE2.STUDY s on la.NAME = s.ACC 
where o.SC_USER_ID =1"""
    # print(sql)
    return execute_select(sql, db, True)

def retrieve_all_experiments():
    sql = """select s.id, s.ACC from 
AE2.SC_LABEL la join AE2.SC_OWNER o on la.ID = o.SC_LABEL_ID 
join AE2.STUDY s on la.NAME = s.ACC"""
    return execute_select(sql, db, True)


def retrieve_owner(acc):
    # print(acc, type(acc))
    sql = """SELECT * FROM AE2.SC_OWNER o, AE2.SC_USER u, AE2.SC_LABEL l, AE2.CONTACT c
WHERE o.SC_USER_ID=u.ID 
AND USERNAME not like 'Reviewer%%' 
AND u.ID != 1
AND o.SC_LABEL_ID = l.ID
AND l.NAME = '%s'
AND u.USEREMAIL = c.EMAIL""" % acc
    # print(sql)
    return execute_select(sql, db)

if __name__ == '__main__':
    print (retrieve_all_experiments())
