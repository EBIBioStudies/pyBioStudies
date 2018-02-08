from dal.ae2 import db
from dal.common import execute_select

__author__ = 'Ahmed G. Ali'


def retrieve_arrays_by_exp_acc(exp_acc):
    sql = """SELECT distinct(ma.ARRAYACCESSION) as array
FROM AE2.NODE n  join study s on s.id=n.STUDY_ID join AE2.MATERIAL ma on n.MATERIAL_ID = ma.ID
where s.acc = '%s'""" % exp_acc
    return execute_select(sql, db)


def retrieve_public_experiments():
    sql = """select s.ACC from 
AE2.SC_LABEL la join AE2.SC_OWNER o on la.ID = o.SC_LABEL_ID 
join AE2.STUDY s on la.NAME = s.ACC 
where o.SC_USER_ID =1 and s.ACC like 'E-MTAB-%'
order by s.id asc """
    return execute_select(sql, db, True)
