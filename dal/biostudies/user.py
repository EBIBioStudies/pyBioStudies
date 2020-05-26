import uuid

from dal.biostudies import db
from dal.common_mysql import execute_select, execute_insert

__author__ = 'Ahmed G. Ali'


def retrieve_user_by_email(email):
    sql = """SELECT * from User where email = '%s' or login ='%s'""" % (email, email)
    print(sql)
    return execute_select(sql, db=db)


def insert_user(email, full_name, username, password, secret):
    sql = """INSERT INTO User (email, fullname, login, passwordDigest, superuser, active, keyTime, secret) 
VALUES ('{email}', '{full_name}', '{username}', password('{password}'), 0,1, UNIX_TIMESTAMP (), '{secret}')""".format\
        (email=email, full_name=full_name, username=username, password=password, secret=secret)
    # print(sql)
    execute_insert(sql, db=db)

def update_owner(accession, new_user):
    sql = """UPDATE Submission SET owner_id =%d where accNo = '%s'""" % (new_user, accession)
    # print(sql)
    execute_insert(sql, db)

def activate_user_by_id(u_id, email):
    sql = """UPDATE User set active=1, email='{}' where id = {}""".format(email, u_id)
    execute_insert(sql, db)


def delete_user_by_id(user):
    sql ="""DELETE FROM User where id ={} """.format(user)
    execute_insert(sql, db)