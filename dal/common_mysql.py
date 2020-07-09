from __future__ import print_function

__author__ = 'Ahmed G. Ali'

import MySQLdb as mdb


def retrieve_connection(db):
    """
    Retrieves Database connection object for a given connection parameters.

    :param db: Json object containing connection parameters
    :type db: dict

    :return: MySQLDB.Connection object
    """
    con = mdb.connect(host=db['host'], user=db['username'], passwd=db['password'], port=db['port'], db=db['name'])
    return con


def execute_select(sql_stmt, db):
    """
    Executes select statement and returning list of results.

    :param sql_stmt: SQL statement to be executed.
    :type sql_stmt: str
    :param db: Json object containing connection string parameters
    :type db: dict
    :return: list of results from DB
    """
    res = []
    con = None
    try:
        con = retrieve_connection(db)
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute(sql_stmt)
        res = cur.fetchall()
    except mdb.Error as e:
        print(sql_stmt)
        print("Error %d: %s" % (e.args[0], e.args[1]))
    finally:
        if con:
            con.close()
    return res


def execute_insert(sql_stmt, db):
    """
    Executes insert/update statement

    :param sql_stmt: SQL statement to be executed.
    :type sql_stmt: str
    :param db: Json object containing connection string parameters
    :type db: dict
    """
    con = None
    try:
        con = retrieve_connection(db)
        cur = con.cursor(mdb.cursors.DictCursor)
        cur.execute(sql_stmt)
        con.commit()
    except mdb.Error as e:

        print("Error %d: %s" % (e.args[0], e.args[1]))
    finally:
        if con:
            con.close()
