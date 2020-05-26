__author__ = 'Ahmed G. Ali'
import dbms


def retrieve_connection(db):
    """
        Retrieves Database connection object for a given connection parameters.

        :param db: Json object containing connection parameters
        :type db: dict

        :return: oracle.dbms.Connection object
        """
    con = dbms.connect.oracle(user=db['username'], password=db['password'], database=db['name'], host=db['host'],
                              port=db['port'])
                              # port=db['port'], is_service=db.get('is_service', False))
    return con


def execute_select(sql_stmt, db, keep_connection=False):
    """Executes select statement and returning list of results.

    :param sql_stmt: SQL statement to be executed.
    :type sql_stmt: str
    :param db: Json object containing connection string parameters
    :type db: dict
    :param keep_connection: Keep the connection opened so that can be used to retrieve other nested objects.
                            e.g. the content of an xml object.
    :return: list of results from DB

        """
    res = []
    con = None
    try:
        con = retrieve_connection(db)
        # print(con.nencoding)
        cur = con.cursor()
        # print sql_stmt
        cur.execute(sql_stmt)
        res = cur.fetchall()
    except Exception as e:
        # raise
        print(e)
    finally:
        if keep_connection:
            return res, con
        if con:
            if cur:
                cur.close()
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
        cur = con.cursor()
        cur.execute(sql_stmt)
        con.commit()
    except Exception as e:
        raise Exception(str(e) + '\n' + sql_stmt)
        print(e)
        print("Error %d: %s" % (e.args[0], e.args[1]))
    finally:
        if con:
            con.close()
