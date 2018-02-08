"""
Create connection objects

The dbms.connect.Connection object provides a uniform DB API 2.0 interface 
to the underlying database connection objects.

The different database adapters are only imported in the factory functions
so they are not required unless an adapter is actually used. A reference
to the adapter is saved in the connection's interface variable.
"""

import os as _os
import sys as _sys

from . import cursors as _cursors
from . import probe as _probe
from . import schema as _schema

def _passwordPrompt(prompt='Enter password:\n'):
    if _sys.version_info[0] < 3:
        return str(raw_input(prompt))
    else:
        return input(prompt)


class Connection(object):
    """
    DBI 2.0 Compliant Connection object.
    :param connection: Connection of underlying adapter. Saved as _connection 
            and any methods or attributes not handled locally are delegated to this.
    :param interface: Reference to the underlying adapter. e.g. cx_Oracle, ibm_db_dbi, pymssql...
    :param dbname: name of connected database or name of database file
    
    In addition to the 
    """
    # variables stored in local __dict__ others will be sent to ._connection
    _localAttrs = ['_connection', '_server', 'dbname', 'inspect', 'interface',
                    'loggedCursor', 'name', 'paramHelp', 'placeholder', 'probe']
    
    def __init__(self, connection, interface, dbname=None):
        
        self._connection = connection #: reference to actual (cx_Oracle, ibm_db_dbi...) connection 
        self.interface = interface    #: reference to adapter (cx_Oracle, ibm_db_dbi...)
        self.dbname = dbname          #: database name
        
        # set placeholder
        if self.interface.paramstyle == 'qmark':
            self.placeholder = '?'    #: placeholder for variable replacement
        elif self.interface.paramstyle == 'numeric':
            self.placeholder = ':1'
        else:
            self.placeholder = '%s'
            
        # set server type
        if interface.__name__ == 'cx_Oracle':
            self._server = 'oracle'   #: name of database engine
            self.placeholder = ':1'
            self.probe = _probe.ProbeOracle(self) #: get detailed information of database schemata
        elif interface.__name__ in ('psycopg2', 'pgdb'):
            self._server = 'postgres'
            self.probe = _probe.ProbePostgres(self)
        elif interface.__name__ in ('MySQLdb', 'mysql.connector'):
            self._server = 'mysql'
            self.probe = _probe.ProbeMySQL(self)
        elif interface.__name__ in ('pymssql'):
            self._server = 'mssql'
            self.probe = _probe.ProbeMSSQL(self)
        elif interface.__name__ in ('sqlite', 'sqlite3'):
            self._server = 'sqlite'
            self.probe = _probe.ProbeSQLite(self)
        elif interface.__name__ in ('fdb', 'kinterbasd'):
            self._server = 'firebird'
            self.probe = _probe.ProbeFirebird(self)
        elif interface.__name__ in ('ibm_db', 'ibm_db_dbi', 'ibm_db_sa', 'ibm_db_django', 'pydb2'):
            self._server = 'db2'
            self.probe = _probe.ProbeDB2(self)
        elif interface.__name__ == 'pyodbc':
            self._server = 'odbc'
            self.probe  = _probe.Probe(self)
        else:
            self._server = 'unknown'
            
        if getattr(self, 'probe') :
            self.inspect = _schema.Inspect(self.probe) #: get names of database objects
            
    
    def __getattr__(self, key):
        # key was not found in __dict__ 
        if key in self._localAttrs:
            return None
        elif key == '__name__':
            if 'name' in self.__dict__:
                return self.name
            else:
                return self.dbname
        else:
            return getattr(self._connection, key)
    
    def __setattr(self, key, value):
        if key in self._localAttrs:
            self.__dict__[key] = value
        else:
            return setattr(self._connection, key, value)
        
    def __dir__(self):
        return list(set(dir(self._connection) + dir(Connection) + Connection._localAttrs))
    
    def __str__(self):
        if self.dbname:
            return '%s:%s' % (self.dbname, self._server)
        else:
            return self._server
    
    def cursor(self, cursorType=_cursors.DictCursor, **kwargs):
        """Return new Cursor object."""
        if callable(cursorType) and hasattr(cursorType, 'fetchone'):
            return cursorType(self, **kwargs)
        else:
            return _cursors.DictCursor(self, **kwargs)
        
    def loggedCursor(self, cursorType=_cursors.DictCursor, **kwargs):
        """Return new logged cursor. 
        By default queries are logged to the configuration database and are
        viewable from dbms.logger.view() and dbms.logger.search()."""
        from dbms import logger
        if kwargs:
            kwargs['logger'] = logger
        else:
            kwargs = {'logger': logger}
        if callable(cursorType) and hasattr(cursorType, 'fetchone'):
            return cursorType(self, **kwargs)
        else:
            return _cursors.DictCursor(self, **kwargs)
    
    @property    
    def paramHelp(self):
        """Print help on this interface's parameter style."""
        print("%s's parameter style is \"%s\"" % (self.interface.__name__, self.interface.paramstyle))
        print('''"SELECT * FROM people WHERE last_name = %s AND age > %s", ('Smith', 30)''' % (self.placeholder, self.placeholder))
        if self.interface.paramstyle == 'named':
            print('''"SELECT * FROM people WHERE last_name = :last_name AND age > :age", {'name': 'Smith', 'age': 30}''')
        elif self.interface.paramstyle == 'pyformat':
            print('''"SELECT * FROM people WHERE last_name = %(last_name)s AND age > %(age)s", {'name': 'Smith', 'age': 30}''')

#------------------------------------------------------------------------------# 
#                           Connection factories
#------------------------------------------------------------------------------#

def db2(user, password, database, host='localhost', **kwargs):
    """Return a connection to IBM DB2. Adapter ibm_db_dbi is required."""
    import ibm_db_dbi
    
    if not password:
        password = _passwordPrompt()
    dbc = ibm_db_dbi.connect(dsn=database, user=user, password=password, database=database, host=host, **kwargs)
    
    return Connection(dbc, ibm_db_dbi, database, **kwargs)

def firebird(user, password, database, host='localhost', port=3050, **kwargs):
    """Return a connection to Firebird. Adapter fdb is required."""
    import fdb
    
    if not password:
        password = _passwordPrompt()
    if 'charset' not in kwargs:
        kwargs['charset'] = 'UTF8'
    if 'sql_dialect' not in kwargs:
        kwargs['sql_dialect'] = 3
    dbc = fdb.connect(user=user, password=password, database=database, host=host, 
                      port=port, **kwargs)
    
    return Connection(dbc, fdb, _os.path.basename(database))

def mssql(user, password, database, host='localhost', port=1433, **kwargs):
    """Return a connection to an MS SQL Server database. Adapter pymssql is required."""
    import pymssql
    
    if not password:
        password = _passwordPrompt()
    dbc = pymssql.connect(user=user, password=password, database=database, 
                          host=r'%s:%d' % (host, int(port)), **kwargs)
    return Connection(dbc, pymssql, database)

def mysql(user, password, database='mysql', host='localhost', port=3306, **kwargs):
    """Return a connection to a MySQL database. Either MySQLdb or msql.connector are required."""
    try:
        import MySQLdb as mysql
    except ImportError:
        from mysql import connector as mysql

    if not password:
        password = _passwordPrompt()
    params = {'db': database, 'user': user, 'passwd': password, 
              'host': host, 'port': port}
    if kwargs:
        params.update(kwargs)
    dbc = mysql.connect(**params)
    return Connection(dbc, mysql, database)

def odbc(user, password, database, host='localhost', port=1433, 
         driver='SQL Server', **kwargs):
    """Return a connection to an ODBC database. Adapter pyodbc is required."""
    import pyodbc
    
    if not password:
        password = _passwordPrompt()
    dbc = pyodbc.connect('DRIVER={%s};SERVER=%s,%d;UID=%s;PWD=%s;DATABASE=%s;' %
                         (driver, host, port, user, password, database), **kwargs)
    return Connection(dbc, pyodbc, database)

def oracle(user, password, database, host=None, port=1521, is_service =False, **kwargs):
    """Return a connection to an Oracle database. Adapter cx_Oracle is required."""
    import cx_Oracle

    # If this doesn't match you'll have weird type conversion errors
    if _os.environ.get('NLS_LANG') is None:
        _os.environ['NLS_LANG'] = '.UTF8'
    _os.environ['ORA_NCHAR_LITERAL_REPLACE'] = 'TRUE'

    if host is None:
        host = database        
    if not password:
        password = _passwordPrompt()
    if is_service:
        dsn = cx_Oracle.makedsn(host, port, service_name=database)
    else:
        dsn = cx_Oracle.makedsn(host, port, database)
    dbc = cx_Oracle.connect(user, password, dsn, **kwargs)
    return Connection(dbc, cx_Oracle, database)

def postgres(user, password, database='postgres', host='localhost', port=5432, **kwargs):
    """Return a connection to a PostgreSQL database. Adapter psycopg2 is required."""
    import psycopg2
    if not password:
        password = _passwordPrompt()
    params = {'database': database, 'user': user, 'password': password, 
              'host': host, 'port': port}
    if kwargs:
        params.update(kwargs)
    
    dbc = psycopg2.connect(**params)
    return Connection(dbc, psycopg2, database)

def sqlite(database, **kwargs):
    """Return connecton to SQLite database."""
    import sqlite3
    
    dbc = sqlite3.connect(database)
    
    return Connection(dbc, sqlite3, _os.path.basename(database))