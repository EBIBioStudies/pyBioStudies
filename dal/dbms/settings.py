"""
The settings module manages the configuration db, saved connections
and query logging.

The configuration is stored in a sqlite database at ~/.config/dbms/user.sqlite.
"""

import inspect as _inspect
import os as _os
import sys as _sys
import time as _time

from . import connect as _connect

_db = None
_cursor = None
_confDir = _os.path.expanduser('~/.config/dbms')
_dbDisabled = False
_masterPassword = None

maxLogSize = 1000

def _initDB():
    """Initialize the configuration database connection."""
    global _dbDisabled, _db, _cursor
    if _dbDisabled:
        print('Local configuration and logging is disabled.')
        return False
    if not _os.path.exists(_confDir):
        try:
            _os.makedirs(_confDir)
        except:
            print('Could not create configuration directory (%s)' % _confDir)
            _dbDisabled = True
            return
    try:
        _db = _connect.sqlite('%s/user.sqlite' % _confDir)
        _cursor = _db.cursor()
    except:
        print('Failed to open configuration database.')
        _dbDisabled = True
        return

    if 'connections' not in _db.inspect.tables():
        # create schema
        _cursor.execute('''CREATE TABLE connections (
          conn_name VARCHAR(30) PRIMARY KEY,
          server_type  VARCHAR(30) NOT NULL)''')
        
        _cursor.execute('''CREATE TABLE connection_args (
           conn_name  VARCHAR(30) NOT NULL,
           arg_name   VARCHAR(30) NOT NULL,
           value VARCHAR(100),
           CONSTRAINT connection_args_pk PRIMARY KEY (conn_name, arg_name))''')
        
        _cursor.execute('''CREATE TABLE query_log (
          timestamp   DATETIME DEFAULT (datetime('now', 'localtime')) PRIMARY KEY,
          connection  VARCHAR(100),
          query       TEXT, 
          params      TEXT,
          status      VARCHAR(30))''')
        
        _db.commit()
        
    # limit query log size
    _cursor.execute('SELECT timestamp FROM query_log ORDER BY timestamp DESC LIMIT 1 OFFSET ?', (maxLogSize,))
    row = _cursor.fetchone()
    if row:
        _cursor.execute('DELETE FROM query_log WHERE timestamp <= ?', row[0])
        _db.commit()
        
    return True

class Servers(object):
    """Manage saved connections."""
    def __init__(self):
        if not _db:
            _initDB()
        self._db = _db
        self._cursor = _cursor
        
    def setMasterPassword(self, value):
        """Set MasterPassword to enable encryption/decryption of stored passwords."""
        global _masterPassword
        if not value:
            _masterPassword = None
        elif _sys.version_info[0] < 3:
            _masterPassword = bytearray(value)
        else:
            _masterPassword = value.encode('utf8', 'ignore')
        
    def _enc(self, value):
        global _masterPassword
        while not _masterPassword:
            tmp = _connect._passwordPrompt('Enter master password:\n')
            self.setMasterPassword(tmp)
    
        keyLen = len(_masterPassword)
            
        if _sys.version_info[0] < 3:
            value = bytearray(unicode(value).encode('utf8','ignore'))
            return bytes(''.join([chr(value[i] ^ _masterPassword[i % keyLen]) for i in range(len(value))]))
        else:
            value = value.encode('utf8', 'ignore')
            return ''.join([chr(value[i] ^ _masterPassword[i % keyLen]) for i in range(len(value))])
        
    def list(self):
        """List saved connections."""
        self._cursor.execute('SELECT conn_name FROM connections ORDER BY 1')
        return [rec[0] for rec in self._cursor.fetchall()]
    
    def open(self, name):
        """Open saved connection."""
        try:
            cnx = self._cursor.selectinto('SELECT * FROM connections WHERE UPPER(conn_name) = ?',
                    (name.upper(),))
        except:
            print('Connection %s was not found.' % name)
            return
        args = {}
        self._cursor.execute('SELECT arg_name, value FROM connection_args WHERE conn_name = ?',
                    (cnx['conn_name'],))
        for rec in self._cursor:
            if rec[0] == 'enc_pwd':
                args['password'] = self._enc(rec[1])
            else:
                args[rec[0]] = rec[1]
        #print('Opening %s dbms.connect.%s(%s)' % (name, cnx['server_type'], 
        #      ', '.join("%s=%s" % (k,v) for k,v in args.items())))
        connection = getattr(_connect, cnx['server_type'])(**args)
        connection.name = name
        return connection
    
    def save(self, name, serverType, **kwargs):
        """
        Save connection to config db.
            :param name: name to save connection as
            :param serverType: connection factory from dbms.connect ('oracle', 'mysql', 'mssql')
            :param kwargs: arguments to be passed to connection factory (user='dba', password='secret')
        """
        if hasattr(_connect, serverType):
            cnxFactory = getattr(_connect, serverType)
            fnArgs = _inspect.getargspec(cnxFactory)
            if 'password' not in kwargs and 'password' in fnArgs[0]:
                # add password=None so call doesn't fail
                kwargs['password'] = None
            elif 'password' in kwargs and kwargs['password']:
                kwargs['enc_pwd'] = self._enc(kwargs['password'])
                del kwargs['password']
            self._cursor.execute('INSERT INTO connections (conn_name, server_type) VALUES (?, ?)', (name, serverType))
            for key in kwargs:
                self._cursor.execute('INSERT INTO connection_args (conn_name, arg_name, value) VALUES (?, ?, ?)',
                                    (name, key, kwargs[key]))
            self._db.commit()
        else:
            raise ValueError("Invalid serverType %s. Must be a connection factory defined in dbms.connect." % serverType)     
   
    def delete(self, name):
        """Delete a saved connection."""
        self._cursor.execute('DELETE FROM connection_args WHERE UPPER(conn_name) = ?', (name.upper(),))
        self._cursor.execute('DELETE FROM connections WHERE UPPER(conn_name) = ?', (name.upper(),))
        self._db.commit()
    
class Logger(object):
    """Log queries and view query history."""
    def __init__(self):
        if not _db:
            _initDB()
        self._db = _db
        self._cursor = _cursor
        self.lastLogTime = 0
    
    def __call__(self, *args, **kwargs):
        return self.log(*args, **kwargs) 
        
    def log(self, connection, query, params=None, status=None):
        ts = _time.time()
        if ts - self.lastLogTime < 1.0:
            #Don't log more than 1 query per second
            self.lastLogTime = ts
            return
        self.lastLogTime = ts
        if not params:
            # insert NULL instead of empty tuple
            params = None
        self._cursor.execute('''INSERT INTO query_log (connection, query, params, status) 
            VALUES (?, ?, ?, ?)''', (connection, query, str(params), status))
        self._db.commit()
    
    def view(self, connection=None, limit=50, offset=0):
        """List entries in the query log."""
        query = '''SELECT timestamp, connection, query, params 
            FROM query_log 
            WHERE connection = IFNULL(?, connection)
            ORDER BY timestamp DESC LIMIT ? OFFSET ?'''
        self._cursor.execute(query, (connection, limit, offset))
        return self._cursor.fetchall()
    
    def search(self, searchTerms, connection=None):
        """Search for matches in the query log."""
        query = '''SELECT timestamp, connection, query, params
            FROM query_log
            WHERE query LIKE ?
              AND connection = IFNULL(?, connection)
            ORDER BY timestamp DESC'''
        self._cursor.execute(query, (searchTerms, connection))