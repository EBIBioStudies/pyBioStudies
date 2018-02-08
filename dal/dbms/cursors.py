"""
  Defines four cursor types that only differ in the data type of the record set returned.
  They all wrap and delegate to the adapter's actual cursor stored in the _cursor attribute.
  
  The default cursor is DictCursor because it offers the most flexibility.
"""

import re
from datetime import date as _date
from keyword import iskeyword
from collections import namedtuple as _namedtuple

try:
    from collections import OrderedDict as _OrderedDict
except ImportError:
    try:
        # for Python 2.6  `pip install ordereddict`
        from ordereddict import OrderedDict as _OrderedDict
    except ImportError:
        """OrderedDict is not available in this verson of Python. 
        Continue to run w/o throwing an exception unless we actually
        try to instantiate an OrderedDictCursor.
        """
        _OrderedDict = object

class Cursor(object):
    """Basic cursor object that returns results as list.
    Designed as the base class for the dictionary type cursors.
    But can also be used like a standard cursor.
    """

    UPPER_CASE = 'upper'
    LOWER_CASE = 'lower'
    PRESERVE_CASE = 'preserve'
    
    _localAttrs = ['bindvars', 'connection', 'columnCase', 'debug', 'logger', 
        'placeholder', 'paramstyle', 'record', '_cursor', '_validateRowFactory']

    def __init__(self, *args, **kwargs):
        self.connection = args[0]
        self.debug = False
        self.record = None
        self._validateRowFactory = True
        self.logger = None
                       
        if 'columnCase' in kwargs:
            self.columnCase = kwargs['columnCase']
            del kwargs['columnCase']
        else:
            self.columnCase = Cursor.LOWER_CASE
            
        if 'debug' in kwargs:
            self.debug = kwargs['debug']
            del kwargs['debug']
            
        if 'logger' in kwargs:
            self.logger = kwargs['logger']
            del kwargs['logger']
            
        try:
            if hasattr(self.connection, '_connection'):
                self._cursor = self.connection._connection.cursor(**kwargs)
            else:
                self._cursor = self.connection.cursor(**kwargs)
        except:
            raise TypeError('First argument must be a database connection object.')
        
        self.paramstyle = self.connection.interface.paramstyle
        if hasattr(self.connection, 'placeholder'):
            self.placeholder = self.connection.placeholder
        
        if not hasattr(self._cursor, 'arraysize'):
            #not all adapters have this even though it is required by the spec
            self.__dict__['arraysize'] = 1
        

    def __getattr__(self, key):
        if key in Cursor._localAttrs:
            return None
        else:
            return getattr(self._cursor, key)

    def __setattr__(self, key, value):
        if key in Cursor._localAttrs:
            self.__dict__[key] = value
        else:
            return setattr(self._cursor, key, value)

    def __dir__(self):
        return list(set(dir(self._cursor)  + dir(Cursor) + Cursor._localAttrs))

    def __iter__(self):
        if self._isReady():
            return self

    def next(self):
        """Iterate over result set"""
        while True:
            row = self.fetchone()
            if row:
                return row
            else:
                raise StopIteration
            
    def __next__(self):
        return self.next()  
            
    def _rowFactory(self):
        """Initializes the function that will be used to process each record from the result."""
        def rec(*args):
            return list(args)
        self.record = rec

    def _sanitize(self, colName, idx):
        """Clean up any illegal column names"""
        if colName is None or colName == '':
            # if we didn't get a column name just name it column_X
            return 'column_%d' % (idx + 1)

        # replace any non-word characters with underscore
        colName = re.sub(r'\W+', '_', colName)

        # uppercase if colName is Python keyword
        if iskeyword(colName):
            colName = colName.upper()
        return colName

    def columns(self, case=None):
        """Return list of column names."""
        if self.description:
            if case not in ('upper', 'lower'):
                case = self.columnCase
            if case == Cursor.LOWER_CASE:
                cols = [c[0].lower() for c in self.description]
            elif case == Cursor.UPPER_CASE:
                cols = [c[0].upper() for c in self.description]
            else:
                cols = [c[0] for c in self.description]

            return [self._sanitize(cols[i], i) for i in range(len(cols))]

    def _isReady(self):
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        if self.record is None:
            self._rowFactory()
        return True

    def _debugInfo(self, query, bindvars=()):
        print ('Query:\n%s' % query)
        if bindvars:
            print ('Bind vars:\n%s' % bindvars)

    def execute(self, query, bindvars=()):
        """Prepare and execute a database operation
        For help on parameters see your connection object's showParmHelp()
        """
        self._validateRowFactory = True
        
        if self.debug:
            self._debugInfo(query, bindvars)
        
        if not hasattr(self._cursor, 'statement'):
            # interface does not store last statement
            self.__dict__['statement'] = query
            
        if not hasattr(self._cursor, 'bindvars'):
            self.__dict__['bindvars'] = bindvars
        
        if self.logger:
            self.logger(self.connection.__name__, query, bindvars)
        
        ret = self._cursor.execute(query, bindvars)

    def executemany(self, query, bindvars):
        """Prepare a database operation and execute against all sequences"""
        self._validateRowFactory = True
        
        if self.debug:
            self._debugInfo(query, bindvars)
        
        if not hasattr(self._cursor, 'statement'):
            self.__dict__['statement'] = '--executemany\n%s' % query
        if not hasattr(self._cursor, 'bindvars'):
            self.__dict__['bindvars'] = bindvars[0]
            
        if self.logger:
            self.logger(self.connection.__name__, query, bindvars[0])
        self._cursor.executemany(query, bindvars)

    def selectinto(self, query, bindvars=()):
        """Run query and return the result. Query must return one and only one row."""
        self.execute(query, bindvars)
        rows = self.fetchmany(2)
        if len(rows) == 0:
            raise self.connection.interface.DatabaseError('No Data Found.')
        elif len(rows) > 1:
            raise self.connection.interface.DatabaseError('selectinto() must return one and only one row.')
        else:
            return rows[0]

    def fetchone(self):
        """Fetch the next row of a query result, returning a single sequence, or None if no more data."""
        if self._isReady():
            row = self._cursor.fetchone()
            if row:
                return self.record(*row)

    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a sequence of rows.
        An empty sequence is returned when no more rows are available.

        :param size: The number of rows fetched. If it is not given, the
        cursor's arraysize attribute is used.
        """
        if size is None:
            size = self._cursor.arraysize
            
        if self._isReady():
            return [self.record(*row) for row in self._cursor.fetchmany(size)]

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of rows.
        Note the cursor's arraysize attribute can affect the performance of this operation."""
        if self._isReady():
            return [self.record(*row) for row in self._cursor.fetchall()]
        
    def showStatement(self):
        """Output last statement and bind variables"""
        if hasattr(self, 'statement'):
            statement = self.statement
        elif hasattr(self, 'query'):
            statement = self.query
        else:
            statement = ''
        if self.bindvars:
            statement += '\nParams:\n%s' % repr(self.bindvars) 
                
        return statement

class DictCursor(Cursor):
    """Cursor that returns records as Record objects. 
       This is the most versatile cursor."""
    
    def _isReady(self):  
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        elif self.record is None:
            self._rowFactory()
        elif self._validateRowFactory:
            #another query has been run since we created the record type
            if self.record._fields != self.columns():
                # columns have changed
                self._rowFactory()
            else:
                # columns are the same as last query
                self._validateRowFactory = False
        return True
    
    def _rowFactory(self):
        # Create a subclass of Record to hold returned rows.
        self._validateRowFactory = False
        self.record = type('Record',
                        (Record,), {'_fields': self.columns()})
        
class NamedTupleCursor(Cursor):
    """Cursor that returns records as namedtuples."""    
    def _isReady(self):
        if self._cursor.description is None:
            raise Exception('Query has not been run or did not succeed.')
        elif self.record is None:
            self._rowFactory()
        elif self._validateRowFactory:
            #another query has been run since we created the record type
            if self.record._fields != self.columns():
                # columns have changed
                self._rowFactory()
            else:
                # columns are the same as last query
                self._validateRowFactory = False
        return True
    
    def _rowFactory(self):
        self._validateRowFactory = False
        self.record = _namedtuple('RecordNT', self.columns())

class OrderedDictCursor(Cursor):
    """Cursor that returns records OrderedDict. 
    Not as efficient or functional as DictCursor or NamedTupleCursor."""
    
    def __init__(self, *args, **kwargs):
        if _OrderedDict.__name__ == 'object':
            raise ImportError('OrderedDict is not available. You can install it with `pip install ordereddict`.')
        Cursor.__init__(self, *args, **kwargs)
    
    def _rowFactory(self):
        def rec(*args):
            return RecordOD(zip(self.columns(), args))
        self.record = rec

#------------------------------------------------------------------------------#
#     Types to return database results into.  
#------------------------------------------------------------------------------#

class Record(list):
    """Row object is a memory optimized object that allows access by:
       column name   row['column_name']
       attributes    row.column_name
       column index  row[3]
       slicing       row[1:4]

       Record will be dynamically subclassed as dbms.cursors.Record each
       time a DictCursor is executed.
    """

    __slots__ = ()
    _fields = []
    def __init__(self, *args):
        super(Record, self).__init__()
        self[:] = args

    @classmethod
    def setColumns(cls, *args):
        if isinstance(args[0], (list, tuple)):
            cls._fields = args[0]
        else:
            cls._fields = args

    def __getattr__(self, attr):
        if attr in self._fields:
            x = self._fields.index(attr)
            return self[x]

    def __getitem__(self, item):
        if not isinstance(item, (int, slice)):
            item = self._fields.index(item)
        return list.__getitem__(self, item)

    def __setitem__(self, item, val):
        if not isinstance(item, (int, slice)):
            item = self._fields.index(item)
        list.__setitem__(self, item, val)

    def __dir__(self):
        return list(self._fields) + ['copy', 'get', 'items', 'keys', 'values', 'pprint']

    def __str__(self):
        return self.__class__.__name__ + '{' \
            + ', '.join(["'%s': %r" % (k, v) for k, v in self.items()]) + '}'

    def __repr__(self):
        return self.__class__.__name__ + '(' \
            + ', '.join("%r" % v for v in self.values()) + ')'

    def items(self):
        """Record.items() -> list of Record's (key, value) pairs as 2-tuples"""
        return list(self.iteritems())

    def keys(self):
        """Record.keys() -> list of Record's keys"""
        return self._fields

    def values(self):
        """Record.values() -> list of Record's values"""
        return tuple(self[:])

    def has_key(self, key):
        """Record.has_key(k) -> True if Record has key k, else False"""
        return key in self._fields

    def get(self, key, default=None):
        """Record.get(k[,d]) -> Record[k] if k in Record, else d"""
        try:
            return self[key]
        except ValueError:
            return default

    def iteritems(self):
        """Record.iteritems() -> iterator over (key, value) items of Record"""
        for i in range(len(self)):
            yield self._fields[i], list.__getitem__(self, i)

    def iterkeys(self):
        """Record.iterkeys() -> iterator over keys of Record"""
        for key in self._fields:
            yield key

    def itervalues(self):
        """Record.itervalues() -> iterator over values of Record"""
        return list.__iter__(self)

    def copy(self):
        """Record.copy() -> a dict representation of Record"""
        return dict(self.iteritems())
    
    def pprint(self):
        """Pretty Print record."""
        colWidth = max([len(f) for f in self._fields])
        template = "{0:<%d} : {1}" % colWidth
        for item in self.items():
            print(template.format(item[0], _toString(item[1])))
        
class RecordOD(_OrderedDict):
    """OrderedDict that allows value to be fetched by index."""
    def __getitem__(self, item):
        try:
            value = super(RecordOD, self).__getitem__(item)
        except KeyError:
            keys = list(self.keys())
            if isinstance(item, int) and item < len(keys):
                value = super(RecordOD, self).__getitem__(keys[item])
            else:
                raise KeyError
        return value
    
    def pprint(self):
        colWidth = max([len(f) for f in self.keys()])
        template = "{:<%d} : {}" % colWidth
        for item in self.items():
            print(template.format(item[0], _toString(item[1])))

def _toString(obj):
    """Convert a record value to string"""
    ret = ''
    if obj is None:
        ret = '(NULL)'
    elif isinstance(obj, _date):
        # datetime.datetime is subclassed from date
        if hasattr(obj, 'microsecond') and obj.microsecond:
            ret = _date.strftime(obj, '%Y-%m-%d %H:%M:%S.%f %Z')
        elif hasattr(obj, 'hour') and (obj.hour or obj.minute or obj.second):
            ret = _date.strftime(obj, '%Y-%m-%d %H:%M:%S')
        else:
            ret = _date.strftime(obj, '%Y-%m-%d')
    else:
        ret = str(obj)
    return ret