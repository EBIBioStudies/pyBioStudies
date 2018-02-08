"""
Utilities to work with cursors and write queries independent of 
server implementations.

"""

import json as _json
import sys as _sys
from datetime import date as _date
from . import cursors as _cursors

_warnings = None

class Param(object):
    """Parameter to use with formatQuery()"""
    def __init__(self, value):
        self.value = value
        
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.value)
    
class NamedParam(Param):
    """Named parameter to use with formatQuery()"""
    def __init__(self, name, value):
        self.name = name
        self.value = value
        
    def __repr__(self):
        return '%s(%s, %r)' % (self.__class__.__name__, self.name, self.value)
    
def formatQuery(chunks, paramstyle):
    """
    Return formatted query and parameter list 
    
    chunks is an array of query pieces that are either text or instance of Param class.
    Usage:
    query = ('SELECT * FROM people WHERE status = ', 
            Param('A'), 
            ' AND last_name LIKE ',
            NamedParam('last_name', 'Smith%'), 
            ' ORDER BY last_name, first_name') 
    sql, params = formatQuery(query, 'named')   
    Adapted from: http://code.activestate.com/recipes/278612-single-parameters-style-for-db-api-modules/
    """
    dictParamStyles = ('named', 'pyformat')
    queryParts = []
    if paramstyle in dictParamStyles:
        params = {}
    else:
        params = []
        
    placeholders = {'named': ':%s',
                    'pyformat': '%%(%s)s',
                    'format': '%s',
                    'qmark': '?',
                    'numeric': ':%d'}
    
    if paramstyle not in placeholders:
        raise ValueError("%s not a valid paramstyle" % paramstyle)
    
    for chunk in chunks:
        if isinstance(chunk, Param):
            if paramstyle in dictParamStyles:
                if hasattr(chunk, 'name'):
                    name = chunk.name
                else:
                    name = 'p%d' % (len(params) + 1)
                params[name] = chunk.value
                queryParts.append(placeholders[paramstyle] % name)
            else:
                params.append(chunk.value)
                if paramstyle == 'numeric':
                    queryParts.append(':%d' % len(params))
                else:
                    queryParts.append(placeholders[paramstyle])
        else:
            queryParts.append(chunk)
            
    if isinstance(params, list):
        params = tuple(params)
    return (' '.join(queryParts), params)                 

def _toString(obj):
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
    elif isinstance(obj, str):
        ret = obj
    elif _sys.version_info[0] < 3 and isinstance(obj, unicode):
        ret = obj
    else:
        ret = str(obj)
    return ret

def _dumps(obj):
    """Convert to string with proper quoting for SQL statement."""
    ret = ''
    if obj is None:
        ret = 'NULL'
    elif isinstance(obj, (int, float)):
        ret = obj
    elif isinstance(obj, _date):
        if hasattr(obj, 'microsecond') and obj.microsecond:
            ret = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SSFF3')" % _date.strftime(obj, '%Y-%m-%d %H:%M:%S.%f')
        elif hasattr(obj, 'hour') and (obj.hour or obj.minute or obj.second):
            # some db's have date types with h:m:s other have datetime types
            ret = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % _date.strftime(obj, '%Y-%m-%d %H:%M:%S')
        else:
            ret = "TO_DATE('%s', 'YYYY-MM-DD')" %  _date.strftime(obj, '%Y-%m-%d')
    else:
        ret = "'%s'" % _toString(obj).replace("'", "''")
    return ret

def _fixedWidth(value, width):
    """Convert value to string and trim or pad as needed"""
    global _warnings
    ret = _toString(value)
    if len(ret) > width:
        if not _warnings:
            _warnings = 'WARNING - Some values were truncated during export.'
        return ret[0:width]
    return ret.ljust(width)

class _JSONEncoder(_json.JSONEncoder):
    """ JSONEncoder that handles dates."""
    def default(self, obj):
        if isinstance(obj, _date):
            encoded_obj = _toString(obj)
        elif isinstance(obj, _cursors.Record):
            encoded_obj = super(_JSONEncoder, self).default(obj.copy())
        elif hasattr(obj, '_asdict'):
            encoded_obj = super(_JSONEncoder, self).default(obj._asdict())
        else:
            encoded_obj = super(_JSONEncoder, self).default(obj)
        return encoded_obj

def _splitData(data):
    """Takes either a cursor or result set and returns result set and list of columns."""
    if hasattr(data, 'fetchall'):
        rows = data.fetchall()
        cols = data.columns()
    elif isinstance(data, list):
        rows = data
        if hasattr(rows[0], '_fields'):
            cols = rows[0]._fields
        elif hasattr(rows[0], 'keys'):
            cols = list(rows[0].keys())
        else:
            raise TypeError('Can not determine the list of columns from the result set.')
    return (rows, cols)

def createInsert(cur, table, paramstyle=None):
    """Create Insert Statement"""
    if not paramstyle:
        paramstyle = cur.connection.interface.paramstyle  
    columns = cur.connection.inspect.columns(table)
    
    if paramstyle == 'qmark':
        params = ', '.join(['?' for c in columns])
    elif paramstyle == 'numeric':
        params = ', '.join([':%d' % (i+1) for i in range(len(columns))])
    elif paramstyle == 'format':
        params = ', '.join(['%s' for c in columns])
    elif paramstyle == 'named':
        params = ', '.join([':%s' % c.lower().strip('"') for c in columns])
    elif paramstyle == 'pyformat':
        params = ', '.join(['%%(%s)s' % c.lower().strip('"') for c in columns])
    
    query = 'INSERT INTO %s (%s) VALUES (%s)' % (cur.connection.inspect.formatName(table),
                                                ', '.join(columns),
                                                params)
    return query

    
def cursorToCursorCopy(data, destCursor, destTable):
    """
    Copy record set from source to destination cursor. 
    This can be used to copy data across different servers. However, their
    data types need to be similar.
    
    This does not perform constraint checks and will fail if you try to 
    insert a record that already exists on the destination table. 
    """
    query = createInsert(destCursor, destTable)
    inserts = 0
    blockSize = 100
    
    rows, cols = _splitData(data)
    
    numRows = len(rows)
    
    for k in range(numRows // blockSize):
        startIdx = blockSize * k
        endIdx = min((numRows, blockSize * k - 1))
        destCursor.execute(query, rows[startIdx:endIdx])
    
    while rows:
        destCursor.executemany(query, rows)
        inserts += len(rows) 
        rows = data.fetchmany()
    print('Inserted %d records into %s' % (inserts, destTable))
    
def cursorToSQLInserts(cursor, tableName, filename=None):
    """Export cursor to Insert statements."""
    if filename:
        fp = open(filename, 'ab')
    else:
        fp = _sys.stdout
    
    query = createInsert(cursor, tableName, paramstyle='pyformat') + ';\n'
    for row in cursor:
        vals = dict([(k.lower(), _dumps(v)) for k, v in row.items()])
        line = query % vals
        fp.write(line.encode('utf8'))
    if filename:
        fp.close()
    
def cursorToCSV(data, filename):
    """Exports cursor to CSV file"""
    import csv
    if _sys.version_info >= (3,0,0):
        fp = open(filename, 'w', newline='')
    else:
        fp = open(filename, 'wb')
    dest = csv.writer(fp)
        
    rows, cols = _splitData(data)
    dest.writerow([col.upper() for col in  cols])    
    
    for rec in rows:
        dest.writerow([_toString(rec[i]) for i in range(len(cols))])
    fp.close()

def cursorToFixedWidth(data, filename=None, colWidths=()):
    """
    Exports cursor or result set to fixed width text.
    If colWidths are passed in values will be truncated if they exceed that.
    If colWidths are not passed, they will be calculated based on the first
    50 rows and values will not be truncated.
    """
    global _warnings
    if filename:
        # print to file if filename given
        fp = open(filename, 'w')
    else:
        # else print to stdout
        fp = _sys.stdout
        
    rows, cols = _splitData(data)

    if not colWidths:
        # column widths were not passed in. Calculate based on the first 20 rows    
        colWidths = []
        trimToWidth = False
        
        for col in cols:
            colWidths.append(len(col))
            
        #sample column widths of first 50 records
        for j in range(50):
            if j < len(rows):
                row = rows[j]
                for i in range(len(cols)):
                    strLen = len(_toString(row[i]))
                    if strLen > colWidths[i]:
                        colWidths[i] = strLen
            else:
                break
        # pad a bit
        for i in range(len(cols)):
            colWidths[i] = 2 + int(colWidths[i] * 1.1)
    
        #write column headers
        fp.write('%s\n' % ''.join([cols[i].upper().ljust(colWidths[i]) for i in range(len(cols))]))
    else:
        # column widths were passed in 
        if len(colWidths) != len(cols):
            raise Exception('Length of colWidths parameter must match number of columns.')
        trimToWidth = True

    for row in rows:
        if trimToWidth:
            fp.write('%s\n' % ''.join([_fixedWidth(row[i], colWidths[i]) for i in range(len(cols))]))
        else:
            fp.write('%s\n' % ''.join([_toString(row[i]).ljust(colWidths[i]) for i in range(len(cols))]))
        
    if filename:
        fp.close()
        
    if _warnings:
        print(_warnings)
        _warnings = None

def cursorToXLSX(data, filename, exportQuery=True):
    """Exports cursor or result set to Excel file. Requires xlswriter."""
    import xlsxwriter
    wb = xlsxwriter.Workbook(filename)
    ws = wb.add_worksheet('Data')
    bold_fmt = wb.add_format({'bold': True})
    date_fmt = wb.add_format({'num_format': 'yyyy-mm-dd'})
    
    rows, cols = _splitData(data)
    
    ws.write_row(0, 0, [col.upper() for col in  cols], bold_fmt)
    row = 1
    for rec in rows:
        for col in range(len(cols)):
            ws.write(row, col, rec[col])
        row += 1
        
    # calculate column widths    
    for i in range(len(rec)):
        val_len = len(str(rec[i]))
        if isinstance(rec[i], _date):
            ws.set_column(i, i, 15, date_fmt)
        elif val_len > 20:
            ws.set_column(i, i, round(val_len * 1.25))
        else:
            ws.set_column(i, i, 12)
            
    if exportQuery and hasattr(data, 'showStatement'):
        # export query to a second worksheet
        wrap = wb.add_format()
        wrap.set_text_wrap(1)
        wrap.set_valign('top')
        qws = wb.add_worksheet('Query')
        qws.write(0, 0, data.showStatement(), wrap)
        qws.set_column(0, 0, 80)
        qws.set_row(0, 300)

    wb.close()
        
def cursorToXML(data, filename, prettyprint=True, exportQuery=True):
    """Exports cursor or result set to XML file"""
    try:
        from lxml import etree as et
    except ImportError:
        import xml.etree.ElementTree as et
    
    fp = open(filename, 'wb') 
    root = et.Element("root")
        
    rows, cols = _splitData(data)
    
    if exportQuery and hasattr(data, 'showStatement'):
        root.append(et.Comment(data.showStatement()))
    
    if len(rows) == 0:
        return

    rn = 0
    for rec in rows:
        rn += 1
        element = et.SubElement(root, 'row', {'num': str(rn)})
        for i in range(len(cols)):
            et.SubElement(element, cols[i]).text = _toString(rec[i])
                
    fp.write(et.tostring(root, pretty_print = prettyprint))
    fp.close()
 
def cursorToJSON(data, filename=None):
    """Export cursor or result set to JSON encoded string."""
    rows, cols = _splitData(data)
    
    if len(rows) == 0:
        return
    
    if filename:
        # print to file if filename given
        fp = open(filename, 'w')
    else:
        # else print to stdout
        fp = _sys.stdout
        
    fp.write('[')
    
    recType = rows[0].__class__.__name__
    # Must be converted to dict in order to encode properly
    if recType == 'Record':
        toDict = lambda r: r.copy()
    elif recType == 'RecordNT':
        toDict = lambda r: r._asdict()
    elif recType == 'RecordOD':
        toDict = lambda r: r
    else:
        toDict = lambda r: dict(zip(cols, r))
        
    for rec in rows:
        fp.write('\n%s,' % _json.dumps(toDict(rec), cls=_JSONEncoder))
    fp.write('\n]')
    
    if filename:
        fp.close()

def cursorToDataFrame(data):
    """Export cursor or result set to Pandas DataFrame."""
    from pandas import DataFrame
    
    rows, cols = _splitData(data)
        
    if len(rows) == 0:
        return
    
    recType = rows[0].__class__.__name__
    
    if recType == 'RecordNT':
        df = DataFrame.from_records(rows, columns=cols)
    elif recType == 'Record':
        df = DataFrame.from_records([row.copy() for row in rows], columns=cols)
    elif recType == 'RecordOD':
        df = DataFrame.from_dict(rows, columns=cols)
    else:
        df = DataFrame(rows, columns=cols)
    return df
