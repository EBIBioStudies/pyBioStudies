"""
DBMS - DataBases Made Simpler is a database tool kit that wraps and provides
uniform interface to other DB API compliant adapters. 

It manages connections, doing automatic imports and providing a
uniform connection method to most common databases.
Implements DictCursor, NamedTupleCursor and OrderedDictCursor.

It currently supports IBM DB2, Firebird, MSSQL Server, MySQL, Oracle,
PostgreSQL, SQLite and ODBC connections.
"""
__version__ = '1.1.1'
__author__ = 'Scott Bailey <scottrbailey@gmail.com>'

from . import connect
from .settings import Logger, Servers
from .utils import Param, NamedParam

servers = Servers()
"""
dbms.servers manages stored connections
    dbms.servers.list()
    dbms.servers.open(name)
    dbms.servers.save(name, serverType, **kwargs)
    dbms.servers.delete(name)
"""

logger = Logger()
"""
dbms.logger manages the query log
    dbms.logger.view([connection, limit, offset])
    dbms.logger.search(searchTerms, [connection])
    dbms.logger.log(connection, query, [params, status])
"""