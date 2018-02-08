"""
Probe queries the database to get detailed information about its structure.

If the server type can be determined, the Connection object instantiates Probe,
or one of its subclasses, in the Connection.probe attribute.
"""

from .cursors import Record as _Record

class Probe(object):
    """
    Probe database schemata.

    There is no universal way to probe a database's structure. But Probe will work 
    with databases that provide information_schema views. Other database must 
    subclass Probe and return the same column names.
    """
    hasSchema = True
    def __init__(self, connection):
        if hasattr(connection, 'cursor') and callable(getattr(connection, 'cursor')):
            self._server = connection._server
            self._cursor = connection.cursor(None, logger=None)
            self._dbname = connection.dbname
        else:
            raise TypeError('First argument must be a dbms.Connection')
        
    def getServerVersion(self):
        """Get version of database engine"""
        query = 'SELECT version() AS version'
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        """Return list of databases."""
        raise NotImplementedError('No cross DBMS way to list databases.')
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_current, sys_ind)]."""
        query = '''SELECT LOWER(ss.schema_name) AS name, 
          CASE WHEN schema_name = %s THEN 1 ELSE 0 END is_current,
          CASE WHEN schema_name = 'information_schema' THEN 1
            WHEN lower(substring(schema_name, 1, 3)) IN ('pg_', 'db_') THEN 2
            WHEN schema_name IN ('mysql', 'sys', 'dbo', 'guest') THEN 2
            ELSE 0 END sys_ind
        FROM information_schema.schemata ss 
        ORDER BY is_current DESC, sys_ind, ss.schema_name''' % (
            self._cursor.placeholder)
        self._cursor.execute(query, (self._dbname,))
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT table_name AS name, table_schema AS schema_name, 
          CASE WHEN table_schema IN ('information_schema', 'pg_catalog') 
          THEN 1 ELSE 0 END AS sys_ind, NULL AS description
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema = COALESCE(LOWER(%s), t.table_schema)
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT table_name  AS name, table_schema AS schema_name,
        CASE WHEN table_schema IN ('information_schema', 'pg_catalog')
        THEN 1 ELSE 0 END sys_ind, NULL AS description
        FROM information_schema.views v
        WHERE v.table_schema = COALESCE(lower(%s), v.table_schema)
        ORDER BY 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getFunctions(self, schema=None):
        """Return list of functions. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT DISTINCT r.routine_name AS name,
          r.routine_schema             AS schema_name,
          CASE WHEN r.routine_schema IN ('pg_catalog') 
            THEN 1 ELSE 0 END   sys_ind
        FROM information_schema.routines r
        WHERE r.routine_type = 'FUNCTION'
          AND r.routine_schema = COALESCE(%s, r.routine_schema)
          AND substring(routine_name, 1, 1) != '_'
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
        
    def getProcedures(self, schema=None):
        """Return list of procedures. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT DISTINCT r.routine_name AS name,
          r.routine_schema                    AS schema_name,
          CASE WHEN r.routine_schema IN ('pg_catalog') 
            THEN 1 ELSE 0 END   sys_ind
        FROM information_schema.routines r
        WHERE r.routine_type = 'PROCEDURE'
          AND substring(routine_name, 1, 1) != '_'
          AND r.routine_schema = COALESCE(%s, r.routine_schema)
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk, description)]"""
        query = '''SELECT c.column_name AS name, c.data_type, 
          substring(c.is_nullable, 1, 1) is_nullable,
          CASE WHEN kc.column_name IS NOT NULL THEN 'Y' END is_pk,
          NULL AS description
        FROM information_schema.columns c
        LEFT JOIN information_schema.table_constraints tc 
          ON c.table_schema = tc.table_schema
          AND c.table_name = tc.table_name
          AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage kc
          ON c.table_schema = kc.table_schema
          AND c.table_name = kc.table_name
          AND tc.constraint_name = kc.constraint_name
          AND c.column_name = kc.column_name
        WHERE LOWER(c.table_name) = LOWER(%s)
          AND LOWER(c.table_schema) = COALESCE(LOWER(%s), c.table_schema)
        ORDER BY c.ordinal_position''' % (self._cursor.placeholder, 
                                          self._cursor.placeholder)
        self._cursor.execute(query, (table, schema))
        return self._cursor.fetchall()

class ProbeOracle(Probe):
    def getServerVersion(self):
        """Get version of database engine."""
        query = 'SELECT banner FROM v$version WHERE rownum = 1'
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        print('Oracle has only one database per instance')
        return []
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_connected_db, sys_ind)]."""
        # just return schema that have objects
        query = '''SELECT username name, NULL AS is_connected_db,
          CASE WHEN username LIKE '%SYS%' THEN 1 ELSE 0 END sys_ind
        FROM all_users u
        WHERE EXISTS (SELECT 1 FROM all_objects o WHERE u.username = o.owner)
        ORDER BY 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, num_rows, description)]"""
        query = '''SELECT t.table_name AS name, t.owner schema_name, 
          CASE WHEN t.owner LIKE 'SYS%' THEN 1 ELSE 0 END AS sys_ind,
          num_rows, tc.comments description
        FROM all_tables t 
        LEFT JOIN all_tab_comments tc ON t.owner = tc.owner
          AND t.table_name = tc.table_name
        WHERE t.OWNER = COALESCE(UPPER(:0), t.owner)
        ORDER BY 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT v.view_name AS name, v.owner schema_name,
        CASE WHEN v.owner LIKE 'SYS%' THEN 1 ELSE 0 END AS sys_ind,
          vc.comments description
        FROM all_views v 
        LEFT JOIN all_tab_comments vc ON v.OWNER = vc.OWNER
          AND v.VIEW_NAME = vc.TABLE_NAME
        WHERE v.owner = COALESCE(UPPER(:0), v.owner)
        ORDER BY 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getFunctions(self, schema=None):
        """Return list of functions. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT p.object_name name,
          p.owner                   schema_name,
          CASE WHEN owner LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind
        FROM all_procedures p
        WHERE p.object_type = 'FUNCTION'
          AND p.owner = COALESCE(UPPER(:0), p.owner)
          AND COALESCE(p.overload, '1') = '1'
        ORDER BY 3, 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getProcedures(self, schema=None):
        """Return list of procedures. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT p.object_name name,
          p.owner                   schema_name,
          CASE WHEN owner LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind
        FROM all_procedures p
        WHERE p.object_type = 'PROCEDURE'
          AND p.owner = COALESCE(UPPER(:0), p.owner)
          AND COALESCE(p.overload, '1') = '1'
        ORDER BY 3, 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getPackages(self, schema=None):
        """Return list of packages. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT p.object_name name,
          p.owner                   schema_name,
          CASE WHEN owner LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind
        FROM all_procedures p
        WHERE p.object_type = 'PACKAGE'
          AND p.owner = COALESCE(UPPER(:0), p.owner)
          AND p.subprogram_id = 0
        ORDER BY 3, 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getPackageProcedures(self, package, schema=None):
        """Return list of functions/procedures in a package. [Record(name, package_name, schema_name)]"""
        query = '''SELECT p.procedure_name name,
          p.object_name                package_name,
          p.owner                      schema_name
        FROM all_procedures p
        WHERE p.object_type = 'PACKAGE'
          AND p.object_name = UPPER(:0)
          AND p.owner = COALESCE(UPPER(:1), p.owner)
          AND p.subprogram_id > 0
          AND COALESCE(p.overload, '1') = '1'
        ORDER BY p.subprogram_id'''
        self._cursor.execute(query, (package, schema))
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk, description)]"""
        query = '''SELECT c.column_name name, c.data_type, c.nullable is_nullable,
          CASE WHEN cc.column_name IS NOT NULL THEN 'Y' END is_pk,
          cmt.comments description
        FROM all_tab_columns c
        LEFT JOIN all_col_comments cmt ON c.owner = cmt.owner
          AND c.table_name = cmt.table_name
          AND c.column_name = cmt.column_name
        LEFT JOIN all_constraints tc ON c.owner = tc.owner
          AND c.table_name = tc.table_name
          AND tc.constraint_type = 'P'
        LEFT JOIN all_cons_columns cc ON tc.owner = cc.owner
          AND tc.table_name = cc.table_name 
          AND tc.constraint_name = cc.constraint_name 
          AND c.column_name = cc.column_name
        WHERE c.table_name = UPPER(:1)
          AND c.owner = COALESCE(UPPER(:2), c.owner)
        ORDER BY c.column_id'''
        self._cursor.execute(query, (table, schema))
        return self._cursor.fetchall()

class ProbeSQLite(Probe):
    hasSchema = False
    def getServerVersion(self):
        """Get version of database engine"""
        query = '''select 'SQLite ' ||  sqlite_version() AS version'''
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        """Return list of databases."""
        self._cursor.execute('PRAGMA database_list')
        return [(rec['name'], rec['file']) for rec in self._cursor]
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_connected_db, sys_ind)]."""
        print('SQLite does not have schema.')
        return []
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT name, NULL AS schema_name,
        CASE WHEN name LIKE 'sqlite$_' escape '$' 
          THEN 1 ELSE 0 END AS sys_ind 
        FROM sqlite_master WHERE type = 'table' 
        ORDER BY 1 '''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT name, NULL AS schema_name,
        CASE WHEN name LIKE 'sqlite$_' escape '$' 
          THEN 1 ELSE 0 END AS sys_ind 
        FROM sqlite_master WHERE type = 'view' 
        ORDER BY 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getFunctions(self, schema=None):
        """Return list of functions. [Record(name, schema_name, sys_ind)]"""
        print('SQLite does not support user defined functions.')
        return []
    
    def getProcedures(self, schema=None):
        """Return list of procedures. [Record(name, schema_name, sys_ind)]"""
        print('SQLite does not support user defined procedures')
        return []
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk)]"""
        query = 'PRAGMA table_info(\'%s\')' % table
        self._cursor.execute(query)
        rowType = type('Record', (_Record,),
                   {'_fields': ['name', 'data_type', 'is_nullable', 'is_pk']})

        return [rowType(rec['name'], rec['type'], 'YN'[bool(rec['notnull'])], 'NY'[bool(rec['pk'])])
                for rec in self._cursor]
    
class ProbePostgres(Probe):
    def getDatabases(self):
        """Return list of databases/catalogs."""
        query = '''SELECT datname AS name FROM pg_database 
        WHERE datistemplate=false ORDER BY 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_current, sys_ind)]."""
        query = '''SELECT nspname AS name, 
          CASE WHEN nspname = current_schema() THEN 1 ELSE 0 END AS is_current,
          CASE WHEN nspname LIKE 'pg_%' OR nspname = 'information_schema' THEN 1
          ELSE 0 END sys_ind
        FROM pg_catalog.pg_namespace
        ORDER BY 3, 1'''
        self._cursor.execute(query, None)
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT table_name AS name, table_schema AS schema_name, 
          CASE WHEN table_schema IN ('information_schema', 'pg_catalog') 
          THEN 1 ELSE 0 END AS sys_ind,
          obj_description((table_schema ||'.'|| table_name)::regclass, 'pg_class') AS description
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema = COALESCE(LOWER(%s), t.table_schema)
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT table_name  AS name, table_schema AS schema_name,
        CASE WHEN table_schema IN ('information_schema', 'pg_catalog')
        THEN 1 ELSE 0 END sys_ind,
        obj_description((table_schema ||'.'|| table_name)::regclass, 'pg_class') AS description
        FROM information_schema.views v
        WHERE v.table_schema = COALESCE(lower(%s), v.table_schema)
        ORDER BY 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk)]"""
        query = '''SELECT c.column_name AS name, c.data_type, 
          substring(c.is_nullable, 1, 1) is_nullable,
          CASE WHEN kc.column_name IS NOT NULL THEN 'Y' END is_pk
        FROM information_schema.columns c
        LEFT JOIN information_schema.table_constraints tc 
          ON c.table_schema = tc.table_schema
          AND c.table_name = tc.table_name
          AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage kc
          ON c.table_schema = kc.table_schema
          AND c.table_name = kc.table_name
          AND tc.constraint_name = kc.constraint_name
          AND c.column_name = kc.column_name
        WHERE LOWER(c.table_name) = LOWER(%s)
          AND LOWER(c.table_schema) = COALESCE(LOWER(%s), c.table_schema)
        ORDER BY c.ordinal_position''' % (self._cursor.placeholder, 
                                          self._cursor.placeholder)
        self._cursor.execute(query, (table, schema))
        return self._cursor.fetchall()
    
class ProbeMySQL(Probe):   
    def getDatabases(self):
        """Return list of databases/catalogs."""
        query = '''SELECT ss.schema_name AS name 
        FROM information_schema.schemata ss ORDER BY 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_connected_db, sys_ind)]."""
        # databases == schemas on mysql
        return self.getDatabases()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, num_rows, description)]"""
        if schema is None:
            schema = self._dbname
        query = '''SELECT table_name AS name, table_schema AS schema_name, 
          CASE WHEN table_schema IN ('information_schema', 'pg_catalog') 
          THEN 1 ELSE 0 END AS sys_ind, t.table_rows AS num_rows,
          t.table_comment AS description
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema = COALESCE(LOWER(%s), t.table_schema)
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk, description)]"""
        if schema is None:
            schema = self._dbname        
        query = '''SELECT c.COLUMN_NAME AS name, c.data_type,
          substring(c.is_nullable, 1, 1) is_nullable,
          CASE WHEN c.column_key = 'PRI' THEN 'Y' END is_pk,
          c.column_comment AS description
        FROM information_schema.columns c
        WHERE LOWER(c.table_name) = LOWER(%s)
          AND LOWER(c.table_schema) = COALESCE(LOWER(%s), c.table_schema)
        ORDER BY c.ordinal_position''' % (self._cursor.placeholder, 
                                          self._cursor.placeholder)
        self._cursor.execute(query, (table, schema))
        return self._cursor.fetchall()
       
class ProbeMSSQL(Probe):
    def getServerVersion(self):
        """Get version of database engine."""
        query = 'SELECT @@version'
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        """Return list of databases/catalogs."""
        query = '''SELECT name
        FROM master..sysdatabases WHERE sid != 1
        ORDER BY CASE WHEN name = %s THEN 0
        ELSE 1 END, name'''
        self._cursor.execute(query, (self._cursor.connection.dbname,))
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT table_name AS name, table_schema AS schema_name, 
          CASE WHEN table_schema IN ('information_schema', 'pg_catalog') 
          THEN 1 ELSE 0 END AS sys_ind, p.value AS description
        FROM information_schema.tables t
        LEFT JOIN sys.extended_properties p ON p.major_id = OBJECT_ID(t.table_name)
          AND p.minor_id = 0 
          AND p.name = 'MS_Description'
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema = COALESCE(LOWER(%s), t.table_schema)
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT v.name, s.name schema_name,
          CASE WHEN LOWER(s.name) IN ('information_schema', 'sys') 
          THEN 1 ELSE 0 END sys_ind
        FROM sys.all_views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE LOWER(s.name) = LOWER(COALESCE(%s, s.name)) 
        ORDER BY 3, 2, 1''' % self._cursor.placeholder
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
class ProbeFirebird(Probe):
    hasSchema = False
    def getServerVersion(self):
        """Get version of database engine"""
        query = '''SELECT 'Firebird ' || rdb$get_context('SYSTEM', 'ENGINE_VERSION') 
            AS version FROM rdb$database'''
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        """Return list of databases/catalogs."""
        print('Not supported on Firebird.')
        return []
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_connected_db, sys_ind)]."""
        # Firebird doesn't have schema. Return a list of object owners instead. 
        query = '''SELECT DISTINCT TRIM(rdb$owner_name) name, NULL is_connected_db, 
          NULL sys_ind
        FROM rdb$relations ORDER BY 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT TRIM(rdb$relation_name) name,
              TRIM(rdb$owner_name) schema_name,
              rdb$system_flag sys_ind,
              rdb$description AS description
            FROM rdb$relations 
            WHERE rdb$view_blr IS NULL
              AND rdb$owner_name  = COALESCE(?, rdb$owner_name)
            ORDER BY 3, 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT TRIM(rdb$relation_name) name,
              TRIM(rdb$owner_name) schema_name,
              rdb$system_flag sys_ind,
              rdb$description AS description
            FROM rdb$relations 
            WHERE rdb$view_blr IS NOT NULL
              AND rdb$owner_name  = COALESCE(?, rdb$owner_name)
            ORDER BY 3, 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getFunctions(self, schema=None):
        """Return list of functions. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT TRIM(rdb$function_name) name, 
              NULL                          schema_name,
              rdb$system_flag               sys_ind,
              rdb$description               description 
            FROM rdb$functions
            ORDER BY 3, 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getProcedures(self, schema=None):
        """Return list of procedures. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT TRIM(rdb$procedure_name) name, 
              NULL                           schema_name,
              rdb$system_flag                sys_ind,
              rdb$description                description 
            FROM rdb$procedures
            ORDER BY 3, 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk, description)]"""
        query = '''SELECT TRIM(rf.rdb$field_name) AS name,
              TRIM(CASE f.rdb$field_type
                  WHEN 261 THEN 'BLOB'
                  WHEN 14 THEN 'CHAR'
                  WHEN 40 THEN 'C STRING'
                  WHEN 11 THEN 'D_FLOAT'
                  WHEN 27 THEN 'DOUBLE'
                  WHEN 10 THEN 'FLOAT'
                  WHEN 16 THEN 'INT64'
                  WHEN 8 THEN 'INTEGER'
                  WHEN 9 THEN 'QUAD'
                  WHEN 7 THEN 'SMALLINT'
                  WHEN 12 THEN 'DATE'
                  WHEN 13 THEN 'TIME'
                  WHEN 35 THEN 'TIMESTAMP'
                  WHEN 37 THEN 'VARCHAR'
                  ELSE 'UNKNOWN'
              END) AS data_type,
              CASE WHEN rf.rdb$null_flag = 1 THEN 0 ELSE 1 END is_nullable,
              CASE WHEN seg.rdb$field_name IS NOT NULL THEN 1 END pk_ind,
              rf.rdb$description description
            FROM rdb$relation_fields rf
            JOIN rdb$fields f ON rf.rdb$field_source = f.rdb$field_name
            LEFT JOIN rdb$relation_constraints rc ON rf.rdb$relation_name = rc.rdb$relation_name
              AND rc.rdb$constraint_type = 'PRIMARY KEY'
            LEFT JOIN rdb$index_segments seg ON rc.rdb$index_name = seg.rdb$index_name
              AND rf.rdb$field_name = seg.rdb$field_name
            WHERE UPPER(rf.rdb$relation_name) = UPPER(?)
            ORDER BY rf.rdb$field_position'''
        self._cursor.execute(query, (table,))
        return self._cursor.fetchall()
    
class ProbeDB2(Probe):
    def getServerVersion(self):
        """Get version of database engine"""
        query = ''' SELECT service_level FROM TABLE(SYSPROC.ENV_GET_INST_INFO())'''
        return self._cursor.selectinto(query)
    
    def getDatabases(self):
        """Return list of databases/catalogs."""
        return []
    
    def getSchemas(self):
        """Return list of schemas. [Record(name, is_connected_db, sys_ind)]."""
        query = '''SELECT schemaname AS name,
          CASE WHEN schemaname LIKE 'SYS%' THEN 2
            WHEN definertype = 'U' THEN 0
            ELSE 2 END AS sys_ind 
        FROM syscat.schemata
        ORDER BY 2, 1'''
        self._cursor.execute(query)
        return self._cursor.fetchall()
    
    def getTables(self, schema=None):
        """Return list of tables. [Record(name, schema_name, sys_ind, num_rows, description)]"""
        query = '''SELECT tabname AS name,
          TRIM(tabschema) AS schema_name, 
          CASE WHEN tabschema LIKE 'SYS%' THEN 1
          WHEN ownertype = 'U' THEN 0
          ELSE 1 END sys_ind,
          t.card AS num_rows,
          t.remarks AS description
        FROM syscat.tables t
        WHERE type = 'T'
          AND tabschema = COALESCE(?, tabschema)
        ORDER BY 3,2,1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getViews(self, schema=None):
        """Return list of views. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT viewname AS name,
          TRIM(viewschema) AS schema_name,
          CASE WHEN ownertype = 'U' THEN 0 
          ELSE 1 END AS sys_ind
        FROM syscat.views
        WHERE viewschema = COALESCE(?, viewschema)
        ORDER BY 3, 2 , 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
        
    def getFunctions(self, schema=None):
        """Return list of functions. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT DISTINCT funcname AS name,
          TRIM(funcschema) AS schema_name,
          CASE WHEN funcschema LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind
        FROM syscat.functions
        WHERE substr(funcname,1,1) NOT IN ('<', '>', '=')
          AND funcschema = COALESCE(?, funcschema)
        ORDER BY 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
        
    def getProcedures(self, schema=None):
        """Return list of procedures. [Record(name, schema_name, sys_ind)]"""
        query = '''SELECT DISTINCT procname AS name,
          TRIM(procschema) AS schema_name, 
          CASE WHEN procschema LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind
        FROM syscat.procedures
        WHERE procschema = COALESCE(?, procschema)
        ORDER BY 1, 2'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getPackages(self, schema=None):
        """Return list of packages. [Record(name, schema_name, sys_ind, description)]"""
        query = '''SELECT pkgname AS name,
          pkgschema AS schema_name,
          CASE WHEN pkgschema LIKE 'SYS%' THEN 1 ELSE 0 END sys_ind,
          remarks AS description
        FROM syscat.packages p
        WHERE pkgschema = COALESCE(?, pkgschema)
        ORDER BY 2, 1'''
        self._cursor.execute(query, (schema,))
        return self._cursor.fetchall()
    
    def getColumns(self, table, schema=None):
        """Return list of table/view columns. [Record(name, data_type, is_nullable, is_pk, description)]"""
        query = '''SELECT colname AS name,
          typename AS data_type,
          CASE WHEN c.nulls = 'Y' THEN 1 ELSE 0 END AS is_nullable,
          CASE WHEN keyseq IS NOT NULL THEN 1 ELSE 0 END AS is_pk,
          c.remarks AS description
        FROM syscat.columns c
        WHERE tabname = ?
          AND tabschema = COALESCE(?, tabschema) 
        ORDER BY colno'''
        self._cursor.execute(query, (table, schema))
        return self._cursor.fetchall()