class Inspect(object):
    """Inspect returns a simple list of object names.
    The Connection object instantiates Inspect in the Connection.inspect attribute. 
    The database schemata is cached so if you are executing DDL statements
    you must call inspect.refresh() to see the changes.
    
    If you need detailed information, rather than a list of names, use Connection.probe instead.
    """
    def __init__(self, probe):
        if hasattr(probe, 'getTables'):
            self._probe = probe
            self._schemata = {'schemas': [],
                              'tables': [],
                              'views': [],
                              'functions': [],
                              'procedures': [],
                              'packages': []}
            self._tabColumns = {}
            self._template = '{name}'
            self.qualifyNames = False
            if self._probe._server == 'mssql':
                self._quoteChars = '[]'
            elif self._probe._server == 'mysql':
                self._quoteChars = '``'
            else:
                self._quoteChars = '""'
        else:
            raise TypeError('First argument must be a dbms.probe.Probe object.')
    
    def formatName(self, name, schema = None):
        """Properly quote identifiers if needed."""
        if not (name.islower() or name.isupper() or name.startswith(self._quoteChars[0])):
            # quote mixed case identifier
            if self.qualifyNames and schema:
                return '%s%s%s.%s%s%s' % (self._quoteChars[0], schema, self._quoteChars[1],
                                         self._quoteChars[0], name, self._quoteChars[1])
            else:
                return '%s%s%s' % (self._quoteChars[0], name, self._quoteChars[1])
        else:
            if self.qualifyNames and schema:
                return '%s.%s' % (schema, name)
            else:
                return name
    
    def refresh(self, object_type='tree'):
        """Refresh objects"""
        if object_type in self._schemata:
            self._schemata[object_type] = []
        elif object == 'tree':
            for key in self._schemata:
                self._schemata[key] = []
        elif object_type in self._tabColumns:
            self._tabColumns[object_type] = []
    
    def databases(self):
        return [row[0] for row in self._probe.getDatabases()]
    
    def schemas(self):
        if not self._schemata['schemas']:
            self._schemata['schemas'] = self._probe.getSchemas()
        return [row[0] for row in self._schemata['schemas']]
    
    def tables(self, schema=None):
        if not self._schemata['tables']:
            self._schemata['tables'] = self._probe.getTables()

        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['tables'] 
                if rec['sys_ind'] == 0
                and (rec['schema_name'] == schema or schema is None)]
    
    def systemTables(self, schema=None):
        if not self._schemata['tables']:
            self._schemata['tables'] = self._probe.getTables()
        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['tables'] 
                if rec['sys_ind'] > 0
                and (rec['schema_name'] == schema or schema is None)]        
    
    def views(self, schema=None):
        if not self._schemata['views']:
            self._schemata['views'] = self._probe.getViews()

        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['views']
                if rec['sys_ind'] == 0 and (rec['schema_name'] == schema or schema is None)]
        
    def systemViews(self, schema=None):
        if not self._schemata['views']:
            self._schemata['views'] = self._probe.getViews()
        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['views']
                if rec['sys_ind'] > 0 and (rec['schema_name'] == schema or schema is None)]
    
    def functions(self, schema=None):
        if not self._schemata['functions']:
            self._schemata['functions'] = self._probe.getFunctions()
            
        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['functions'] 
                if rec['schema_name'] == schema or schema is None]
    
    def procedures(self, schema=None):
        if not self._schemata['procedures']:
            self._schemata['procedures'] = self._probe.getProcedures()
        return [self.formatName(rec['name'], rec['schema_name']) for rec in self._schemata['procedures'] 
                if rec['schema_name'] == schema or schema is None]
        
    def columns(self, table, schema=None):
        if '.' in table:
            schema, table = table.split('.')
        if table.startswith(self._quoteChars[0]):
            table = table[1:-1]
        if schema and schema.startswith(self._quoteChars[0]):
            schema = schema[1:-1]
        if table not in self._tabColumns:
            self._tabColumns[table] = self._probe.getColumns(table, schema)
        return [self.formatName(rec['name']) for rec in self._tabColumns[table]]
        