import MySQLdb

class Row(dict):
    """Dictionary with attribute-based lookup."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError

class Database(object):
    """Wrapper around MySQL connection."""

    def __init__(self, db, user, passwd, host = '127.0.0.1', **kwargs):
        self._db = None
        self._db_args = {'db': db, 'user': user, 'passwd': passwd, 'host': host}
        self._db_args.update(**kwargs)
        self.connect()

    def close(self):
        """Close the connection to the database."""
        if self._db is not None:
            self._db.close()
            self._db = None

    def commit(self):
        """Commit the current transaction."""
        self._db.commit()

    def connect(self):
        """Connect to the database."""
        self._db = MySQLdb.connect(**self._db_args)
        self._db.autocommit(False)

    def delete(self, table):
        """Perform a simple delete."""
        template = 'DELETE FROM `%s`%s'
        def where(**kwargs):
            pairs = self._join(' AND ', "`%s`='%s'", kwargs.items())
            where = kwargs and ' WHERE %s' % pairs or ''
            return self.execute(template % (table, where))
        def all():
            return self.execute(template % (table, ''))
        return methodsuite(where, all)

    def drop(self, table):
        """Drop the given table."""
        return self.execute('DROP TABLE IF EXISTS `%s`' % table)

    def execute(self, query):
        """Execute the given query and return the last row id."""
        cursor = self._cursor()
        try:
            cursor.execute(query)
            return cursor.lastrowid
        finally:
            cursor.close()

    def get(self, table):
        """Perform a simple select query returning a single row."""
        def query_single(query):
            rows = self.query(query)
            if len(rows) == 0:
                return None
            else:
                return rows[0]
        return self.select(table, query_func = query_single)

    def insert(self, table):
        """Perform simple insert."""
        def set(**kwargs):
            fields = '(%s)' % self._join(', ', "`%s`", kwargs.keys())
            values = '(%s)' % self._join(', ', "'%s'", kwargs.values())
            return self.execute('INSERT INTO `%s` %s VALUES %s' % (table, fields, values))
        return methodsuite(set)

    def query(self, query):
        """Execute the given query and return the rows as dictionaries."""
        cursor = self._cursor()
        try:
            cursor.execute(query)
            columns = [d[0] for d in cursor.description]
            return [Row(zip(columns, row)) for row in cursor]
        finally:
            cursor.close()

    def select(self, table, query_func = None):
        """Perform simple select query."""
        template = 'SELECT * FROM `%s`%s'
        query_func = query_func or self.query
        def where(**kwargs):
            pairs = self._join(' AND ', "`%s`='%s'", kwargs.items())
            where = kwargs and ' WHERE %s' % pairs or ''
            return query_func(template % (table, where))
        def all():
            return query_func(template % (table, ''))
        return methodsuite(where, all)

    def update(self, table):
        """Perform simple update."""
        template = 'UPDATE `%s` SET %s%s'
        def set(**kwargs):
            set = self._join(', ', "`%s`='%s'", kwargs.items())
            def where(**kwargs):
                pairs = self._join(' AND ', "`%s`='%s'", kwargs.items())
                where = kwargs and ' WHERE %s' % pairs or ''
                return self.execute(template % (table, set, where))
            def all():
                return self.execute(template % (table, set, ''))
            return methodsuite(where, all)
        return methodsuite(set)

    def _cursor(self):
        """Get the cursor from the database connection."""
        if self._db is None:
            self.connect()
        return self._db.cursor()

    def _join(self, joiner, template, items):
        """Join each item applied to the given string template."""
        return joiner.join([template % x for x in items])

class methodsuite(object):
    """Class whose methods are passed as constructor parameters."""

    def __init__(self, *args):
        for f in args:
            setattr(self, f.func_name, f)
