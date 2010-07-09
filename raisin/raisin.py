import json
import mysql
import sql
import uuid

class DataStore(object):

    table = 'entities'

    def __init__(self, db, user, passwd, host = '127.0.0.1'):
        self.db = mysql.Database(db, user, passwd, host = host)

    def create(self):
        """Create the datastore's table."""
        self.db.execute(sql.create_table % self.table)
        self.db.commit()

    def create_index(self, attr):
        """Create an index on the given attribute."""
        if attr in self.indices:
            raise AttributeError('index already exists')
        index = Index(self, attr)
        index.create()
        for row in self.db.select('entities').all():
            entity = json.loads(row.entity)
            if attr in entity:
                index.insert(entity['id'], entity[attr])
        self.db.commit()

    def delete(self, id):
        """Delete the entity with the given id from the datastore."""
        entity = self.get(id)
        self.db.delete(self.table).where(id = id)
        self._unindex(entity)
        self.db.commit()

    def drop(self):
        """Drop the datastore's table and all associated indices."""
        self.db.drop(self.table)
        for index in self.indices.itervalues():
            index.drop()
        self.db.commit()

    def drop_index(self, attr):
        """Drop the index for the given attribute."""
        indices = self.indices
        if attr not in indices:
            raise AttributeError('non-existent index')
        index = indices[attr]
        index.drop()
        self.db.commit()

    def find(self, **kwargs):
        """Find entities with all the given attribute/value pairs."""
        if len(kwargs) != 1:
            raise TypeError('exactly one search condition must be given')
        attr, value = kwargs.items().pop()
        if attr not in self.indices:
            raise AttributeError('%s attribute not indexed' % attr)
        entities = []
        for id in self.indices[attr].ids(value):
            entity = self.get(id)
            if entity and attr in entity:
                entities.append(entity)
        return entities

    def get(self, id):
        """Get the entity with the given id."""
        row = self.db.get(self.table).where(id = id)
        if row is None:
            return None
        else:
            return json.loads(row.entity)

    @property
    def indices(self):
        """Get list of indexed attributes."""
        all_tables = map(lambda x: x.values().pop(), self.db.query("SHOW TABLES"))
        index_tables = filter(lambda x: x.startswith(Index.prefix), all_tables)
        attrs = [x[len(Index.prefix):] for x in index_tables]
        return dict([(x, Index(self, x)) for x in attrs])

    def put(self, entity):
        """Put the given entity into the datastore."""
        id = self._identify(entity)
        self.db.insert(self.table).set(id = id, entity = json.dumps(entity))
        self._index(entity)
        self.db.commit()
        return id

    def update(self, entity):
        """Update the given entity in the datastore."""
        id = self._identify(entity)
        self._unindex(self.get(id))
        self.db.update(self.table).set(entity = json.dumps(entity)).where(id = id)
        self._index(entity)
        self.db.commit()

    def _identify(self, entity):
        """Ensure entity has an id."""
        if 'id' in entity:
            return entity['id']
        else:
            id = str(uuid.uuid4())
            entity['id'] = id
            return id

    def _index(self, entity):
        """Index the given entity across all indices."""
        for attr in self.indices:
            if attr in entity:
                self.indices[attr].insert(entity['id'], entity[attr])

    def _unindex(self, entity):
        """Unindex the given entity across all indices."""
        for attr in self.indices:
            if attr in entity:
                self.indices[attr].delete(entity['id'], entity[attr])

class Index(object):

    prefix = 'index_'

    def __init__(self, datastore, attribute):
        self.datastore = datastore
        self.attribute = attribute

    def create(self):
        """Create the index's table."""
        self.datastore.db.execute(sql.create_index % self._table)

    def delete(self, id, value):
        """Delete the given id/value pair from the index."""
        self.datastore.db.delete(self._table).where(id = id, value = value)

    def drop(self):
        """Drop the index's table."""
        self.datastore.db.drop(self._table)

    def ids(self, value):
        """Get list of entity ids with the given value."""
        rows = self.datastore.db.select(self._table).where(value = value)
        return [row.id for row in rows]

    def insert(self, id, value):
        """Insert the given id/value pair in the index."""
        self.datastore.db.insert(self._table).set(id = id, value = value)

    def update(self, id, value):
        """Update the given id/value pair in the index."""
        self.datastore.db.update(self._table).set(value = value).where(id = id)

    @property
    def _table(self):
        """The index's table name."""
        return '%s%s' % (self.prefix, self.attribute)
