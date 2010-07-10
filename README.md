raisin
======

Raisin is a Python library for storing, retrieving and indexing JSON objects
(Python dictionaries) in MySQL.

Install
-------

Make sure you either have the MySQL and Python development libraries and header
files installed (*libmysqlclient-dev* and *libpython2.6* Ubuntu packages) or the
Python MySQLdb library (*python-mysqldb* Ubuntu package).  Then run...

    $ python setup.py install

API
---

Instantiate our data store:

    import raisin
    d = raisin.DataStore('dbname', 'username', 'passwd', host = 'somehost')

Before we can use a data store we must create the underlying MySQL table.

    d.create()

Put a JSON object into the data store:

    id = d.put({'test': 'abc'})

Retrieve the JSON object:

    obj = d.get(id)

`get()` returns the JSON object or `None` if no such id exists.

Update and object:

    obj['test'] = 'def'
    d.update(obj)

Delete an object:

    d.delete(id)

We can also build indices on top-level attributes:

    d.create_index('test')

Then search on those attributes:

    d.find(test = 'def')

`find()` returns a list of objects matching the single given condition.

We can get a list of existing indices with:

    d.indices
    
Drop the index we created with:

    d.drop_index('test')

To drop all tables associated with the data store (including all index
tables):

    d.drop()
