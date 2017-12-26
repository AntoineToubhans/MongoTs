==================
MongoTs
==================


.. image:: https://img.shields.io/pypi/v/mongots.svg
        :target: https://pypi.python.org/pypi/mongots

.. image:: https://travis-ci.org/AntoineToubhans/MongoTs.svg?branch=master
        :target: https://travis-ci.org/AntoineToubhans/MongoTs
        :alt: Build Status

.. image:: https://coveralls.io/repos/github/AntoineToubhans/MongoTs/badge.svg?branch=master
        :target: https://coveralls.io/github/AntoineToubhans/MongoTs?branch=master
        :alt: Coverage Status

.. image:: https://readthedocs.org/projects/mongots/badge/?version=latest
        :target: https://mongots.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://api.codeclimate.com/v1/badges/86bcfbb432f84462a594/maintainability
        :target: https://codeclimate.com/github/AntoineToubhans/MongoTs/maintainability
        :alt: Maintainability

A fast python API for storing and querying time series in MongoDb.


* Free software: MIT license
* Documentation: https://mongots.readthedocs.io.


Requirements
------------

* python >= 3.3
* MongoDb >= 3.4

Install
-------

Install package::

    pip install mongots

Manual install:

1. Clone this repo: ``git clone git@github.com:AntoineToubhans/MongoTs.git``
2. install dependencies: ``pip install -r requirements.txt``


Usage
-----

You can instanciate client, database, collection just like you would
do with pymongo::

    import mongots
    client = mongots.MongoTSClient()
    db = client.MyDatabase
    collection = db.temperatures

Insert
~~~~~~

    collection.insert_one(value, datetime, tags=tags)

Arguments:

* ``value`` (float): the value to be inserted
* ``timestamp`` (ddatemie.datetime): the timestamp for the value
* ``tags`` (dict, default=None): tags for the value; can be use the search/filter the value later on.

Return (bool): ``True`` if the insertion succeeded, ``False`` otherwise.

Query
~~~~~

    collection.query(start, end, tags=tags, aggregateby=aggregateby, groupby=groupby)

Arguments:

* ``start`` (datetime): filters values after the start datetime
* ``end`` (datetime): filters values before the end datetime
* ``aggregateby`` (str): aggregates value per interval:

  * years: '1y', '2y', ...
  * month '1M', '2M', ...
  * days: '1d', '2d', ...
  * hours: '1h', '2h', ...
* ``tags`` (dict, default=None): similar to a mongo filter
* ``groupby`` (array): return statistics grouped by a list of tags (string)

Return (pandas.DataFrame): dataframe containing the statistics and indexed by datetimes.

Get tags
~~~~~

    collection.get_tags()

Return (dict): The tags contained in the collection.

Get timerange
~~~~~

    collection.get_timerange():

Return (None | tuple):
    None if the collection is empty.
    Otherwise, a tuple containing two datetime object representing
    the minimum and maximum timestamps in the collection.

Run tests
---------

Integration test requires a MongoDb to be up (run docker-compose up).

Launch all tests::

    pytest

Launch only unit test::

    python -m unittest -v test
