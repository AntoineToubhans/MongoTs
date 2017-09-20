MongoTS
======

A fast API for storing time series in MongoDb

[![Build Status](https://travis-ci.org/AntoineToubhans/MongoTs.svg?branch=master)](https://travis-ci.org/AntoineToubhans/MongoTs)

## Install

1. Clone this repo
2. install dependencies: `pip install -r requirements.txt`

## Usage

You can instanciate client, database, collection just like you would
do with pymongo:

```python
import mongots
client = mongots.MongoTSClient()
db = client.MyDatabase
collection = db.temperatures
```

### Insert

```
collection.insert_one(value, datetime, tags=tags)
```

Arguments:
- `value` (float): the value to be inserted
- `timestamp` (ddatemie.datetime): the timestamp for the value
- `tags` (dict, default=None): tags for the value; can be use the search/filter the value later on.

Return (bool): `True` if the insertion succeeded, `False` otherwise.

### Query

```
query(self, start, end, interval=interval, tags=tags, groupby=groupby)
```

Arguments:
- `start` (datetime): filters values after the start datetime
- `end` (datetime): filters values before the end datetime
- `interval` (str): compute statistics for each year ('1y'), month ('1m'), day ('1d') or hour ('1h')
- `tags` (dict, default=None): similar to a mongo filter
- `groupby` (array): return statistics grouped by a list of tags (string)

Return (pandas.DataFrame): dataframe containing the statistics and indexed by datetimes.


## Run tests

Integration test requires a MongoDb to be up (run docker-compose up).

Launch all tests:

```bash
pytest
```

Launch only unit test:

```bash
python -m unittest -v test
```
