import pytest
import pymongo
from datetime import datetime, timedelta

import mongots

mongo_client = pymongo.MongoClient()
mongo_client.TestDb.temperatures.remove({})
mongo_client.TestDb.lotOfValues.remove({})


@pytest.fixture
def client():
    return mongots.MongoTSClient(mongo_client=mongo_client)


@pytest.fixture
def db(client):
    return client.TestDb


@pytest.fixture
def collection(db):
    return db.temperatures


def test_are_mongots_instances(client, db, collection):
    assert isinstance(client, mongots.MongoTSClient)
    assert isinstance(db, mongots.MongoTSDatabase)
    assert isinstance(collection, mongots.MongoTSCollection)


def test_contains_mongo_instances(client, db, collection):
    assert isinstance(client._client, pymongo.MongoClient)
    assert isinstance(db._database, pymongo.database.Database)
    assert isinstance(collection._collection, pymongo.collection.Collection)


temperatures_in_paris = [
    (35.6, datetime(2010, 7, 23, 13, 45), {'city': 'Paris'}),
    (36.8, datetime(2010, 7, 23, 15), {'city': 'Paris'}),
    (29, datetime(2010, 7, 23, 23, 57), {'city': 'Paris'}),
    (18, datetime(2010, 7, 25, 20), {'city': 'Paris'}),
]


@pytest.mark.parametrize('value, timestamp, tags', temperatures_in_paris)
def test_insert_temperatures_in_paris_one_by_one(
    collection,
    value,
    timestamp,
    tags
):
    assert collection.insert_one(value, timestamp, tags=tags)


def test_right_number_of_documents_were_inserted(collection):
    assert 1 == collection._collection.count({})
    assert 1 == collection._collection.count({'city': 'Paris'})


def test_yearly_temperatures_in_paris_were_correctly_inserted(collection):
    year_document = collection._collection.find_one({
        'city': 'Paris',
    }, {
        'count': 1,
        'sum': 1,
        'sum2': 1,
    })

    assert 4 == year_document['count']
    assert 35.6 + 36.8 + 29 + 18 == year_document['sum']
    assert 35.6**2 + 36.8**2 + 29**2 + 18**2 == year_document['sum2']


def test_monthly_temperatures_in_paris_were_correctly_inserted(collection):
    months_document = collection._collection.find_one({
        'city': 'Paris',
    }, {
        'months': 1,
    })['months']

    assert 12 == len(months_document)

    for month_index, month_document in enumerate(months_document):
        if 6 == month_index:
            assert 4 == month_document['count']
            assert 35.6 + 36.8 + 29 + 18 == month_document['sum']
            assert 35.6**2 + 36.8**2 + 29**2 + 18**2 == month_document['sum2']
        else:
            assert 0 == month_document['count']
            assert 0 == month_document['sum']
            assert 0 == month_document['sum2']


def test_daily_temperatures_in_paris_were_correctly_inserted(collection):
    days_document = collection._collection.find_one({
        'city': 'Paris',
    }, {
        'months': 1,
    })['months'][6]['days']

    assert 31 == len(days_document)

    for day_index, day_document in enumerate(days_document):
        if 22 == day_index:
            assert 3 == day_document['count']
            assert 35.6 + 36.8 + 29 == day_document['sum']
            assert 35.6**2 + 36.8**2 + 29**2 == day_document['sum2']
        elif 24 == day_index:
            assert 1 == day_document['count']
            assert 18 == day_document['sum']
            assert 18**2 == day_document['sum2']
        else:
            assert 0 == day_document['count']
            assert 0 == day_document['sum']
            assert 0 == day_document['sum2']


def test_hourly_temperatures_in_paris_were_correctly_inserted(collection):
    hours_document = collection._collection.find_one({
        'city': 'Paris',
    }, {
        'months': 1,
    })['months'][6]['days'][22]['hours']

    assert 24 == len(hours_document)

    for hour_index, hour_document in enumerate(hours_document):
        if 13 == hour_index:
            assert 1 == hour_document['count']
            assert 35.6 == hour_document['sum']
            assert 35.6**2 == hour_document['sum2']
        elif 15 == hour_index:
            assert 1 == hour_document['count']
            assert 36.8 == hour_document['sum']
            assert 36.8**2 == hour_document['sum2']
        elif 23 == hour_index:
            assert 1 == hour_document['count']
            assert 29 == hour_document['sum']
            assert 29**2 == hour_document['sum2']
        else:
            assert 0 == hour_document['count']
            assert 0 == hour_document['sum']
            assert 0 == hour_document['sum2']


@pytest.fixture
def big_collection(db):
    return db.lotOfValues


def test_insert_many_succeeds(big_collection):
    ts = datetime(2010, 1, 1)
    while ts < datetime(2010, 9, 1):
        value = (ts.month-1) * 5
        assert big_collection.insert_one(value, ts)
        ts += timedelta(days=1)


def test_query_retrieve_expected_result(big_collection):
    df = big_collection.query(
        datetime(2010, 1, 1),
        datetime(2010, 9, 1),
        interval='1m',
    )

    assert (9, 3) == df.shape
    assert 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 == df['count'].sum()

    for month in range(1, 9):
        assert (month-1) * 5 == df.loc[datetime(2010, month, 1)]['mean']
        assert 0 == df.loc[datetime(2010, month, 1)]['std']
