import numpy as np
import pandas as pd
import pytest
import pymongo
from csv import DictReader
from datetime import datetime, timedelta

import mongots

mongo_client = pymongo.MongoClient()
mongo_client.drop_database('TestDb')


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
    tags,
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


def test_insert_constants_per_month_succeeds(big_collection):
    ts = datetime(2010, 1, 1)
    while ts < datetime(2010, 9, 1):
        value = (ts.month-1) * 5
        assert big_collection.insert_one(value, ts)
        ts += timedelta(days=1)


def test_query_retrieve_expected_constant_per_month(big_collection):
    df = big_collection.query(
        datetime(2010, 1, 1),
        datetime(2010, 9, 1),
        aggregateby='1m',
    )

    assert (9, 5) == df.shape

    assert list(df['count']) == [31, 28, 31, 30, 31, 30, 31, 31, 0]

    assert list(df['mean'])[:8] == [0, 5, 10, 15, 20, 25, 30, 35]
    assert np.isnan(list(df['mean'])[8])

    assert list(df['std'])[:8] == [0, 0, 0, 0, 0, 0, 0, 0]
    assert np.isnan(list(df['std'])[8])

    assert list(df['min'])[:8] == [0, 5, 10, 15, 20, 25, 30, 35]
    assert list(df['min'])[8] == np.inf

    assert list(df['max'])[:8] == [0, 5, 10, 15, 20, 25, 30, 35]
    assert list(df['max'])[8] == -np.inf


@pytest.fixture
def weather_data_pressure():
    with open('test/data/weather_data.csv') as file:
        reader = DictReader(file, delimiter=';')

        result = []

        for row in reader:
            try:
                timestamp = datetime.strptime(
                    row['datetime'],
                    '%Y-%m-%d %H:%M:%S'
                )
                pressure = float(row['atmospheric pressure'])
                city = row['city']

                result.append((pressure, timestamp, {'city': city}))
            except Exception as e:
                pass

        return result


@pytest.fixture
def pressure_collection(db):
    return db.atmosphericPressure


def test_insert_pressure_succeeds(pressure_collection, weather_data_pressure):
    assert 6348 == len(weather_data_pressure)
    for pressure, timestamp, tags in weather_data_pressure:
        assert pressure_collection.insert_one(pressure, timestamp, tags=tags)


@pytest.mark.parametrize('args, kwargs, expected', [(
    # 1996 per year
    (datetime(1996, 1, 1), datetime(1996, 12, 31)),
    {'aggregateby': '1y'},
    {
        'index': pd.Index([datetime(1996, 1, 1)], name='datetime'),
        'data': [
            [6348, 1001.0, 1074.2, 1.015427520478e+03, 5.8321529378],
        ],
    },
), (
    # 1996 per month
    (datetime(1996, 1, 1), datetime(1996, 12, 31)),
    {'aggregateby': '1m'},
    {
        'index': pd.Index([
            datetime(1996, 1, 1),
            datetime(1996, 2, 1),
            datetime(1996, 3, 1),
            datetime(1996, 4, 1),
            datetime(1996, 5, 1),
            datetime(1996, 6, 1),
            datetime(1996, 7, 1),
            datetime(1996, 8, 1),
            datetime(1996, 9, 1),
            datetime(1996, 10, 1),
            datetime(1996, 11, 1),
            datetime(1996, 12, 1),
        ], name='datetime'),
        'data': [
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [3431, 1001.0, 1074.2, 1016.739872, 6.253782],
            [2917, 1002.0, 1028.1, 1013.883922, 4.859204],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
        ],
    },
), (
    # 1996 summer per month and per city
    (datetime(1996, 7, 15), datetime(1996, 9, 15)),
    {'aggregateby': '1m', 'groupby': ['city']},
    {
        'index': pd.MultiIndex.from_product([
            [datetime(1996, 7, 1), datetime(1996, 8, 1), datetime(1996, 9, 1)],
            ['istanbul', 'london', 'paris'],
        ], names=['datetime', 'city']),
        'data': [
            [1244, 1001, 1074.2, 1014.047186, 4.207450],
            [780, 1001, 1037.3, 1017.958462, 7.899298],
            [1407, 1006.1, 1028.1, 1018.445060, 5.914784],
            [1063, 1003, 1019, 1012.393979, 2.477956],
            [639, 1002, 1028.1, 1014.007668, 6.711384],
            [1215, 1004.1, 1026.1, 1015.122387, 4.913515],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
            [0, np.inf, -np.inf, np.nan, np.nan],
        ],
    },
), (
    # 1996 July per day and per city
    (datetime(1996, 7, 15), datetime(1996, 7, 20)),
    {'aggregateby': '1d', 'groupby': ['city']},
    {
        'index': pd.MultiIndex.from_product([[
            datetime(1996, 7, 15),
            datetime(1996, 7, 16),
            datetime(1996, 7, 17),
            datetime(1996, 7, 18),
            datetime(1996, 7, 19),
            datetime(1996, 7, 20),
        ], [
            'istanbul',
            'london',
            'paris'
        ]], names=['datetime', 'city']),
        'data': [
            [43, 1013.2, 1017.9, 1015.558140, 1.259980],
            [23, 1027.1, 1032.2, 1029.286957, 1.153936],
            [48, 1024.0, 1027.1, 1025.845833, 0.640949],
            [37, 1009.1, 1074.2, 1013.621622, 10.251780],
            [25, 1030.1, 1033.2, 1032.356000, 0.975738],
            [47, 1025.1, 1028.1, 1026.695745, 1.044939],
            [43, 1010.2, 1016.3, 1012.974419, 1.654043],
            [27, 1027.1, 1037.3, 1029.459259, 2.167138],
            [45, 1022.0, 1026.1, 1024.371111, 1.340584],
            [43, 1015.2, 1016.3, 1015.993023, 0.493396],
            [29, 1025.1, 1028.1, 1026.893103, 0.713385],
            [48, 1023.0, 1024.0, 1023.729167, 0.444390],
            [42, 1016.3, 1017.3, 1016.776190, 0.499433],
            [24, 1025.1, 1028.1, 1026.350000, 0.968246],
            [44, 1022.0, 1025.1, 1023.938636, 0.911596],
            [41, 1017.3, 1019, 1018.141463, 0.693515],
            [25, 1023.0, 1025.1, 1024.372000, 0.831153],
            [45, 1022.0, 1024.0, 1023.022222, 0.614234],
        ],
    },
), (
    # 1996 July 16th, per hours
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 15)),
    {'aggregateby': '1h'},
    {
        'index': pd.Index([
            datetime(1996, 7, 16, 11),
            datetime(1996, 7, 16, 12),
            datetime(1996, 7, 16, 13),
            datetime(1996, 7, 16, 14),
            datetime(1996, 7, 16, 15),
        ], name='datetime'),
        'data': [
            [5, 1012.2, 1033.2, 1022.760, 8.821020],
            [5, 1012.2, 1033.2, 1022.760, 8.821020],
            [4, 1012.2, 1033.2, 1026.425, 8.582067],
            [5, 1011.2, 1033.2, 1021.960, 9.063465],
            [5, 1011.2, 1032.2, 1021.560, 8.708295],
        ],
    },
), (
    # 1996 July 16th, per hours in Paris
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 15)),
    {'aggregateby': '1h', 'tags': {'city': 'paris'}},
    {
        'index': pd.Index([
            datetime(1996, 7, 16, 11),
            datetime(1996, 7, 16, 12),
            datetime(1996, 7, 16, 13),
            datetime(1996, 7, 16, 14),
            datetime(1996, 7, 16, 15),
        ], name='datetime'),
        'data': [
            [2, 1028.1, 1028.1, 1028.1, 0.0],
            [2, 1028.1, 1028.1, 1028.1, 0.0],
            [1, 1027.1, 1027.1, 1027.1, 0.0],
            [2, 1027.1, 1027.1, 1027.1, 0.0],
            [2, 1026.1, 1027.1, 1026.6, 0.5],
        ],
    },
), (
    # 1996 July 16th, per hours everywhere but in Paris
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 15)),
    {'aggregateby': '1h', 'tags': {'city': {'$ne': 'paris'}}},
    {
        'index': pd.Index([
            datetime(1996, 7, 16, 11),
            datetime(1996, 7, 16, 12),
            datetime(1996, 7, 16, 13),
            datetime(1996, 7, 16, 14),
            datetime(1996, 7, 16, 15),
        ], name='datetime'),
        'data': [
            [3, 1012.2, 1033.2, 1019.200000, 9.899495],
            [3, 1012.2, 1033.2, 1019.200000, 9.899495],
            [3, 1012.2, 1033.2, 1026.200000, 9.899495],
            [3, 1011.2, 1033.2, 1018.533333, 10.370899],
            [3, 1011.2, 1032.2, 1018.200000, 9.899495],
        ],
    },
), (
    # 1996 July 16th, per hours and per city in Paris and London
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 13)),
    {
        'aggregateby': '1h',
        'groupby': ['city'],
        'tags': {'city': {'$in': ['paris', 'london']}},
    }, {
        'index': pd.MultiIndex.from_product([[
            datetime(1996, 7, 16, 11),
            datetime(1996, 7, 16, 12),
            datetime(1996, 7, 16, 13),
        ], [
            'london',
            'paris',
        ]], names=['datetime', 'city']),
        'data': [
            [1, 1033.2, 1033.2, 1033.2, 0.0],
            [2, 1028.1, 1028.1, 1028.1, 0.0],
            [1, 1033.2, 1033.2, 1033.2, 0.0],
            [2, 1028.1, 1028.1, 1028.1, 0.0],
            [2, 1033.2, 1033.2, 1033.2, 0.0],
            [1, 1027.1, 1027.1, 1027.1, 0.0],
        ],
    },
), (
    # 1996 July 16th, per 2 hours in Paris and London
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 13)),
    {
        'aggregateby': '2h',
        'tags': {'city': {'$in': ['paris', 'london']}},
    }, {
        'index': pd.Index([
            datetime(1996, 7, 16, 10),
            datetime(1996, 7, 16, 12),
        ], name='datetime'),
        'data': [
            [3, 1028.1, 1033.2, 1029.8, 2.40416305609],
            [6, 1027.1, 1033.2, 1030.48333333, 2.73704016938],
        ],
    },
), (
    # 1996 July 16th, per 2 hours per city in Paris and London
    (datetime(1996, 7, 16, 11), datetime(1996, 7, 16, 13)),
    {
        'aggregateby': '2h',
        'groupby': ['city'],
        'tags': {'city': {'$in': ['paris', 'london']}},
    }, {
        'index': pd.MultiIndex.from_product([[
            datetime(1996, 7, 16, 10),
            datetime(1996, 7, 16, 12),
        ], [
            'london',
            'paris',
        ]], names=['datetime', 'city']),
        'data': [
            [1, 1033.2, 1033.2, 1033.2, 0.0],
            [2, 1028.1, 1028.1, 1028.1, 0.0],
            [3, 1033.2, 1033.2, 1033.2, 0.0],
            [3, 1027.1, 1028.1, 1027.766666, 0.471404],
        ],
    },
), (
    # no data for the selected range
    (datetime(1995, 7, 10), datetime(1995, 8, 10)),
    {'aggregateby': '1d'},
    {
        'index': pd.Index([], name='datetime'),
        'data': [],
    },
), (
    # end date before start date
    (datetime(1996, 7, 10), datetime(1996, 7, 9)),
    {'aggregateby': '1d'},
    {
        'index': pd.Index([], name='datetime'),
        'data': [],
    },
)])
def test_pressure_queries(pressure_collection, args, kwargs, expected):
    actual_df = pressure_collection.query(*args, **kwargs)

    expected_df = pd.DataFrame(
        index=expected['index'],
        columns=['count', 'min', 'max', 'mean', 'std'],
        data=expected['data'],
    )

    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_get_tags(pressure_collection):
    assert pressure_collection.get_tags() == {
        'city': ['paris', 'london', 'istanbul'],
    }


def test_get_timerange(pressure_collection):
    min_datetime, max_datetime = pressure_collection.get_timerange()

    assert min_datetime == datetime(1996, 7, 1, 1)
    assert max_datetime == datetime(1996, 8, 26, 21, 30)


def test_get_collections(db):
    collections = db.get_collections()

    assert 3 == len(collections)

    assert collections[0] == {
        'collection_name': 'temperatures',
        'count': 4,
        'timerange': (
            datetime(2010, 7, 23, 13, 45),
            datetime(2010, 7, 25, 20),
        ),
        'tags': {'city': ['Paris']},
    }

    assert collections[1] == {
        'collection_name': 'lotOfValues',
        'count': 243,
        'timerange': (
            datetime(2010, 1, 1),
            datetime(2010, 8, 31),
        ),
        'tags': {},
    }

    assert collections[2] == {
        'collection_name': 'atmosphericPressure',
        'count': 6348,
        'timerange': (
            datetime(1996, 7, 1, 1),
            datetime(1996, 8, 26, 21, 30),
        ),
        'tags': {
            'city': ['paris', 'london', 'istanbul'],
        },
    }
