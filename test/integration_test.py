import numpy as np
import pytest
import pymongo
from csv import DictReader
from datetime import datetime, timedelta

import mongots

mongo_client = pymongo.MongoClient()
mongo_client.TestDb.temperatures.remove({})
mongo_client.TestDb.lotOfValues.remove({})
mongo_client.TestDb.atmosphericPressure.remove({})


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


def test_query_pressure_per_year_is_correct(pressure_collection):
    df = pressure_collection.query(
        datetime(1996, 1, 1),
        datetime(1996, 12, 31),
        interval='1y',
    )

    assert ['count', 'mean', 'std'] == list(df.columns)
    assert 1 == len(df)

    timestamp = df.index.values[0]
    count, mean, std = df.values[0]

    assert np.datetime64('1996-01-01T00:00:00.000000000') == timestamp
    assert 6348 == count
    assert np.isclose(mean, 1.015427520478e+03)
    assert np.isclose(std, 5.8321529378)


def test_query_pressure_per_month_is_correct(pressure_collection):
    df = pressure_collection.query(
        datetime(1996, 1, 1),
        datetime(1996, 12, 31),
        interval='1m',
    )

    assert ['count', 'mean', 'std'] == list(df.columns)
    assert 12 == len(df)

    expected_pressure_per_month = [
        (
            np.datetime64('1996-01-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-02-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-03-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-04-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-05-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-06-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-07-01T00:00:00.000000000'),
            3431, 1016.739872, 6.253782,
        ), (
            np.datetime64('1996-08-01T00:00:00.000000000'),
            2917, 1013.883922, 4.859204,
        ), (
            np.datetime64('1996-09-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-10-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-11-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ), (
            np.datetime64('1996-12-01T00:00:00.000000000'),
            0, np.nan, np.nan,
        ),
    ]

    for (
            (expected_timestamp, expected_count, expected_mean, expected_std),
            actual_timestamp,
            (actual_count, actual_mean, actual_std),
    ) in zip(expected_pressure_per_month, df.index.values, df.values):
        assert expected_timestamp == actual_timestamp
        assert expected_count == actual_count

        if np.isnan(expected_mean):
            assert np.isnan(actual_mean)
        else:
            assert np.isclose(expected_mean, actual_mean)

        if np.isnan(expected_std):
            assert np.isnan(actual_std)
        else:
            assert np.isclose(expected_std, actual_std)


def test_query_pressure_per_month_groupby_city_is_correct(pressure_collection):
    df = pressure_collection.query(
        datetime(1996, 7, 15),
        datetime(1996, 9, 15),
        interval='1m',
        groupby=['city'],
    )

    assert ['count', 'mean', 'std'] == list(df.columns)
    assert 9 == len(df)

    expected_pressure_per_month = [
        (datetime(1996, 7, 1), 'istanbul', 1244, 1014.047186, 4.207450),
        (datetime(1996, 7, 1), 'london', 780, 1017.958462, 7.899298),
        (datetime(1996, 7, 1), 'paris', 1407, 1018.445060, 5.914784),
        (datetime(1996, 8, 1), 'istanbul', 1063, 1012.393979, 2.477956),
        (datetime(1996, 8, 1), 'london', 639, 1014.007668, 6.711384),
        (datetime(1996, 8, 1), 'paris', 1215, 1015.122387, 4.913515),
        (datetime(1996, 9, 1), 'istanbul', 0, np.nan, np.nan),
        (datetime(1996, 9, 1), 'london', 0, np.nan, np.nan),
        (datetime(1996, 9, 1), 'paris', 0, np.nan, np.nan),
    ]

    for (
            (
                expected_timestamp,
                expected_city,
                expected_count,
                expected_mean,
                expected_std
            ),
            (actual_timestamp, actual_city),
            (actual_count, actual_mean, actual_std),
    ) in zip(expected_pressure_per_month, df.index.values, df.values):
        assert expected_timestamp == actual_timestamp
        assert expected_count == actual_count

        if np.isnan(expected_mean):
            assert np.isnan(actual_mean)
        else:
            assert np.isclose(expected_mean, actual_mean)

        if np.isnan(expected_std):
            assert np.isnan(actual_std)
        else:
            assert np.isclose(expected_std, actual_std)


def test_query_pressure_per_day_groupby_city_is_correct(pressure_collection):
    df = pressure_collection.query(
        datetime(1996, 7, 15),
        datetime(1996, 7, 20),
        interval='1d',
        groupby=['city'],
    )

    assert ['count', 'mean', 'std'] == list(df.columns)
    assert 18 == len(df)

    expected_pressure_per_day = [
        (datetime(1996, 7, 15), 'istanbul', 43, 1015.558140, 1.259980),
        (datetime(1996, 7, 15), 'london', 23, 1029.286957, 1.153936),
        (datetime(1996, 7, 15), 'paris', 48, 1025.845833, 0.640949),
        (datetime(1996, 7, 16), 'istanbul', 37, 1013.621622, 10.251780),
        (datetime(1996, 7, 16), 'london', 25, 1032.356000, 0.975738),
        (datetime(1996, 7, 16), 'paris', 47, 1026.695745, 1.044939),
        (datetime(1996, 7, 17), 'istanbul', 43, 1012.974419, 1.654043),
        (datetime(1996, 7, 17), 'london', 27, 1029.459259, 2.167138),
        (datetime(1996, 7, 17), 'paris', 45, 1024.371111, 1.340584),
        (datetime(1996, 7, 18), 'istanbul', 43, 1015.993023, 0.493396),
        (datetime(1996, 7, 18), 'london', 29, 1026.893103, 0.713385),
        (datetime(1996, 7, 18), 'paris', 48, 1023.729167, 0.444390),
        (datetime(1996, 7, 19), 'istanbul', 42, 1016.776190, 0.499433),
        (datetime(1996, 7, 19), 'london', 24, 1026.350000, 0.968246),
        (datetime(1996, 7, 19), 'paris', 44, 1023.938636, 0.911596),
        (datetime(1996, 7, 20), 'istanbul', 41, 1018.141463, 0.693515),
        (datetime(1996, 7, 20), 'london', 25, 1024.372000, 0.831153),
        (datetime(1996, 7, 20), 'paris', 45, 1023.022222, 0.614234),
    ]

    for (
            (
                expected_timestamp,
                expected_city,
                expected_count,
                expected_mean,
                expected_std
            ),
            (actual_timestamp, actual_city),
            (actual_count, actual_mean, actual_std),
    ) in zip(expected_pressure_per_day, df.index.values, df.values):
        assert expected_timestamp == actual_timestamp
        assert expected_count == actual_count

        if np.isnan(expected_mean):
            assert np.isnan(actual_mean)
        else:
            assert np.isclose(expected_mean, actual_mean)

        if np.isnan(expected_std):
            assert np.isnan(actual_std)
        else:
            assert np.isclose(expected_std, actual_std)


@pytest.mark.parametrize('start, end', [
    (datetime(1996, 7, 10), datetime(1996, 7, 9)),  # end before start
    (datetime(1995, 7, 10), datetime(1995, 8, 10)),  # no data
])
def test_query_pressure_returns_empty_dataframe(
    pressure_collection,
    start,
    end,
):
    df = pressure_collection.query(
        start,
        end,
        interval='1d',
        groupby=['city'],
    )

    assert 0 == len(df)
