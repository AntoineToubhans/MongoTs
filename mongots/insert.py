from datetime import datetime

from mongots.utils import get_day_count
from mongots.constants import AGGREGATION_MONTH_KEY
from mongots.constants import AGGREGATION_DAY_KEY
from mongots.constants import AGGREGATION_HOUR_KEY
from mongots.constants import COUNT_KEY
from mongots.constants import DATETIME_KEY
from mongots.constants import SUM_KEY
from mongots.constants import SUM2_KEY


UPDATE_KEY_TEMPLATE = [
    '',
    '{month_key}.{month}.',
    '{month_key}.{month}.{day_key}.{day}.',
    '{month_key}.{month}.{day_key}.{day}.{hour_key}.{hour}.',
]


def _build_empty_aggregate_document():
    return {
        COUNT_KEY: 0,
        SUM_KEY: 0,
        SUM2_KEY: 0,
    }


def _build_empty_one_hour_document(year, month, day, hour):
    base = _build_empty_aggregate_document()
    base[DATETIME_KEY] = datetime(year, month, day, hour)

    return base


def _build_empty_one_day_document(year, month, day):
    base = _build_empty_aggregate_document()
    base[DATETIME_KEY] = datetime(year, month, day)
    base[AGGREGATION_HOUR_KEY] = [
        _build_empty_one_hour_document(year, month, day, hour)
        for hour in range(0, 24)
    ]

    return base


def _build_empty_one_month_document(year, month):
    day_count = get_day_count(year, month)

    base = _build_empty_aggregate_document()
    base[DATETIME_KEY] = datetime(year, month, 1)
    base[AGGREGATION_DAY_KEY] = [
        _build_empty_one_day_document(year, month, day)
        for day in range(1, day_count+1)
    ]

    return base


def _build_empty_one_year_document(year):
    base = _build_empty_aggregate_document()
    base[DATETIME_KEY] = datetime(year, 1, 1)
    base[AGGREGATION_MONTH_KEY] = [
        _build_empty_one_month_document(year, month)
        for month in range(1, 13)
    ]

    return base


def build_empty_document(timestamp):
    return _build_empty_one_year_document(timestamp.year)


def build_filter(timestamp, tags=None):
    filters = tags or {}

    filters[DATETIME_KEY] = datetime(timestamp.year, 1, 1)

    return filters


def _build_update_keys(timestamp):
    key_format_kwargs = {
        'month_key': AGGREGATION_MONTH_KEY,
        'day_key': AGGREGATION_DAY_KEY,
        'hour_key': AGGREGATION_HOUR_KEY,
        # Array index: range from 0 to 11
        'month': str(timestamp.month - 1),
        # Array index: range from 0 to 27 / 28 / 29 or 30
        'day': str(timestamp.day - 1),
        # range from 0 to 23
        'hour': str(timestamp.hour),
    }

    return [
        key.format(**key_format_kwargs)
        for key in UPDATE_KEY_TEMPLATE
    ]


def _build_inc_update(value, timestamp):
    inc_values = {
        COUNT_KEY: 1,
        SUM_KEY: value,
        SUM2_KEY: value**2,
    }

    inc_keys = _build_update_keys(timestamp)

    return {
        '{}{}'.format(inc_key, aggregate_type): inc_values[aggregate_type]
        for inc_key in inc_keys
        for aggregate_type in inc_values
    }


def _build_min_max_update(value, timestamp):
    min_max_keys = _build_update_keys(timestamp)

    min_update = {
        '{}min'.format(min_max_key): value
        for min_max_key in min_max_keys
    }

    max_update = {
        '{}max'.format(min_max_key): value
        for min_max_key in min_max_keys
    }

    return min_update, max_update


def build_update(value, timestamp):
    inc_update = _build_inc_update(value, timestamp)

    return {
        '$inc': inc_update,
    }
