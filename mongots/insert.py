from datetime import datetime
from pandas import np

from mongots.utils import get_day_count
from mongots.constants import AGGREGATION_MONTH_KEY
from mongots.constants import AGGREGATION_DAY_KEY
from mongots.constants import AGGREGATION_HOUR_KEY
from mongots.constants import COUNT_KEY
from mongots.constants import DATETIME_KEY
from mongots.constants import SUM_KEY
from mongots.constants import SUM2_KEY
from mongots.constants import MIN_KEY
from mongots.constants import MAX_KEY


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
        MIN_KEY: np.infty,
        MAX_KEY: -np.infty,
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
    if tags is None:
        filters = {}
    else:
        filters = tags.copy()

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


def _build_inc_update(value, update_keys):
    inc_values = {
        COUNT_KEY: 1,
        SUM_KEY: value,
        SUM2_KEY: value**2,
    }

    return {
        '{}{}'.format(inc_key, aggregate_type): inc_values[aggregate_type]
        for inc_key in update_keys
        for aggregate_type in inc_values
    }


def _build_min_max_update(value, update_keys):
    min_update = {
        '{}{}'.format(update_key, MIN_KEY): value
        for update_key in update_keys
    }

    max_update = {
        '{}{}'.format(update_key, MAX_KEY): value
        for update_key in update_keys
    }

    return min_update, max_update


def build_update(value, timestamp):
    update_keys = _build_update_keys(timestamp)

    inc_update = _build_inc_update(value, update_keys)
    min_update, max_update = _build_min_max_update(value, update_keys)

    return {
        '$inc': inc_update,
        '$max': max_update,
        '$min': min_update,
    }
