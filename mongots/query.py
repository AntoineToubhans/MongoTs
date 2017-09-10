from datetime import datetime

AGGREGATION_KEYS = [
    '',
    'months.{month}.',
    'months.{month}.days.{day}.',
    'months.{month}.days.{day}.hours.{hour}.',
]
DATETIME_KEY = 'datetime'


def build_filter_query(timestamp, tags=None):
    filters = tags or {}

    filters[DATETIME_KEY] = datetime(timestamp.year, 1, 1)

    return filters


def build_update_query(value, timestamp):
    inc_values = {
        'count': 1,
        'sum': value,
    }

    datetime_args = {
        'month': str(timestamp.month - 1), # Array index: range from 0 to 11
        'day': str(timestamp.day - 1),     # Array index: range from 0 to 27 / 28 / 29 or 30
        'hour': str(timestamp.hour),       # range from 0 to 23
    }

    inc_keys = [
        key.format(**datetime_args)
        for key in AGGREGATION_KEYS
    ]

    inc_update = {
        '%s%s' % (inc_key, aggregate_type): inc_values[aggregate_type]
        for inc_key in inc_keys
        for aggregate_type in inc_values
    }

    return {
        '$inc': inc_update,
    }
