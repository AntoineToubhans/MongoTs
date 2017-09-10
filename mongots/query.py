from datetime import datetime

AGGR_MONTH_KEY = 'months'
AGGR_DAY_KEY = 'days'
AGGR_HOUR_KEY = 'hours'
DATETIME_KEY = 'datetime'


def build_filter_query(timestamp, tags=None):
    filters = tags or {}

    filters[DATETIME_KEY] = datetime(timestamp.year, 1, 1)

    return filters


def build_update_query(value, timestamp):
    month = str(timestamp.month - 1) # Array index: range from 0 to 11
    day = str(timestamp.day - 1)     # Array index: range from 0 to 27 / 28 / 29 or 30
    hour = str(timestamp.hour)       # range from 0 to 23

    base_inc_keys = [
        ''.join([]),
        ''.join([AGGR_MONTH_KEY, '.', month, '.']),
        ''.join([AGGR_MONTH_KEY, '.', month, '.', AGGR_DAY_KEY, '.', day, '.']),
        ''.join([AGGR_MONTH_KEY, '.', month, '.', AGGR_DAY_KEY, '.', day, '.', AGGR_HOUR_KEY, '.', hour, '.']),
    ]

    inc_update = {
        '%s%s' % (base_inc_key, aggregate_type): value if aggregate_type is "sum" else 1
        for base_inc_key in base_inc_keys
        for aggregate_type in ['count', 'sum']
    }


    return {
        '$inc': inc_update,
    }
