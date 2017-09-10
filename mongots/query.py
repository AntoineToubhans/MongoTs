from datetime import datetime

DATETIME_KEY = 'datetime'
AGGR_MONTH_KEY = 'months'
AGGR_DAY_KEY = 'days'
AGGR_HOUR_KEY = 'hours'


def build_filter_query(timestamp, tags=None):
    filters = tags or {}

    filters[DATETIME_KEY] = datetime(timestamp.year, 1, 1)

    return filters


def build_update_query():
    return {
        '$inc': {},
    }
