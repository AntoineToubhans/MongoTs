from datetime import datetime

from mongots.constants import *


def build_initial_match(start, end, tags):
    filters = tags or {}

    filters[DATETIME_KEY] = {
        '$gte': datetime(start.year, 1, 1),
        '$lte': datetime(end.year, 1, 1),
    }

    return { '$match': filters }


def _get_keys_from_interval(interval):
    try:
        int_interval = {
            '1y': 0,
            'year': 0,
            '1m': 1,
            'month': 1,
            '1d': 2,
            'day': 2,
            '1h': 3,
            'hour': 3,
        }[interval]

        return [AGGREGATION_MONTH_KEY, AGGREGATION_DAY_KEY, AGGREGATION_HOUR_KEY][0:int_interval]
    except Exception:
        raise Exception('Bad interval {interval}'.format(interval=interval))


def _get_floor_datetime(interval, dt):
    if interval == AGGREGATION_MONTH_KEY:
        return datetime(dt.year, dt.month, 1)
    elif interval == AGGREGATION_DAY_KEY:
        return datetime(dt.year, dt.month, dt.day)
    elif interval == AGGREGATION_HOUR_KEY:
        return datetime(dt.year, dt.month, dt.day, dt.hour)
    else:
        raise Exception('Bad interval {interval}'.format(interval=interval))


def build_unwind_and_match(start, end, interval):
    interval_keys = _get_keys_from_interval(interval)

    pipeline = []

    for end_index, aggregation_key in enumerate(interval_keys):
        pipeline.extend([{
          '$unwind': '${}'.format('.'.join(interval_keys[:end_index+1])),
        }, {
          '$match': {
            '.'.join(interval_keys[:end_index+1]+[DATETIME_KEY]): {
              '$gte': _get_floor_datetime(aggregation_key, start),
              '$lte': _get_floor_datetime(aggregation_key, end),
            }
          }
        }])

    return pipeline

def build_project(interval):
    interval_keys = _get_keys_from_interval(interval)

    return {
        '$project': {
            DATETIME_KEY: '${}'.format('.'.join(interval_keys+[DATETIME_KEY])),
            'count': '${}'.format('.'.join(interval_keys+['count'])),
            'sum': '${}'.format('.'.join(interval_keys+['sum'])),
            'sum2': '${}'.format('.'.join(interval_keys+['sum2'])),
        }
    }
