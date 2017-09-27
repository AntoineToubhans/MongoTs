from datetime import datetime

from mongots.constants import AGGREGATION_MONTH_KEY
from mongots.constants import AGGREGATION_DAY_KEY
from mongots.constants import AGGREGATION_HOUR_KEY
from mongots.constants import COUNT_KEY
from mongots.constants import DATETIME_KEY
from mongots.constants import SUM_KEY
from mongots.constants import SUM2_KEY
from mongots.constants import MIN_KEY
from mongots.constants import MAX_KEY


def build_initial_match(start, end, tags):
    filters = tags or {}

    filters[DATETIME_KEY] = {
        '$gte': datetime(start.year, 1, 1),
        '$lte': datetime(end.year, 1, 1),
    }

    return {'$match': filters}


def _get_floor_datetime(aggregation_level, dt):
    if aggregation_level == AGGREGATION_MONTH_KEY:
        return datetime(dt.year, dt.month, 1)
    elif aggregation_level == AGGREGATION_DAY_KEY:
        return datetime(dt.year, dt.month, dt.day)
    elif aggregation_level == AGGREGATION_HOUR_KEY:
        return datetime(dt.year, dt.month, dt.day, dt.hour)
    else:
        raise Exception('Bad aggregation_level {aggregation_level}'.format(
            aggregation_level=aggregation_level,
        ))


def build_unwind_and_match(start, end, interval):
    interval_keys = interval.aggregation_keys

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


def build_project(interval, groupby):
    interval_keys = interval.aggregation_keys
    base_projection_keys = [
        DATETIME_KEY,
        COUNT_KEY,
        SUM_KEY,
        SUM2_KEY,
        MIN_KEY,
        MAX_KEY,
    ]

    projection = {
        key: '${}'.format('.'.join(interval_keys+[key]))
        for key in base_projection_keys
    }

    projection.update({
        groupby_key: '${}'.format(groupby_key)
        for groupby_key in groupby
    })

    return {'$project': projection}


def build_sort():
    return {
        '$sort': {
            'datetime': 1,
        },
    }
