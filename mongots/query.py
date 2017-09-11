from mongots.constants import *


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
