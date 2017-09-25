from mongots.constants import AGGREGATION_MONTH_KEY
from mongots.constants import AGGREGATION_DAY_KEY
from mongots.constants import AGGREGATION_HOUR_KEY

YEAR = 0
MONTH = 1
DAY = 2
HOUR = 3
MINUTE = 4
SECOND = 5
MILISECOND = 6

AGGREGATION_KEYS = [
    None,  # year
    AGGREGATION_MONTH_KEY,
    AGGREGATION_DAY_KEY,
    AGGREGATION_HOUR_KEY,
    None,  # minute
    None,  # second
    None,  # milisecond
]


def parse_interval(str_interval):
    try:
        interval = {
            '1y': YEAR,
            'year': YEAR,
            '1m': MONTH,
            'month': MONTH,
            '1d': DAY,
            'day': DAY,
            '1h': HOUR,
            'hour': HOUR,
        }[str_interval]
    except Exception:
        raise Exception('Bad interval {}'.format(str_interval))

    return Interval(interval)


class Interval:
    def __init__(self, interval, min_interval=HOUR, max_interval=MONTH):
        self._interval = interval
        self._min_interval = min_interval
        self._max_interval = max_interval

        self._aggregation_keys = AGGREGATION_KEYS[
            self._max_interval:(self._interval+1)
        ]

    @property
    def aggregation_keys(self):
        return self._aggregation_keys
