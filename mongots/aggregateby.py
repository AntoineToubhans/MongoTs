from parse import parse

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

INTERVAL_STR = ['y', 'm', 'd', 'h', 'min', 's']
STR_INTERVAL = {s: idx for idx, s in enumerate(INTERVAL_STR)}

# http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
PANDAS_FREQ_ALIAS = [
    'AS',  # year start
    'MS',  # month start
    'D',   # calendar day
    'H',   # hourly
    'T',   # minutely
    'S',   # secondly
    'L',   # milliseconds
    'U',   # microseconds
    'N',   # nanoseconds
]


class Aggregateby:
    def __init__(
        self,
        interval: int,
        coef: int = 1,
        min_interval: int = HOUR,
        max_interval: int = MONTH,
    ) -> None:
        self._interval = interval
        self._coef = coef
        self._min_interval = min_interval
        self._max_interval = max_interval

        self._aggregation_keys = AGGREGATION_KEYS[
            self._max_interval:(self._interval+1)
        ]

        self._pandas_freq = '{}{}'.format(
            self._coef,
            PANDAS_FREQ_ALIAS[self._interval]
        )

    @property
    def aggregation_keys(self):
        return self._aggregation_keys

    @property
    def freq(self) -> str:
        return self._pandas_freq


def parse_aggregateby(raw_aggregateby: str) -> Aggregateby:
    try:
        coef, str_interval = parse('{:d}{:w}', raw_aggregateby)

        interval = STR_INTERVAL[str_interval]
    except Exception:
        raise Exception('Bad interval {}'.format(raw_aggregateby))

    return Aggregateby(interval, coef=coef)
