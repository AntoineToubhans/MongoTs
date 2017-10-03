import pandas as pd

from mongots.constants import DATETIME_KEY
from mongots.constants import COUNT_KEY
from mongots.constants import SUM_KEY
from mongots.constants import SUM2_KEY
from mongots.constants import MIN_KEY
from mongots.constants import MAX_KEY
from mongots.constants import MEAN_KEY
from mongots.constants import STD_KEY


def build_dataframe(raw_data, aggregateby, groupby):
    if 0 == len(raw_data):
        return pd.DataFrame(
            data=[],
            columns=[COUNT_KEY, MIN_KEY, MAX_KEY, MEAN_KEY, STD_KEY],
            index=pd.Index([], name='datetime'),
        )

    columns = [
        DATETIME_KEY,
        COUNT_KEY,
        SUM_KEY,
        SUM2_KEY,
        MIN_KEY,
        MAX_KEY,
    ] + groupby

    datetime_grouper = pd.Grouper(key=DATETIME_KEY, freq=aggregateby.freq)

    df = pd.DataFrame(
        data=raw_data,
        columns=columns
    ).groupby([datetime_grouper] + groupby).aggregate({
        COUNT_KEY: 'sum',
        SUM_KEY: 'sum',
        SUM2_KEY: 'sum',
        MIN_KEY: 'min',
        MAX_KEY: 'max',
    })

    df[MEAN_KEY] = df[SUM_KEY] / df[COUNT_KEY]
    df[STD_KEY] = pd.np.sqrt(
        ((df[SUM2_KEY] / df[COUNT_KEY]) - df[MEAN_KEY]**2).apply(
            lambda v: max(v, 0.0)
        ),
    )

    return df[[COUNT_KEY, MIN_KEY, MAX_KEY, MEAN_KEY, STD_KEY]]
