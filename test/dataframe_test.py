from datetime import datetime
import pandas as pd
import unittest
from unittest_data_provider import data_provider

from mongots import dataframe


class DataframeTest(unittest.TestCase):

    def raw_data_groupbys_and_expected_outputs():
        return [(
            # raw data
            [],
            # groupby
            [],
            # expected df index
            pd.Index([], name='datetime'),
            # expected df data
            [],
        ), (
            # raw data
            [{
                'datetime': datetime(1987, 5, 8),
                'count': 0,
                'sum': 0,
                'sum2': 0,
                'min': pd.np.inf,
                'max': -pd.np.inf,
            }],
            # groupby
            [],
            # expected df index
            pd.Index([datetime(1987, 5, 8)], name='datetime'),
            # expected df data
            [
                [0, pd.np.inf, -pd.np.inf, pd.np.nan, pd.np.nan],
            ],
        ), (
            # raw data
            [{
                'datetime': datetime(1987, 5, 8),
                'count': 5,
                'sum': 8.2,
                'sum2': 14.32,
                'min': 1.1,
                'max': 2.3,
            }],
            # groupby
            [],
            # expected df index
            pd.Index([datetime(1987, 5, 8)], name='datetime'),
            # expected df data
            [
                [5, 1.1, 2.3, 1.64, 0.41761226035642196],
            ],
        ), (
            # raw data
            [{
                'datetime': datetime(1987, 5, 8),
                'count': 3,
                'sum': 4.1,
                'sum2': 5.79,
                'min': 1.1,
                'max': 1.7,
            }, {
                'datetime': datetime(1987, 5, 8),
                'count': 2,
                'sum': 4.1,
                'sum2': 8.53,
                'min': 1.8,
                'max': 2.3,
            }],
            # groupby
            [],
            # expected df index
            pd.Index([datetime(1987, 5, 8)], name='datetime'),
            # expected df data
            [
                [5, 1.1, 2.3, 1.64, 0.41761226035642196],
            ],
        ), (
            # raw data
            [{
                'datetime': datetime(1987, 5, 8),
                'count': 3,
                'sum': 4.1,
                'sum2': 5.79,
                'min': 1.1,
                'max': 1.7,
                'plop': 'lol',
            }, {
                'datetime': datetime(1987, 5, 8),
                'count': 2,
                'sum': 4.1,
                'sum2': 8.53,
                'min': 1.8,
                'max': 2.3,
                'plop': 'mdr',
            }],
            # groupby
            ['plop'],
            # expected df index
            pd.MultiIndex.from_product([[
                datetime(1987, 5, 8),
            ], [
                'lol',
                'mdr',
            ]], names=['datetime', 'plop']),
            # expected df data
            [
                [3, 1.1, 1.7, 1.3666666666666667, 0.24944382578492938],
                [2, 1.8, 2.3, 2.05, 0.25],
            ],
        ), (
            # raw data
            [{
                'datetime': datetime(1987, 5, 8),
                'count': 3,
                'sum': 21.6 * 3,
                'sum2': 21.6**2 * 3,
                'min': 21.6,
                'max': 21.6,
            }],
            # groupby
            [],
            # expected df index
            pd.Index([datetime(1987, 5, 8)], name='datetime'),
            # expected df data
            [
                [3, 21.6, 21.6, 21.6, 0.0],
            ],
        )]

    @data_provider(raw_data_groupbys_and_expected_outputs)
    def test_build_dataframe(
        self,
        raw_data,
        groupby,
        expected_index,
        expected_data,
    ):
        actual_df = dataframe.build_dataframe(raw_data, groupby)

        pd.testing.assert_frame_equal(actual_df, pd.DataFrame(
            data=expected_data,
            index=expected_index,
            columns=['count', 'min', 'max', 'mean', 'std'],
        ))
