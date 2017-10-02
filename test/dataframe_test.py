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
