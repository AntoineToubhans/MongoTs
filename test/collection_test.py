import unittest
import mongomock
import pandas as pd
from unittest.mock import patch
from datetime import datetime

import mongots


class MongoTSCollectionTest(unittest.TestCase):

    def setUp(self):
        self.mongots_collection = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        ).TestDb.temperatures

    def test_insert_one_succeeds(self):
        result = self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        self.assertEqual(result, True)

    @patch('mongots.collection.build_filter_query')
    def test_insert_one_call_build_filters(self, build_filter_query):
        build_filter_query.return_value = {}
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_filter_query.assert_called_with(
            datetime(2001, 11, 23, 13, 45),
            { 'city': 'Paris' },
        )

    @patch('mongots.collection.build_update_query')
    def test_insert_one_call_build_update(self, build_update_query):
        build_update_query.return_value = { '$inc': {}}
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_update_query.assert_called_with(
            42.66,
            datetime(2001, 11, 23, 13, 45),
        )

    def test_query_returns_a_pandas_dataframe(self):
        df = self.mongots_collection.query(
            datetime(2001, 6, 23, 13, 45),
            datetime(2001, 9, 2),
            interval='1d',
        )

        self.assertIsInstance(df, pd.DataFrame)
