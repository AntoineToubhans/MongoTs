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

    @patch('mongots.collection.build_filter')
    def test_insert_one_call_build_filters(self, build_filter):
        build_filter.return_value = {}
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_filter.assert_called_with(
            datetime(2001, 11, 23, 13, 45),
            { 'city': 'Paris' },
        )

    @patch('mongots.collection.build_update')
    def test_insert_one_call_build_update(self, build_update):
        build_update.return_value = { '$inc': {} }
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_update.assert_called_with(
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

    @patch('mongots.collection.build_initial_match')
    def test_query_calls_build_update(self, build_initial_match):
        build_initial_match.return_value = { '$match': {} }
        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            interval='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        build_initial_match.assert_called_with(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            {'city': 'Paris'},
        )

    @patch('mongots.collection.build_unwind_and_match')
    def test_query_calls_build_unwind_and_match(self, build_unwind_and_match):
        build_unwind_and_match.return_value = []
        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            interval='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        build_unwind_and_match.assert_called_with(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            '1m',
        )

    @patch('mongots.collection.build_project')
    def test_query_calls_build_project(self, build_project):
        build_project.return_value = { '$project': {} }
        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            interval='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        build_project.assert_called_with('1m')
