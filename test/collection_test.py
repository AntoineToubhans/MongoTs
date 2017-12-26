import unittest
import mongomock
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest_data_provider import data_provider

import mongots


class MongoTSCollectionTest(unittest.TestCase):

    def setUp(self):
        self.mongots_collection = mongots.MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        ).TestDb.temperatures

    def assertDataframeColumns(self, df):
        self.assertListEqual(list(df.columns), [
            'count',
            'min',
            'max',
            'mean',
            'std',
        ])

    @patch('mongots.collection.build_update')
    def test_insert_one_succeeds(self, build_update):
        build_update.return_value = {'$inc': {}}
        result = self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        self.assertEqual(result, True)

    @patch('mongots.collection.build_filter')
    @patch('mongots.collection.build_update')
    def test_insert_one_call_build_filters(self, build_update, build_filter):
        build_update.return_value = {'$inc': {}}
        build_filter.return_value = {}
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_filter.assert_called_with(
            datetime(2001, 11, 23, 13, 45),
            {'city': 'Paris'},
        )

    @patch('mongots.collection.build_update')
    def test_insert_one_call_build_update(self, build_update):
        build_update.return_value = {'$inc': {}}
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_update.assert_called_with(
            42.66,
            datetime(2001, 11, 23, 13, 45),
        )

    @patch('mongots.collection.build_initial_match')
    def test_query_calls_build_update(self, build_initial_match):
        build_initial_match.return_value = {'$match': {}}
        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            aggregateby='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        build_initial_match.assert_called_with(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            {'city': 'Paris'},
        )

    @patch('mongots.collection.parse_aggregateby')
    @patch('mongots.collection.build_unwind_and_match')
    def test_query_calls_build_unwind_and_match(
        self,
        build_unwind_and_match,
        parse_aggregateby,
    ):
        parsed_aggregateby = mongots.aggregateby.Aggregateby(1)
        parse_aggregateby.return_value = parsed_aggregateby
        build_unwind_and_match.return_value = []

        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            aggregateby='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        parse_aggregateby.assert_called_with('1m')
        build_unwind_and_match.assert_called_with(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            parsed_aggregateby,
        )

    @patch('mongots.collection.parse_aggregateby')
    @patch('mongots.collection.build_project')
    def test_query_calls_build_project(self, build_project, parse_aggregateby):
        parsed_aggregateby = mongots.aggregateby.Aggregateby(1)
        parse_aggregateby.return_value = parsed_aggregateby
        build_project.return_value = {'$project': {}}

        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            aggregateby='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        parse_aggregateby.assert_called_with('1m')
        build_project.assert_called_with(parsed_aggregateby, [])

    @patch('mongots.collection.build_sort')
    def test_query_calls_build_sort(self, build_sort):
        build_sort.return_value = {'$sort': {'toto': 1}}
        self.mongots_collection.query(
            datetime(2001, 10, 2, 12),
            datetime(2002, 2, 3),
            aggregateby='1m',
            tags={'city': 'Paris'},
            groupby=[],
        )

        build_sort.assert_called_with()

    def query_args():
        return [(
            datetime(2001, 6, 23, 13, 45),
            datetime(2001, 9, 2),
            {'aggregateby': '1d'}
        ), (
            datetime(2001, 6, 23, 13, 45),
            datetime(2001, 9, 2),
            {'aggregateby': '1d', 'groupby': ['city']},
        ), (
            datetime(2001, 6, 23, 13, 45),
            datetime(2001, 9, 2),
            {'aggregateby': '1d', 'tags': {'city': 'paris'}},
        )]

    @data_provider(query_args)
    def test_query_returns_a_pandas_dataframe_with_the_expected_columns(
        self,
        start,
        end,
        kwargs,
    ):
        df = self.mongots_collection.query(start, end, **kwargs)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertDataframeColumns(df)

    def test_query_raises_an_exception_if_no_interval_is_provided(self):
        with self.assertRaises(NotImplementedError):
            self.mongots_collection.query(
                datetime(1987, 5, 8),
                datetime(2000, 1, 1),
            )

        with self.assertRaises(NotImplementedError):
            self.mongots_collection.query(
                datetime(1987, 5, 8),
                datetime(2000, 1, 1),
                aggregateby=None,
            )

    def test_metadata_is_updated_when_inserting(self):
        self.mongots_collection._metadata = MagicMock()
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        self.mongots_collection._metadata.update.assert_called_once_with(
            'temperatures',
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

    def test_get_tags_returns_no_tag(self):
        tags = self.mongots_collection.get_tags()

        self.assertEqual(tags, {})

    def test_get_tags_returns_tags(self):
        self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        tags = self.mongots_collection.get_tags()

        self.assertEqual(tags, {
            'city': ['Paris'],
        })

    def test_get_timerange_returns_no_timerange(self):
        timerange = self.mongots_collection.get_timerange()

        self.assertEqual(timerange, None)

    def test_get_timerange_returns_timerange(self):
        self.mongots_collection.insert_one(42.66, datetime(2001, 11, 23))
        self.mongots_collection.insert_one(42.66, datetime(2001, 11, 20))
        self.mongots_collection.insert_one(42.66, datetime(2001, 11, 22))
        self.mongots_collection.insert_one(42.66, datetime(2001, 10, 22))

        timerange = self.mongots_collection.get_timerange()

        self.assertEqual(timerange, (
            datetime(2001, 10, 22),
            datetime(2001, 11, 23),
        ))
