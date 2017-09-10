import unittest
import mongomock
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

        self.assertEqual(result, 1)

    @patch('mongots.query.MongoTSQueryBuilder.build_filters')
    def test_insert_one_call_build_filters(self, build_filters):
        result = self.mongots_collection.insert_one(
            42.66,
            datetime(2001, 11, 23, 13, 45),
            tags={'city': 'Paris'},
        )

        build_filters.assert_called()
