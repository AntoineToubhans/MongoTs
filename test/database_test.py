import unittest
import mongomock
from datetime import datetime

from mongots import MongoTSClient
from mongots import MongoTSDatabase


class MongoTSDatabaseTest(unittest.TestCase):
    def setUp(self):
        mongots_client = MongoTSClient(
            mongo_client=mongomock.MongoClient(),
        )

        self.database = mongots_client.testDb

    def test_database_is_instance_of_mongots_database(self):
        self.assertIsInstance(self.database, MongoTSDatabase)

    def test_get_collections_return_empty(self):
        self.assertEqual(self.database.get_collections(), [])

    def test_get_collections_return_two_collection(self):
        self.database.test1_collection.insert_one(
            42.6,
            datetime(1987, 5, 8, 15),
        )

        self.database.test2_collection.insert_one(
            66.02,
            datetime(1988, 12, 30),
            tags={'city': 'Vendôme'},
        )

        self.assertEqual(self.database.get_collections(), [{
            'collection_name': 'test1_collection',
            'count': 1,
            'timerange': (datetime(1987, 5, 8, 15), datetime(1987, 5, 8, 15)),
            'tags': {},
        }, {
            'collection_name': 'test2_collection',
            'count': 1,
            'timerange': (datetime(1988, 12, 30), datetime(1988, 12, 30)),
            'tags': {'city': ['Vendôme']},
        }])
