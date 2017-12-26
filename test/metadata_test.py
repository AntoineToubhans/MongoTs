import unittest
import mongomock
from datetime import datetime

from mongots.metadata import MongoTSMetadata


class MetadataTest(unittest.TestCase):
    def setUp(self):
        client = mongomock.MongoClient()
        metadata_collection = client.testDb.mongots__metadata

        self.metadata = MongoTSMetadata(metadata_collection)

    def test_metadata_collection_name_is_right(self):
        self.assertEqual(
            self.metadata._metadata_collection.name,
            'mongots__metadata',
        )

    def test_metadata_acknowledge_no_edge_update(self):
        result = self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
        )

        self.assertEqual(result, True)

    def test_metadata_acknowledge_tag_update(self):
        result = self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        self.assertEqual(result, True)

    def test_metadata_retrieves_no_tags(self):
        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {})

    def test_metadata_retrieves_empty_tags(self):
        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
        )

        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {})

    def test_metadata_retrieves_one_tag(self):
        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {
            'sex': ['Male'],
            'type': [1],
            'is_ok': [True],
        })

    def test_metadata_retrieves_two_tags(self):
        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 11, 2),
            tags={'sex': 'Female', 'is_ok': False, 'aaa': 'yok'},
        )

        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {
            'aaa': ['yok'],
            'sex': ['Male', 'Female'],
            'type': [1],
            'is_ok': [True, False],
        })

    def test_metadata_retrieves_empty_timestamp_statistics(self):
        self.assertEqual(self.metadata.get_timerange('test_collection'), None)

    def test_metadata_retrieves_timestamp_statistics(self):
        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
        )

        self.metadata.update(
            'test_collection',
            datetime(1987, 5, 11, 2),
        )

        self.assertEqual(self.metadata.get_timerange('test_collection'), (
            datetime(1987, 5, 8, 15),
            datetime(1987, 5, 11, 2),
        ))

    def test_get_collections_returns_empty(self):
        self.assertEqual(self.metadata.get_collections(), [])

    def test_get_collections_returns_two_collection(self):
        self.metadata.update(
            'test1_collection',
            datetime(1987, 5, 8, 15),
        )

        self.metadata.update(
            'test2_collection',
            datetime(1988, 12, 30),
            tags={'city': 'Vendôme'},
        )

        self.assertEqual(self.metadata.get_collections(), [{
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
