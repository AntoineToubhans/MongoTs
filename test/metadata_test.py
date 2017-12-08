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

    def test_metadata_acknowledge(self):
        result = self.metadata.update(
            'test_collection',
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        self.assertEqual(result, True)

    def test_metadata_retrieves_empty_tags(self):
        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {})
