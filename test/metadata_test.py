import unittest
import mongomock
from datetime import datetime

from mongots.metadata import MongoTSMetadata


class MetadataTest(unittest.TestCase):
    def setUp(self):
        client = mongomock.MongoClient()
        metadata_collection = client.testDb.metadata

        self.metadata = MongoTSMetadata(metadata_collection)

    def test_metadata_acknowledge(self):
        result = self.metadata.update_metadata(
            'test_collection',
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        self.assertEqual(result, True)

    def test_metadata_retrieves_tags(self):
        tags = self.metadata.get_tags('test_collection')

        self.assertEqual(tags, {})
