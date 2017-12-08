import unittest
import mongomock
from datetime import datetime

from mongots import metadata


class MetadataTest(unittest.TestCase):
    def setUp(self):
        client = mongomock.MongoClient()
        self.metadata_collection = client.testDb.metadata

    def test_metadata_acknowledge(self):
        result = metadata.update_metadata(
            self.metadata_collection,
            datetime(1987, 5, 8, 15),
            tags={'sex': 'Male', 'type': 1, 'is_ok': True},
        )

        self.assertEqual(result, True)

    def test_metadata_retrieves_tags(self):
        tags = metadata.get_tags('test_collection')

        self.assertEqual(tags, {})
