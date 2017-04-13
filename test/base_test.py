import math, random, sys, unittest
from functools import reduce

from lib.generator_fake_data import FakeDataGenerator

class BaseTest(unittest.TestCase):
    def __init__(self, testName):
        super().__init__(testName)

        self._mongoConfig = {
            'uri': 'localhost',
            'port': 27017,
            'dbName': 'TestDb',
            'collectionName': 'documents__test',
        }

        # Use fake data with custom field names
        self._fakeDataGenerator = FakeDataGenerator()

    def assertAlmostEqual(self, value1, value2):
        self.assertEqual(math.floor(100000 * value1), math.floor(100000 * value2))

    def assertInsertCount(self, documents, connector):
        nInserted = 0
        for document in documents:
            nInserted += connector.push(document)

        self.assertEqual(nInserted, len(documents))
