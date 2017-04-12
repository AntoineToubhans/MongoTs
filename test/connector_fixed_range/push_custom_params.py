import math, random, sys, unittest
from functools import reduce

# Temporary hack to import lib files
sys.path.append('./lib/')

from generator_fake_data import FakeDataGenerator
from connector_raw import RawConnector
from connector_fixed_range import FixedRangeConnector

class FixedRangeTests(unittest.TestCase):
    def __init__(self, testName):
        super().__init__(testName)

        self._mongoConfig = {
            'uri': 'localhost',
            'port': 27017,
            'dbName': 'TestDb',
            'collectionName': 'documents__test',
        }

        self._rawConnector = RawConnector(self._mongoConfig, timeParamName='time_yo')
        self._fixedRangeConnector = FixedRangeConnector(self._mongoConfig, timeParamName='time_yo')

        self._rawDocumentCollection = self._rawConnector.getCollection('raw')
        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

        # Use fake data with custom field names
        self._fakeDataGenerator = FakeDataGenerator(timeParamName='time_yo', params=[{
            'name': 'param_foo',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'param_bar',
            'generator': lambda: random.randint(0, 10),
        }, {
            'name': 'value',
            'generator': lambda: random.gauss(1, 1),
        }])

    def assertAlmostEqual(self, value1, value2):
        self.assertEqual(math.floor(100000 * value1), math.floor(100000 * value2))

    def test_00_push(self):
        """ It should push 10000 documents with custom groupBy / aggregate / time param names """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        result = self._fakeDataGenerator.pushMany(10000, [
            self._rawConnector,
            self._fixedRangeConnector,
        ])

        self.assertEqual('OK', result)
