import math, unittest, sys
from functools import reduce

# Temporary hack to import lib files
sys.path.append('./lib/')

from generator_fake_data import FakeDataGenerator
from connector_raw import RawConnector
from connector_fixed_range import FixedRangeConnector

class FixedRangeTests(unittest.TestCase):
    def __init__(self, testName):
        super().__init__(testName)

        config = {
            'uri': 'localhost',
            'port': 27017,
            'dbName': 'TestDb',
            'collectionName': 'documents__test',
        }

        self._rawConnector = RawConnector(config)
        self._fixedRangeConnector = FixedRangeConnector(config)

        self._rawDocumentCollection = self._rawConnector.getCollection('raw')
        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

        self._fakeDataGenerator = FakeDataGenerator()

    def assertAlmostEqual(self, value1, value2):
        self.assertEqual(math.floor(100000 * value1), math.floor(100000 * value2))

    def test_0_push(self):
        """ It should push 10000 documents """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        result = self._fakeDataGenerator.pushMany(10000, [
            self._rawConnector,
            self._fixedRangeConnector,
        ])

        self.assertEqual('OK', result)

    def test_1_count(self):
        """ Fixex-range total count should be 10000 """
        fixedRangeAggregates = self._fixedRangeDocumentCollection.find({})
        totalCount = reduce(lambda count, doc: count + doc['count'], fixedRangeAggregates, 0)

        self.assertEqual(totalCount, 10000)

    def test_2_aggregate(self):
        """ Fixex-range collection should be retrieved by aggregating the 10000 raw documents """
        rawAggregates = self._rawDocumentCollection.aggregate([{
            '$group': {
                '_id': {
                    'param_foo': '$param_foo',
                    'param_bar': '$param_bar',
                },
                'count': { '$sum': 1 },
                'sum': { '$sum': '$value' },
            }
        }])

        for aggregate in rawAggregates:
            fixedRangeDocument = self._fixedRangeDocumentCollection.find_one(aggregate['_id'])
            self.assertEqual(fixedRangeDocument['count'], aggregate['count'])
            self.assertAlmostEqual(fixedRangeDocument['sum'], aggregate['sum'])
