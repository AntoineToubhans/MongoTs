import math, random, unittest
from functools import reduce

from test.base_test import BaseTest
from lib.connector_raw import RawConnector
from lib.connector_fixed_range import FixedRangeConnector

class FixedRangeDefaultParamTest(BaseTest):
    def __init__(self, testName):
        super().__init__(testName)

        self._rawConnector = RawConnector(self._mongoConfig)
        self._fixedRangeConnector = FixedRangeConnector(self._mongoConfig)

        self._rawDocumentCollection = self._rawConnector.getCollection('raw')
        self._fixedRangeDocumentCollection = self._fixedRangeConnector.getCollection('fixed_range')

    def test_00_push(self):
        """ It should push 10000 documents """
        # Removing all documents from test collection
        self._rawDocumentCollection.delete_many({})
        self._fixedRangeDocumentCollection.delete_many({})

        result = self._fakeDataGenerator.pushMany(10000, [
            self._rawConnector,
            self._fixedRangeConnector,
        ])

        self.assertEqual('OK', result)

    def test_01_count(self):
        """ Fixex-range total count should be 10000 """
        fixedRangeAggregates = self._fixedRangeDocumentCollection.find({})
        totalCount = reduce(lambda count, doc: count + doc['count'], fixedRangeAggregates, 0)

        self.assertEqual(totalCount, 10000)

    def test_02_aggregate(self):
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
